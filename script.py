import asyncio
import os
import json
import random
import sys
from typing import Optional, Tuple
from datetime import datetime

try:
    from twikit import Client
    import httpx
except ImportError as e:
    print(f"[!] Missing required dependency: {e}")
    print("[!] Install with: pip install twikit httpx[http2]")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────────────────────
# MONKEY PATCH 1: Fix ON_DEMAND_FILE_REGEX in twikit transaction module
# Remove this block once twikit ships a fix upstream.
# Wrapped in try/except so stale or already-fixed installs don't crash.
# ──────────────────────────────────────────────────────────────────────────────
import re
try:
    _tx_mod = __import__('twikit.x_client_transaction.transaction', fromlist=['ClientTransaction'])
    _tx_mod.ON_DEMAND_FILE_REGEX = re.compile(
        r""",(\d+):["']ondemand\.s["']""", flags=(re.VERBOSE | re.MULTILINE))
    _tx_mod.ON_DEMAND_HASH_PATTERN = r',{}:"([0-9a-f]+)"'

    async def _patched_get_indices(self, home_page_response, session, headers):
        key_byte_indices = []
        response = self.validate_response(home_page_response) or self.home_page_response
        on_demand_file_index = _tx_mod.ON_DEMAND_FILE_REGEX.search(str(response)).group(1)
        regex = re.compile(_tx_mod.ON_DEMAND_HASH_PATTERN.format(on_demand_file_index))
        filename = regex.search(str(response)).group(1)
        on_demand_file_url = f"https://abs.twimg.com/responsive-web/client-web/ondemand.s.{filename}a.js"
        on_demand_file_response = await session.request(method="GET", url=on_demand_file_url, headers=headers)
        key_byte_indices_match = _tx_mod.INDICES_REGEX.finditer(str(on_demand_file_response.text))
        for item in key_byte_indices_match:
            key_byte_indices.append(item.group(2))
        if not key_byte_indices:
            raise Exception("Couldn't get KEY_BYTE indices")
        key_byte_indices = list(map(int, key_byte_indices))
        return key_byte_indices[0], key_byte_indices[1:]

    _tx_mod.ClientTransaction.get_indices = _patched_get_indices
    print("[✓] Monkey patch 1 applied (ON_DEMAND_FILE_REGEX)")
except Exception as _mp1_err:
    print(f"[!] Monkey patch 1 skipped: {_mp1_err}")
    print("[!]   → If twikit errors follow, this patch may need updating.")
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# MONKEY PATCH 2: Fill missing optional fields in User.__init__
# Remove this block once twikit handles missing optional fields gracefully.
# ──────────────────────────────────────────────────────────────────────────────
try:
    from twikit import user as _user_mod
    _original_user_init = _user_mod.User.__init__

    def _patched_user_init(self, client, data):
        if 'legacy' in data:
            legacy = data['legacy']
            if 'entities' in legacy and 'description' in legacy['entities']:
                legacy['entities']['description'].setdefault('urls', [])
            legacy.setdefault('withheld_in_countries', [])
            legacy.setdefault('pinned_tweet_ids_str', [])
            legacy.setdefault('profile_interstitial_type', '')
        _original_user_init(self, client, data)

    _user_mod.User.__init__ = _patched_user_init
    print("[✓] Monkey patch 2 applied (User.__init__ optional fields)")
except Exception as _mp2_err:
    print(f"[!] Monkey patch 2 skipped: {_mp2_err}")
    print("[!]   → If User init errors follow, this patch may need updating.")
# ──────────────────────────────────────────────────────────────────────────────

# Check for ffmpeg
FFMPEG_AVAILABLE = False
try:
    import subprocess
    result = subprocess.run(['ffmpeg', '-version'],
                           capture_output=True,
                           timeout=5)
    if result.returncode == 0:
        FFMPEG_AVAILABLE = True
        print("[✓] FFmpeg detected")
except Exception:
    print("[!] WARNING: FFmpeg not found. Video download will be disabled.")
    print("[!] Install FFmpeg: https://ffmpeg.org/download.html")

# Verify HTTP/2 support
try:
    import h2
    print("[✓] HTTP/2 support detected")
except ImportError:
    print("[!] CRITICAL: HTTP/2 not available. Install with: pip install httpx[http2]")
    sys.exit(1)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
TARGET_USERNAME = 'oioiqaqzgr'
COOKIES_FILE = 'simple_twitter_cookies.json'
RAW_COOKIES_FILE = 'twitter_cookies.json'
SAVE_DIR = 'downloaded_images'

# Persisted state file – lives inside SAVE_DIR so it's carried with the artifact
STATE_FILE = os.path.join(SAVE_DIR, '.archiver_state.json')

# Download settings
MIN_FILE_SIZE = 1024        # 1 KB minimum
MAX_RETRIES = 3
TIMEOUT = 30                # seconds
TWEET_COUNT = 40
DOWNLOAD_VIDEOS = True      # Set to False to skip videos

# Early exit optimisation for real-time updates
EARLY_EXIT_THRESHOLD = 3    # Stop after N consecutive tweets with all media cached
ENABLE_EARLY_EXIT = True    # Set to False to always process all tweets

# Rate limiting
DELAY_MIN = 1.5
DELAY_MAX = 3.0
RATE_LIMIT_DELAY_MIN = 60
RATE_LIMIT_DELAY_MAX = 120

# Required cookies for authentication
REQUIRED_COOKIES = ['auth_token', 'ct0']

# Debug mode
DEBUG = False
# ──────────────────────────────────────────────────────────────────────────────


# ─── STATE MANAGEMENT ─────────────────────────────────────────────────────────

def load_state() -> dict:
    """
    Load persisted state from inside the archive directory.
    The state file travels with the artifact so user ID survives handle changes.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
            print(f"[✓] Loaded archive state: user_id={state.get('user_id', '?')}, "
                  f"last_handle=@{state.get('username', '?')}")
            return state
        except Exception as e:
            print(f"[!] Could not read state file: {e} – starting fresh")
    return {}


def save_state(state: dict):
    """Persist state back to the archive directory."""
    try:
        os.makedirs(SAVE_DIR, exist_ok=True)
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[!] Could not save state file: {e}")

# ──────────────────────────────────────────────────────────────────────────────


class DownloadStats:
    """Track download statistics."""
    def __init__(self):
        self.total_tweets = 0
        self.total_media = 0
        self.downloaded = 0
        self.skipped = 0
        self.failed = 0
        self.total_bytes = 0
        self.videos_downloaded = 0
        self.images_downloaded = 0

    def report(self):
        print("\n" + "=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Tweets processed:     {self.total_tweets}")
        print(f"Media items found:    {self.total_media}")
        print(f"Images downloaded:    {self.images_downloaded}")
        print(f"Videos downloaded:    {self.videos_downloaded}")
        print(f"Skipped (existing):   {self.skipped}")
        print(f"Failed:               {self.failed}")
        print(f"Total data:           {self.total_bytes / (1024 * 1024):.2f} MB")
        print("=" * 60)


async def convert_cookies(raw_path: str, output_path: str) -> bool:
    """Convert browser-exported cookie list to simple key-value format."""
    try:
        with open(raw_path, 'r', encoding='utf-8') as f:
            raw_cookies = json.load(f)

        if isinstance(raw_cookies, list):
            simple_cookies = {cookie['name']: cookie['value'] for cookie in raw_cookies}
        elif isinstance(raw_cookies, dict):
            simple_cookies = raw_cookies
        else:
            print(f"[!] Unknown cookie format in {raw_path}")
            return False

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(simple_cookies, f, indent=2)

        print(f"[✓] Converted {len(simple_cookies)} cookies to simple format")
        return True

    except json.JSONDecodeError as e:
        print(f"[!] Invalid JSON in {raw_path}: {e}")
        return False
    except Exception as e:
        print(f"[!] Cookie conversion failed: {e}")
        return False


async def load_and_validate_cookies(client: Client) -> bool:
    """Load cookies into client and validate required tokens."""
    if not os.path.exists(COOKIES_FILE) and os.path.exists(RAW_COOKIES_FILE):
        print("[*] Converting raw cookies to simple format...")
        if not await convert_cookies(RAW_COOKIES_FILE, COOKIES_FILE):
            return False

    if not os.path.exists(COOKIES_FILE):
        print(f"[!] Cookie file not found: {COOKIES_FILE}")
        print("[!] Please export cookies from your browser and save as 'simple_twitter_cookies.json'")
        return False

    try:
        with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
            cookies = json.load(f)

        missing = [key for key in REQUIRED_COOKIES if key not in cookies]
        if missing:
            print(f"[!] Missing required cookies: {', '.join(missing)}")
            print("[!] Please re-export cookies from an authenticated browser session")
            return False

        client.load_cookies(path=COOKIES_FILE)
        print(f"[✓] Loaded {len(cookies)} cookies into session")
        return True

    except Exception as e:
        print(f"[!] Failed to load cookies: {e}")
        return False


async def resolve_user(client: Client, state: dict):
    """
    Resolve the target user, falling back to a stored user ID if the handle
    lookup fails (e.g. because the user renamed their account).

    Returns a twikit User object, or None if resolution is impossible.
    Updates state in-place on success.
    """
    # ── Primary path: look up by current handle ───────────────────────────────
    try:
        print(f"[*] Resolving user by handle: @{TARGET_USERNAME}...")
        user = await client.get_user_by_screen_name(TARGET_USERNAME)
        print(f"[✓] Found: {user.name} (@{user.screen_name}, ID: {user.id})")

        # Persist for future fallback
        state['user_id'] = str(user.id)
        state['username'] = user.screen_name
        state['last_successful_run'] = datetime.now().isoformat()
        state.pop('handle_changed_to', None)   # Clear stale warning if handle was restored
        save_state(state)
        return user

    except Exception as primary_err:
        print(f"[!] Handle lookup failed for @{TARGET_USERNAME}: {primary_err}")

    # ── Fallback path: look up by stored user ID ──────────────────────────────
    stored_id = state.get('user_id')
    if not stored_id:
        print("[!] No stored user ID available – cannot fall back.")
        print("[!] Archive is intact. Fix TARGET_USERNAME and re-run.")
        return None

    print(f"[*] Attempting fallback via stored user ID: {stored_id} ...")
    try:
        user = await client.get_user_by_id(stored_id)
        print(f"[✓] Found user by ID: {user.name} (@{user.screen_name})")

        if user.screen_name.lower() != TARGET_USERNAME.lower():
            new_handle = user.screen_name
            print(f"\n{'!' * 60}")
            print(f"[!] HANDLE CHANGED: @{TARGET_USERNAME} → @{new_handle}")
            print(f"[!] Update TARGET_USERNAME in script.py to continue tracking.")
            print(f"{'!' * 60}\n")
            state['handle_changed_to'] = new_handle

        state['user_id'] = str(user.id)
        state['last_fallback_run'] = datetime.now().isoformat()
        save_state(state)
        return user

    except AttributeError:
        # Older twikit versions may not expose get_user_by_id
        print("[!] client.get_user_by_id() not available in this twikit version.")
        print("[!] Archive is intact. Update TARGET_USERNAME and re-run.")
        state['last_fallback_run'] = datetime.now().isoformat()
        save_state(state)
        return None

    except Exception as fallback_err:
        print(f"[!] Fallback lookup also failed: {fallback_err}")
        print("[!] Archive is intact. Update TARGET_USERNAME and re-run.")
        state['last_fallback_run'] = datetime.now().isoformat()
        save_state(state)
        return None


async def download_image(client: Client, url: str, filepath: str,
                         stats: DownloadStats) -> bool:
    """Download image with retry logic and validation."""
    if DEBUG:
        print(f"    [DEBUG] Attempting to download: {url}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.http.get(url, timeout=TIMEOUT)

            if DEBUG:
                print(f"    [DEBUG] Response status: {response.status_code}")
                print(f"    [DEBUG] Content length: {len(response.content)} bytes")

            response.raise_for_status()
            image_data = response.content

            if len(image_data) < MIN_FILE_SIZE:
                print(f"    [!] Invalid response: {len(image_data)} bytes (expected >{MIN_FILE_SIZE})")
                if DEBUG and image_data:
                    print(f"    [DEBUG] Response preview: {image_data[:200]}")
                stats.failed += 1
                return False

            with open(filepath, 'wb') as f:
                f.write(image_data)

            size_kb = len(image_data) / 1024
            print(f"    [✓] Saved {size_kb:.1f} KB")
            stats.images_downloaded += 1
            stats.total_bytes += len(image_data)
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                delay = random.uniform(RATE_LIMIT_DELAY_MIN, RATE_LIMIT_DELAY_MAX)
                print(f"    [!] Rate limited (429). Waiting {delay:.0f}s...")
                await asyncio.sleep(delay)
                continue
            elif e.response.status_code == 403:
                print(f"    [!] Access forbidden (403). Check HTTP/2 support.")
                if DEBUG:
                    print(f"    [DEBUG] Response: {e.response.text[:200]}")
                stats.failed += 1
                return False
            else:
                print(f"    [!] HTTP error {e.response.status_code}")
                if DEBUG:
                    print(f"    [DEBUG] Response: {e.response.text[:200]}")
                if attempt < MAX_RETRIES:
                    print(f"    [*] Retry {attempt}/{MAX_RETRIES}...")
                    await asyncio.sleep(2 ** attempt)
                    continue
                stats.failed += 1
                return False

        except httpx.RequestError as e:
            print(f"    [!] Network error: {e}")
            if attempt < MAX_RETRIES:
                print(f"    [*] Retry {attempt}/{MAX_RETRIES}...")
                await asyncio.sleep(2 ** attempt)
                continue
            stats.failed += 1
            return False

        except Exception as e:
            print(f"    [!] Unexpected error: {type(e).__name__}: {e}")
            if DEBUG:
                import traceback
                traceback.print_exc()
            stats.failed += 1
            return False

    return False


async def download_video(client: Client, video_url: str, filepath: str,
                         stats: DownloadStats) -> bool:
    """Download video using FFmpeg to merge audio and video streams."""
    if not FFMPEG_AVAILABLE:
        print(f"    [!] FFmpeg not available, skipping video")
        stats.failed += 1
        return False

    if DEBUG:
        print(f"    [DEBUG] Video URL: {video_url}")

    temp_video = filepath + ".temp_video.mp4"

    try:
        print(f"    [*] Downloading video stream...")
        response = await client.http.get(video_url, timeout=TIMEOUT * 3)
        response.raise_for_status()
        video_data = response.content

        if len(video_data) < MIN_FILE_SIZE:
            print(f"    [!] Invalid video response: {len(video_data)} bytes")
            stats.failed += 1
            return False

        with open(temp_video, 'wb') as f:
            f.write(video_data)

        print(f"    [*] Processing with FFmpeg...")
        process = await asyncio.create_subprocess_exec(
            'ffmpeg', '-y',
            '-i', temp_video,
            '-c', 'copy',
            '-movflags', '+faststart',
            filepath,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            print(f"    [!] FFmpeg error: {stderr.decode()[:200]}")
            stats.failed += 1
            return False

        final_size = os.path.getsize(filepath)
        size_mb = final_size / (1024 * 1024)
        print(f"    [✓] Saved {size_mb:.2f} MB")

        stats.videos_downloaded += 1
        stats.total_bytes += final_size
        return True

    except Exception as e:
        print(f"    [!] Video download error: {type(e).__name__}: {e}")
        if DEBUG:
            import traceback
            traceback.print_exc()
        stats.failed += 1
        return False

    finally:
        if os.path.exists(temp_video):
            try:
                os.remove(temp_video)
            except Exception:
                pass


async def process_media(client: Client, tweets, stats: DownloadStats):
    """Process tweets and download media."""
    stats.total_tweets = len(tweets)
    consecutive_fully_cached = 0

    for tweet_idx, tweet in enumerate(tweets, 1):
        if not hasattr(tweet, 'media') or not tweet.media:
            continue

        print(f"\n[{tweet_idx}/{len(tweets)}] Tweet ID: {tweet.id} ({len(tweet.media)} media)")

        tweet_has_new_media = False

        for media_idx, media_item in enumerate(tweet.media):
            stats.total_media += 1

            if DEBUG:
                print(f"  [DEBUG] Media object type: {type(media_item)}")
                print(f"  [DEBUG] Media type attribute: {getattr(media_item, 'type', 'unknown')}")

            media_type = getattr(media_item, 'type', 'photo')

            # ── Video / GIF ───────────────────────────────────────────────────
            if media_type in ['video', 'animated_gif']:
                if not DOWNLOAD_VIDEOS:
                    print(f"  [-] Skipping video (DOWNLOAD_VIDEOS=False)")
                    continue

                video_info = getattr(media_item, 'video_info', None)
                if not video_info:
                    print(f"  [!] No video info found for media {media_idx}")
                    stats.failed += 1
                    continue

                variants = getattr(video_info, 'variants', [])
                if not variants:
                    print(f"  [!] No video variants found")
                    stats.failed += 1
                    continue

                mp4_variants = [v for v in variants if getattr(v, 'content_type', '') == 'video/mp4']
                if not mp4_variants:
                    print(f"  [!] No MP4 variants found")
                    stats.failed += 1
                    continue

                best_variant = max(mp4_variants, key=lambda v: getattr(v, 'bitrate', 0))
                video_url = getattr(best_variant, 'url', None)

                if not video_url:
                    print(f"  [!] No URL in video variant")
                    stats.failed += 1
                    continue

                if DEBUG:
                    print(f"  [DEBUG] Video URL: {video_url}")
                    print(f"  [DEBUG] Bitrate: {getattr(best_variant, 'bitrate', 'unknown')}")

                filename = f"{tweet.id}_{media_idx}.mp4"
                filepath = os.path.join(SAVE_DIR, filename)

                if os.path.exists(filepath):
                    print(f"  [-] Skipping: {filename} (already exists)")
                    stats.skipped += 1
                    continue

                tweet_has_new_media = True
                print(f"  [+] Downloading video: {filename}")
                success = await download_video(client, video_url, filepath, stats)

                if not success:
                    print(f"  [✗] Failed to download {filename}")

                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                await asyncio.sleep(delay)
                continue

            # ── Image ─────────────────────────────────────────────────────────
            if DEBUG:
                print(f"  [DEBUG] Media attributes: {dir(media_item)}")

            media_url = None
            for attr in ['media_url', 'media_url_https', 'expanded_url']:
                if hasattr(media_item, attr):
                    temp_url = getattr(media_item, attr, None)
                    if temp_url and 'pbs.twimg.com' in temp_url:
                        media_url = temp_url
                        if DEBUG:
                            print(f"  [DEBUG] Found URL via '{attr}': {media_url}")
                        break

            if not media_url and hasattr(media_item, '__getitem__'):
                try:
                    temp_url = media_item.get('media_url') or media_item.get('media_url_https')
                    if temp_url and 'pbs.twimg.com' in temp_url:
                        media_url = temp_url
                        if DEBUG:
                            print(f"  [DEBUG] Found URL via dict access: {media_url}")
                except Exception:
                    pass

            if not media_url:
                print(f"  [!] No valid image URL found for media {media_idx}")
                if DEBUG:
                    url_attr = getattr(media_item, 'url', None)
                    expanded = getattr(media_item, 'expanded_url', None)
                    print(f"  [DEBUG] url: {url_attr}")
                    print(f"  [DEBUG] expanded_url: {expanded}")
                stats.failed += 1
                continue

            base_url = media_url.split('?')[0]
            high_res_url = f"{base_url}?format=jpg&name=orig"

            filename = f"{tweet.id}_{media_idx}.jpg"
            filepath = os.path.join(SAVE_DIR, filename)

            if os.path.exists(filepath):
                print(f"  [-] Skipping: {filename} (already exists)")
                stats.skipped += 1
                continue

            tweet_has_new_media = True
            print(f"  [+] Downloading image: {filename}")
            success = await download_image(client, high_res_url, filepath, stats)

            if not success:
                print(f"  [✗] Failed to download {filename}")

            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            await asyncio.sleep(delay)

        # ── Early exit logic ──────────────────────────────────────────────────
        if ENABLE_EARLY_EXIT:
            if tweet_has_new_media:
                consecutive_fully_cached = 0
            else:
                consecutive_fully_cached += 1
                if consecutive_fully_cached >= EARLY_EXIT_THRESHOLD:
                    remaining = len(tweets) - tweet_idx
                    print(f"\n[*] Early exit: {EARLY_EXIT_THRESHOLD} consecutive tweets fully cached.")
                    print(f"[*] Skipping remaining {remaining} tweets (likely all cached).")
                    break


async def main():
    """Main execution flow."""
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("TWITTER/X MEDIA ARCHIVER")
    print("=" * 60)
    print(f"Target: @{TARGET_USERNAME}")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if DEBUG:
        print("Debug mode: ENABLED")
    print("=" * 60 + "\n")

    os.makedirs(SAVE_DIR, exist_ok=True)

    # Load persisted state (carries user ID across runs via artifact)
    state = load_state()

    stats = DownloadStats()
    client = Client(language='en-US')

    # ── Authentication ────────────────────────────────────────────────────────
    print("[*] Loading authentication cookies...")
    if not await load_and_validate_cookies(client):
        print("\n[!] Authentication failed. Exiting.")
        # Hard failure – do NOT suppress; operator must fix cookies
        sys.exit(1)

    # ── User resolution (with handle-change fallback) ─────────────────────────
    user = await resolve_user(client, state)

    if user is None:
        # Could not resolve user even via stored ID. Archive is safe (already
        # on disk from the artifact download step). Exit 0 so the upload step
        # in the workflow runs and the artifact is preserved.
        print("\n[!] Could not resolve target user. No new media will be downloaded.")
        print("[!] Existing archive preserved. Update TARGET_USERNAME to resume.")
        sys.exit(0)

    # ── Fetch & process tweets ────────────────────────────────────────────────
    try:
        print(f"\n[*] Fetching {TWEET_COUNT} recent media tweets...")
        tweets = await client.get_user_tweets(user.id, 'Media', count=TWEET_COUNT)
        print(f"[✓] Retrieved {len(tweets)} tweets")

        if not tweets:
            print("[*] No tweets returned. Archive is up to date.")
            sys.exit(0)

        print("\n[*] Processing media...\n")
        await process_media(client, tweets, stats)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        stats.report()
        print(f"\nCompleted in {duration:.1f} seconds")
        total_downloads = stats.images_downloaded + stats.videos_downloaded
        if total_downloads > 0:
            print(f"Average: {duration / total_downloads:.1f}s per file\n")

    except Exception as e:
        print(f"\n[!] Error during fetch/process: {e}")
        import traceback
        traceback.print_exc()
        # Exit 1 so the workflow marks the run as failed, but the artifact
        # upload step uses 'if: always()' so the archive is still preserved.
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user. Exiting gracefully...")
        sys.exit(0)
