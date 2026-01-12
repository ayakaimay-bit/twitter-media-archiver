"""
Twitter/X Media Archiver - Enhanced Production Version
Robust asynchronous downloader with HTTP/2 support, retry logic, and comprehensive error handling.
"""

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
except:
    print("[!] WARNING: FFmpeg not found. Video download will be disabled.")
    print("[!] Install FFmpeg: https://ffmpeg.org/download.html")

# Verify HTTP/2 support
try:
    import h2
    print("[✓] HTTP/2 support detected")
except ImportError:
       print("[!] CRITICAL: HTTP/2 not available. Install with: pip install httpx[http2]")
       sys.exit(1)  # Exit immediately - don't ask for input

# --- CONFIGURATION ---
TARGET_USERNAME = 'jyx14612'
COOKIES_FILE = 'simple_twitter_cookies.json'
RAW_COOKIES_FILE = 'twitter_cookies.json'
SAVE_DIR = 'downloaded_images'

# Download settings
MIN_FILE_SIZE = 1024  # 1KB minimum
MAX_RETRIES = 3
TIMEOUT = 30  # seconds
TWEET_COUNT = 40
DOWNLOAD_VIDEOS = True  # Set to False to skip videos

# Early exit optimization for real-time updates
EARLY_EXIT_THRESHOLD = 3  # Stop after N consecutive tweets with all media already downloaded
ENABLE_EARLY_EXIT = True  # Set to False to always process all tweets

# Rate limiting
DELAY_MIN = 1.5
DELAY_MAX = 3.0
RATE_LIMIT_DELAY_MIN = 60
RATE_LIMIT_DELAY_MAX = 120

# Required cookies for authentication
REQUIRED_COOKIES = ['auth_token', 'ct0']

# Debug mode - set to True to see detailed information
DEBUG = False


class DownloadStats:
    """Track download statistics"""
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
        """Print final statistics"""
        print("\n" + "="*60)
        print("DOWNLOAD SUMMARY")
        print("="*60)
        print(f"Tweets processed:     {self.total_tweets}")
        print(f"Media items found:    {self.total_media}")
        print(f"Images downloaded:    {self.images_downloaded}")
        print(f"Videos downloaded:    {self.videos_downloaded}")
        print(f"Skipped (existing):   {self.skipped}")
        print(f"Failed:               {self.failed}")
        print(f"Total data:           {self.total_bytes / (1024*1024):.2f} MB")
        print("="*60)


async def convert_cookies(raw_path: str, output_path: str) -> bool:
    """
    Convert browser-exported cookie list to simple key-value format.
    
    Args:
        raw_path: Path to raw cookie file (list of dicts)
        output_path: Path to save converted cookies
    
    Returns:
        True if conversion successful
    """
    try:
        with open(raw_path, 'r', encoding='utf-8') as f:
            raw_cookies = json.load(f)
        
        # Handle both list and dict formats
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
    """
    Load cookies into client and validate required tokens.
    
    Args:
        client: Twikit client instance
    
    Returns:
        True if cookies loaded and validated successfully
    """
    # Auto-convert if raw export exists
    if not os.path.exists(COOKIES_FILE) and os.path.exists(RAW_COOKIES_FILE):
        print("[*] Converting raw cookies to simple format...")
        if not await convert_cookies(RAW_COOKIES_FILE, COOKIES_FILE):
            return False
    
    # Load cookies
    if not os.path.exists(COOKIES_FILE):
        print(f"[!] Cookie file not found: {COOKIES_FILE}")
        print("[!] Please export cookies from your browser and save as 'simple_twitter_cookies.json'")
        return False
    
    try:
        # Load and validate
        with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        
        # Check for required cookies
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


async def download_image(client: Client, url: str, filepath: str, 
                        stats: DownloadStats) -> bool:
    """
    Download image with retry logic and validation.
    
    Args:
        client: Authenticated Twikit client
        url: Image URL to download
        filepath: Where to save the image
        stats: Statistics tracker
    
    Returns:
        True if download successful
    """
    if DEBUG:
        print(f"    [DEBUG] Attempting to download: {url}")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Use client.http to access the underlying httpx client directly
            # This is more reliable than client.get() which may have unpredictable return types
            response = await client.http.get(url, timeout=TIMEOUT)
            
            if DEBUG:
                print(f"    [DEBUG] Response status: {response.status_code}")
                print(f"    [DEBUG] Response type: {type(response)}")
                print(f"    [DEBUG] Content length: {len(response.content)} bytes")
            
            response.raise_for_status()
            image_data = response.content
            
            # Validate file size
            if len(image_data) < MIN_FILE_SIZE:
                print(f"    [!] Invalid response: {len(image_data)} bytes (expected >{MIN_FILE_SIZE})")
                if DEBUG and len(image_data) > 0:
                    print(f"    [DEBUG] Response preview: {image_data[:200]}")
                stats.failed += 1
                return False
            
            # Save file
            with open(filepath, 'wb') as f:
                f.write(image_data)
            
            size_kb = len(image_data) / 1024
            print(f"    [✓] Saved {size_kb:.1f} KB")
            stats.images_downloaded += 1
            stats.total_bytes += len(image_data)
            return True
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # Rate limited
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
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
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
    """
    Download video using FFmpeg to merge audio and video streams.
    
    Args:
        client: Authenticated Twikit client
        video_url: Highest quality video URL
        filepath: Where to save the video
        stats: Statistics tracker
    
    Returns:
        True if download successful
    """
    if not FFMPEG_AVAILABLE:
        print(f"    [!] FFmpeg not available, skipping video")
        stats.failed += 1
        return False
    
    if DEBUG:
        print(f"    [DEBUG] Video URL: {video_url}")
    
    temp_video = filepath + ".temp_video.mp4"
    
    try:
        # Download video stream
        print(f"    [*] Downloading video stream...")
        response = await client.http.get(video_url, timeout=TIMEOUT * 3)
        response.raise_for_status()
        
        video_data = response.content
        
        if len(video_data) < MIN_FILE_SIZE:
            print(f"    [!] Invalid video response: {len(video_data)} bytes")
            stats.failed += 1
            return False
        
        # Save temporary video
        with open(temp_video, 'wb') as f:
            f.write(video_data)
        
        # Use FFmpeg to re-encode (this handles audio/video muxing if needed)
        # -i: input file
        # -c copy: copy streams without re-encoding (fast)
        # -movflags +faststart: optimize for web playback
        print(f"    [*] Processing with FFmpeg...")
        
        process = await asyncio.create_subprocess_exec(
            'ffmpeg', '-y',  # -y to overwrite
            '-i', temp_video,
            '-c', 'copy',  # Copy without re-encoding
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
        
        # Get final file size
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
        # Cleanup temp file
        if os.path.exists(temp_video):
            try:
                os.remove(temp_video)
            except:
                pass


async def process_media(client: Client, tweets, stats: DownloadStats):
    """
    Process tweets and download media.
    
    Args:
        client: Authenticated client
        tweets: List of tweet objects
        stats: Statistics tracker
    """
    stats.total_tweets = len(tweets)
    consecutive_fully_cached = 0  # Track consecutive tweets where all media exists
    
    for tweet_idx, tweet in enumerate(tweets, 1):
        # Check if tweet has media
        if not hasattr(tweet, 'media') or not tweet.media:
            continue
        
        print(f"\n[{tweet_idx}/{len(tweets)}] Tweet ID: {tweet.id} ({len(tweet.media)} media)")
        
        tweet_has_new_media = False  # Track if this tweet has any new media
        
        for media_idx, media_item in enumerate(tweet.media):
            stats.total_media += 1
            
            if DEBUG:
                print(f"  [DEBUG] Media object type: {type(media_item)}")
                print(f"  [DEBUG] Media type attribute: {getattr(media_item, 'type', 'unknown')}")
            
            # Determine media type
            media_type = getattr(media_item, 'type', 'photo')
            
            # Handle videos
            if media_type in ['video', 'animated_gif']:
                if not DOWNLOAD_VIDEOS:
                    print(f"  [-] Skipping video (DOWNLOAD_VIDEOS=False)")
                    continue
                
                # Get video variants (different quality options)
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
                
                # Find highest bitrate MP4 variant
                mp4_variants = [v for v in variants if getattr(v, 'content_type', '') == 'video/mp4']
                if not mp4_variants:
                    print(f"  [!] No MP4 variants found")
                    stats.failed += 1
                    continue
                
                # Sort by bitrate (highest first)
                best_variant = max(mp4_variants, key=lambda v: getattr(v, 'bitrate', 0))
                video_url = getattr(best_variant, 'url', None)
                
                if not video_url:
                    print(f"  [!] No URL in video variant")
                    stats.failed += 1
                    continue
                
                if DEBUG:
                    print(f"  [DEBUG] Video URL: {video_url}")
                    print(f"  [DEBUG] Bitrate: {getattr(best_variant, 'bitrate', 'unknown')}")
                
                # Sanitized filename for video
                filename = f"{tweet.id}_{media_idx}.mp4"
                filepath = os.path.join(SAVE_DIR, filename)
                
                # Skip if already exists
                if os.path.exists(filepath):
                    print(f"  [-] Skipping: {filename} (already exists)")
                    stats.skipped += 1
                    continue
                
                tweet_has_new_media = True
                print(f"  [+] Downloading video: {filename}")
                
                success = await download_video(client, video_url, filepath, stats)
                
                if not success:
                    print(f"  [✗] Failed to download {filename}")
                
                # Anti-bot delay
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                await asyncio.sleep(delay)
                continue
            
            # Handle images (existing code)
            if DEBUG:
                print(f"  [DEBUG] Media attributes: {dir(media_item)}")
            
            # Try multiple attribute paths to extract URL
            # Priority: media_url (direct CDN URL) > media_url_https > expanded_url
            # Skip 'url' as it returns t.co shortened links
            media_url = None
            
            # Try standard attributes in priority order
            for attr in ['media_url', 'media_url_https', 'expanded_url']:
                if hasattr(media_item, attr):
                    temp_url = getattr(media_item, attr, None)
                    if temp_url and 'pbs.twimg.com' in temp_url:
                        media_url = temp_url
                        if DEBUG:
                            print(f"  [DEBUG] Found URL via '{attr}': {media_url}")
                        break
            
            # If still not found, try dict access
            if not media_url and hasattr(media_item, '__getitem__'):
                try:
                    temp_url = media_item.get('media_url') or media_item.get('media_url_https')
                    if temp_url and 'pbs.twimg.com' in temp_url:
                        media_url = temp_url
                        if DEBUG:
                            print(f"  [DEBUG] Found URL via dict access: {media_url}")
                except:
                    pass
            
            if not media_url:
                print(f"  [!] No valid image URL found for media {media_idx}")
                if DEBUG:
                    # Show what we did find
                    url_attr = getattr(media_item, 'url', None)
                    expanded = getattr(media_item, 'expanded_url', None)
                    print(f"  [DEBUG] url: {url_attr}")
                    print(f"  [DEBUG] expanded_url: {expanded}")
                stats.failed += 1
                continue
            
            # Upgrade to original quality
            base_url = media_url.split('?')[0]
            high_res_url = f"{base_url}?format=jpg&name=orig"
            
            # Sanitized filename: {tweet_id}_{index}.jpg
            filename = f"{tweet.id}_{media_idx}.jpg"
            filepath = os.path.join(SAVE_DIR, filename)
            
            # Skip if already exists
            if os.path.exists(filepath):
                print(f"  [-] Skipping: {filename} (already exists)")
                stats.skipped += 1
                continue
            
            # Mark that this tweet has new media
            tweet_has_new_media = True
            
            print(f"  [+] Downloading image: {filename}")
            
            # Download with retry logic
            success = await download_image(client, high_res_url, filepath, stats)
            
            if not success:
                print(f"  [✗] Failed to download {filename}")
            
            # Anti-bot delay
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            await asyncio.sleep(delay)
        
        # Early exit logic for real-time updates
        if ENABLE_EARLY_EXIT:
            if tweet_has_new_media:
                consecutive_fully_cached = 0  # Reset counter
            else:
                consecutive_fully_cached += 1
                if consecutive_fully_cached >= EARLY_EXIT_THRESHOLD:
                    remaining = len(tweets) - tweet_idx
                    print(f"\n[*] Early exit: {EARLY_EXIT_THRESHOLD} consecutive tweets fully cached.")
                    print(f"[*] Skipping remaining {remaining} tweets (likely all cached).")
                    break


async def main():
    """Main execution flow"""
    start_time = datetime.now()
    print("\n" + "="*60)
    print("TWITTER/X MEDIA ARCHIVER - Enhanced Version")
    print("="*60)
    print(f"Target: @{TARGET_USERNAME}")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if DEBUG:
        print("Debug mode: ENABLED")
    print("="*60 + "\n")
    
    # Setup
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
        print(f"[✓] Created directory: {SAVE_DIR}\n")
    
    stats = DownloadStats()
    
    # Initialize client with English locale
    client = Client(language='en-US')
    
    # Authenticate
    print("[*] Loading authentication cookies...")
    if not await load_and_validate_cookies(client):
        print("\n[!] Authentication failed. Exiting.")
        return
    
    try:
        # Resolve user
        print(f"\n[*] Resolving user: @{TARGET_USERNAME}...")
        user = await client.get_user_by_screen_name(TARGET_USERNAME)
        print(f"[✓] Found: {user.name} (ID: {user.id})")
        
        # Fetch tweets - use 'Media' tab to get only tweets with media from this user
        # This excludes retweets and replies, showing only original media posts
        print(f"\n[*] Fetching {TWEET_COUNT} recent media tweets...")
        tweets = await client.get_user_tweets(user.id, 'Media', count=TWEET_COUNT)
        print(f"[✓] Retrieved {len(tweets)} tweets")
        
        # Process media
        print("\n[*] Processing media...\n")
        await process_media(client, tweets, stats)
        
        # Report
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        stats.report()
        print(f"\nCompleted in {duration:.1f} seconds")
        total_downloads = stats.images_downloaded + stats.videos_downloaded
        if total_downloads > 0:
            print(f"Average: {duration/total_downloads:.1f}s per file\n")
        
    except Exception as e:
        print(f"\n[!] Critical error: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user. Exiting gracefully...")
        sys.exit(0)
