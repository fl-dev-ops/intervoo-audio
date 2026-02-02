#!/usr/bin/env python3
"""
Fix audio durations in the database by probing actual files with ffprobe.

Usage:
    # Dry run (preview without updating)
    python fix_durations.py --dry-run

    # Actually update the database
    python fix_durations.py

    # Resume from a specific ID (if interrupted)
    python fix_durations.py --resume-from 123

    # Limit number of records to process
    python fix_durations.py --limit 100
"""

import argparse
import json
import subprocess
import sys
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:mypassword@localhost:4321/postgres"
)


def get_duration_ffprobe(url: str, timeout: int = 30) -> float | None:
    """
    Get audio duration in seconds using ffprobe.

    Args:
        url: Public URL to the audio file
        timeout: Timeout in seconds for ffprobe command

    Returns:
        Duration in seconds (float), or None if failed
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", url],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            return None

        data = json.loads(result.stdout)
        duration = data.get("format", {}).get("duration")

        if duration:
            return float(duration)
        return None

    except subprocess.TimeoutExpired:
        print(f"  Timeout probing: {url[:80]}...")
        return None
    except json.JSONDecodeError:
        print(f"  Invalid ffprobe output for: {url[:80]}...")
        return None
    except FileNotFoundError:
        print("ERROR: ffprobe not found. Install ffmpeg first:")
        print("  macOS: brew install ffmpeg")
        print("  Ubuntu: sudo apt install ffmpeg")
        sys.exit(1)
    except Exception as e:
        print(f"  Error probing {url[:80]}...: {e}")
        return None


def fetch_recordings(
    cursor,
    resume_from: str | None = None,
    limit: int | None = None,
    skip_dead_urls: bool = True,
):
    """Fetch recordings that need duration fixes."""
    query = """
        SELECT id, "audioFileUrl", duration
        FROM conversation_recordings
        WHERE status = 'READY'
          AND "audioFileUrl" IS NOT NULL
    """

    if skip_dead_urls:
        # Skip URLs from domains that are no longer accessible
        query += " AND \"audioFileUrl\" LIKE '%amazonaws.com%'"

    if resume_from:
        query += f" AND id >= '{resume_from}'"

    query += " ORDER BY id ASC"

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    return cursor.fetchall()


def update_duration(cursor, record_id: int, duration_ms: int):
    """Update a single recording's duration."""
    cursor.execute(
        "UPDATE conversation_recordings SET duration = %s WHERE id = %s",
        (duration_ms, record_id),
    )


def main():
    parser = argparse.ArgumentParser(description="Fix audio durations using ffprobe")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without updating database",
    )
    parser.add_argument(
        "--resume-from", type=int, help="Resume from a specific record ID"
    )
    parser.add_argument("--limit", type=int, help="Limit number of records to process")
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout per file in seconds (default: 30)",
    )
    parser.add_argument(
        "--include-dead-urls",
        action="store_true",
        help="Include URLs from dead domains (foreverlearning.in)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Audio Duration Fixer")
    print("=" * 60)
    print(
        f"Mode: {'DRY RUN (no changes)' if args.dry_run else 'LIVE (will update DB)'}"
    )
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Connect to database
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        print("Connected to database")
    except Exception as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)

    # Fetch recordings
    print("Fetching recordings...")
    skip_dead = not args.include_dead_urls
    recordings = fetch_recordings(cursor, args.resume_from, args.limit, skip_dead)
    total = len(recordings)
    print(f"Found {total} recordings to process")
    print()

    if total == 0:
        print("Nothing to do!")
        return

    # Process each recording
    success = 0
    failed = 0
    skipped = 0

    for i, record in enumerate(recordings, 1):
        record_id = record["id"]
        url = record["audioFileUrl"]
        old_duration = record["duration"]

        # Progress indicator
        pct = (i / total) * 100
        print(f"[{i}/{total}] ({pct:.1f}%) ID={record_id}", end=" ")

        # Probe the file
        duration_sec = get_duration_ffprobe(url, timeout=args.timeout)

        if duration_sec is None:
            print("FAILED")
            failed += 1
            continue

        # Convert to milliseconds (integer)
        duration_ms = int(duration_sec * 1000)

        # Compare with old value
        old_display = f"{old_duration}ms" if old_duration else "NULL"
        print(f"{old_display} -> {duration_ms}ms ({duration_sec:.2f}s)", end=" ")

        if not args.dry_run:
            try:
                update_duration(cursor, record_id, duration_ms)
                conn.commit()
                print("UPDATED")
            except Exception as e:
                conn.rollback()
                print(f"DB ERROR: {e}")
                failed += 1
                continue
        else:
            print("(dry run)")

        success += 1

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total processed: {total}")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print(f"Skipped: {skipped}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        print()
        print("This was a DRY RUN. No changes were made.")
        print("Run without --dry-run to apply changes.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
