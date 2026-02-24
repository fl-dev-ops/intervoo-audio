#!/usr/bin/env python3
"""
Streamlit app to browse and download student conversation audio files.

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import subprocess
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Suppress noisy thread warnings from Streamlit cache
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
    logging.ERROR
)

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:mypassword@localhost:4321/postgres"
)

# Persistent cache file for audio durations (shared across all users/sessions)
CACHE_DIR = Path(__file__).parent / ".cache"
DURATION_CACHE_FILE = CACHE_DIR / "audio_durations.json"


def load_duration_cache() -> dict:
    """Load duration cache from persistent file."""
    if DURATION_CACHE_FILE.exists():
        try:
            with open(DURATION_CACHE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_duration_cache(cache: dict) -> None:
    """Save duration cache to persistent file."""
    CACHE_DIR.mkdir(exist_ok=True)
    with open(DURATION_CACHE_FILE, "w") as f:
        json.dump(cache, f)


# Page config
st.set_page_config(page_title="Audio Browser", page_icon="🎧", layout="wide")


@st.cache_data(ttl=86400 * 7)  # Cache for 7 days (durations don't change)
def get_audio_duration(url: str) -> float | None:
    """
    Get audio duration in seconds using ffprobe.
    Cached per URL for 7 days since audio files don't change.
    """
    if not url or "amazonaws.com" not in url:
        return None

    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", url],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            return None

        data = json.loads(result.stdout)
        duration = data.get("format", {}).get("duration")

        if duration:
            return float(duration)
        return None

    except Exception:
        return None


def get_durations_parallel(urls: list[str], max_workers: int = 20) -> list[float]:
    """
    Get audio durations for multiple URLs in parallel.
    Uses ThreadPoolExecutor for concurrent ffprobe calls.
    """
    results = [0.0] * len(urls)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_idx = {
            executor.submit(get_audio_duration, url): idx
            for idx, url in enumerate(urls)
        }

        # Collect results as they complete
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                duration = future.result()
                results[idx] = duration or 0.0
            except Exception:
                results[idx] = 0.0

    return results


def extract_transcript_from_segments(report_json):
    """
    Extract transcript text from ConversationFeedback.report JSON.

    The report structure contains:
    {
        "transcript": {
            "segments": [
                {"speaker": "SPEAKER_00", "content": "Hello...", ...},
                ...
            ],
            "speaker_map": [
                {"speaker_id": "SPEAKER_00", "speaker_name": "Student"},
                ...
            ]
        }
    }
    """
    if not report_json:
        return None

    try:
        transcript = report_json.get("transcript", {})
        segments = transcript.get("segments", [])
        speaker_map = transcript.get("speaker_map", [])

        if not segments:
            return None

        # Build speaker ID to name mapping
        speaker_names = {}
        for mapping in speaker_map:
            speaker_id = mapping.get("speaker_id", "")
            speaker_name = mapping.get("speaker_name", "Unknown")
            speaker_names[speaker_id] = speaker_name

        # Concatenate segments with speaker labels
        transcript_lines = []
        for segment in segments:
            speaker_id = segment.get("speaker", "")
            content = segment.get("content", "").strip()
            if content:
                speaker_name = speaker_names.get(speaker_id, speaker_id)
                transcript_lines.append(f"[{speaker_name}]: {content}")

        return "\n".join(transcript_lines) if transcript_lines else None
    except Exception:
        return None


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data_from_db():
    """Load audio data directly from database."""

    query = """
    SELECT 
        o.name AS org_name,
        u.name AS student_name,
        a.title AS activity_name,
        COALESCE(t.name, 'No Topic') AS topic_name,
        cr."audioFileUrl" AS audio_url,
        cr."createdAt" AS created_at,
        cr.status,
        cr.duration,
        cf.report AS feedback_report
    FROM conversation_recordings cr
    JOIN organizations o ON cr."organizationId" = o.id
    JOIN users u ON cr."studentId" = u.id
    JOIN activities a ON cr."activityId" = a.id
    LEFT JOIN topic_activities ta ON a.id = ta."activityId"
    LEFT JOIN topics t ON ta."topicId" = t.id
    LEFT JOIN conversation_feedback cf ON cr.id = cf."conversationRecordingId"
    WHERE cr.status = 'READY'
    ORDER BY cr."createdAt" DESC;
    """

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            # Extract transcript from feedback_report JSON
            df["transcript"] = df["feedback_report"].apply(
                extract_transcript_from_segments
            )
            # Drop the raw JSON column
            df = df.drop(columns=["feedback_report"])
            return df, None
        else:
            return pd.DataFrame(), None

    except Exception as e:
        return None, str(e)


# Title
st.title("🎧 Student Conversation Audio Browser")
st.markdown("Browse and download student conversation recordings.")

# Refresh button in sidebar
st.sidebar.header("🔄 Data")
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Load data
df, error = load_data_from_db()

if error:
    st.error(f"Database connection error: {error}")
    st.info("Make sure the DATABASE_URL environment variable is set correctly.")
    st.stop()

if df is None or df.empty:
    st.warning("No audio recordings found in the database.")
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Organization filter
orgs = ["All"] + sorted(df["org_name"].unique().tolist())
selected_org = st.sidebar.selectbox("Organization", orgs)

# Filter dataframe by org first (for cascading filters)
filtered_df = df.copy()
if selected_org != "All":
    filtered_df = filtered_df[filtered_df["org_name"] == selected_org]

# Student filter (based on selected org)
students = ["All"] + sorted(filtered_df["student_name"].unique().tolist())
selected_student = st.sidebar.selectbox("Student", students)

if selected_student != "All":
    filtered_df = filtered_df[filtered_df["student_name"] == selected_student]

# Activity filter (based on selected org and student)
activities = ["All"] + sorted(filtered_df["activity_name"].unique().tolist())
selected_activity = st.sidebar.selectbox("Activity", activities)

if selected_activity != "All":
    filtered_df = filtered_df[filtered_df["activity_name"] == selected_activity]

# Topic filter (based on previous filters)
topics = ["All"] + sorted(filtered_df["topic_name"].unique().tolist())
selected_topic = st.sidebar.selectbox("Topic", topics)

if selected_topic != "All":
    filtered_df = filtered_df[filtered_df["topic_name"] == selected_topic]

# Date range filter
min_date = df["created_at"].dt.date.min()
max_date = df["created_at"].dt.date.max()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Apply date range filter (only when both start and end are selected)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df["created_at"].dt.date >= start_date)
        & (filtered_df["created_at"].dt.date <= end_date)
    ]

# Display stats
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Total Records:** {len(df)}")
st.sidebar.markdown(f"**Filtered Records:** {len(filtered_df)}")

# Main content
st.markdown("---")

# Stats cards
total_recordings = len(filtered_df)
unique_students = filtered_df["student_name"].nunique()

# Load persistent duration cache (shared across all users/sessions)
duration_cache = load_duration_cache()

# Check which URLs need duration calculation
urls = filtered_df["audio_url"].tolist()
uncached_urls = [url for url in urls if url not in duration_cache]

# If we have uncached URLs, calculate them automatically
if uncached_urls and not filtered_df.empty:
    # Show progress while calculating
    progress_placeholder = st.empty()
    progress_bar = progress_placeholder.progress(0, text="Loading audio durations...")

    # Process uncached URLs in batches
    batch_size = 50
    for i in range(0, len(uncached_urls), batch_size):
        batch = uncached_urls[i : i + batch_size]
        batch_durations = get_durations_parallel(batch, max_workers=20)

        # Update cache with new durations
        for url, dur in zip(batch, batch_durations):
            duration_cache[url] = dur

        # Update progress
        progress = min((i + batch_size) / len(uncached_urls), 1.0)
        progress_bar.progress(
            progress,
            text=f"Loading audio durations... {min(i + batch_size, len(uncached_urls))}/{len(uncached_urls)}",
        )

    # Save updated cache to file
    save_duration_cache(duration_cache)
    progress_placeholder.empty()

# Now get durations for all URLs from cache
filtered_df = filtered_df.copy()
filtered_df["duration_seconds"] = filtered_df["audio_url"].map(
    lambda x: duration_cache.get(x, 0.0)
)
filtered_df["duration_formatted"] = filtered_df["duration_seconds"].apply(
    lambda x: f"{int(x // 60)}m {int(x % 60):02d}s" if x > 0 else "—"
)

# Calculate totals
total_duration_seconds = filtered_df["duration_seconds"].sum()

# Format duration as human-readable (e.g., "419h 23m" or "42m")
total_hours = int(total_duration_seconds // 3600)
total_minutes = int((total_duration_seconds % 3600) // 60)

if total_hours > 0:
    duration_display = f"{total_hours}h {total_minutes}m"
else:
    duration_display = f"{total_minutes}m"

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Total Session Time",
        value=duration_display,
        help="Accurate duration from audio files",
    )

with col2:
    st.metric(
        label="Downloadable Audio",
        value=f"{total_recordings:,}",
        help="Number of audio files available for download",
    )

with col3:
    st.metric(
        label="Students",
        value=f"{unique_students:,}",
        help="Number of unique students in filtered results",
    )

st.markdown("---")

# Display filtered data
if filtered_df.empty:
    st.warning("No recordings match the selected filters.")
else:
    # Create display dataframe with download links
    columns_to_display = [
        "org_name",
        "student_name",
        "activity_name",
        "topic_name",
        "audio_url",
        "created_at",
        "duration_formatted",
        "transcript",
    ]
    column_names = [
        "Organization",
        "Student",
        "Activity",
        "Topic",
        "Audio URL",
        "Created At",
        "Duration",
        "Transcript",
    ]

    display_df = filtered_df[columns_to_display].copy()
    display_df.columns = column_names

    # Replace None/NaN transcripts with empty string for display
    display_df["Transcript"] = display_df["Transcript"].fillna(
        "No transcript available"
    )

    # Column config for display
    column_config = {
        "Audio URL": st.column_config.LinkColumn(
            "Audio URL", display_text="🔗 Download"
        ),
        "Created At": st.column_config.DatetimeColumn(
            "Created At", format="YYYY-MM-DD HH:mm"
        ),
        "Transcript": st.column_config.TextColumn(
            "Transcript",
            width="large",
            help="Full conversation transcript with speaker labels",
        ),
    }

    # Display as table with clickable links
    st.dataframe(
        display_df,
        column_config=column_config,
        hide_index=True,
        width="stretch",
    )

    # Bulk download section
    st.markdown("---")
    st.subheader("📥 Bulk Download")

    col1, col2 = st.columns(2)

    with col1:
        # Export filtered URLs to text file
        urls = filtered_df["audio_url"].tolist()
        urls_text = "\n".join(urls)
        st.download_button(
            label="📄 Download URL List",
            data=urls_text,
            file_name="audio_urls.txt",
            mime="text/plain",
        )

    with col2:
        # Build export dataframe with duration
        export_columns = [
            "org_name",
            "student_name",
            "activity_name",
            "topic_name",
            "audio_url",
            "created_at",
            "duration_seconds",
            "transcript",
        ]

        export_df = filtered_df[export_columns].copy()
        csv_data = export_df.to_csv(index=False)
        st.download_button(
            label="📊 Download CSV",
            data=csv_data,
            file_name="filtered_audio_data.csv",
            mime="text/csv",
        )

# Footer
st.markdown("---")
st.caption(
    "Data is cached for 1 hour. Audio durations are calculated automatically and cached persistently. "
    "Click 'Refresh Data' to fetch latest records."
)
