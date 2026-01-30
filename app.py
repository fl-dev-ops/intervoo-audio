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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:mypassword@localhost:4321/postgres"
)

# Page config
st.set_page_config(page_title="Audio Browser", page_icon="🎧", layout="wide")


@st.cache_data(ttl=60)  # Cache for 60 seconds
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
        cr.duration
    FROM conversation_recordings cr
    JOIN organizations o ON cr."organizationId" = o.id
    JOIN users u ON cr."studentId" = u.id
    JOIN activities a ON cr."activityId" = a.id
    LEFT JOIN topic_activities ta ON a.id = ta."activityId"
    LEFT JOIN topics t ON ta."topicId" = t.id
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

# Display stats
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Total Records:** {len(df)}")
st.sidebar.markdown(f"**Filtered Records:** {len(filtered_df)}")

# Main content
st.markdown("---")

# Stats cards
# Duration values are mixed: some in seconds (small values), some in milliseconds (large values)
# Normalize all to seconds: if value > 1000, assume milliseconds and convert to seconds
durations = filtered_df["duration"].fillna(0)
normalized_seconds = durations.apply(lambda x: x / 1000 if x > 1000 else x)
total_duration_seconds = normalized_seconds.sum()
total_hours = total_duration_seconds / 3600  # Convert seconds to hours
total_recordings = len(filtered_df)
unique_students = filtered_df["student_name"].nunique()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Total Session Time",
        value=f"{total_hours:.1f} hrs",
        help="Total duration of all filtered recordings",
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
    display_df = filtered_df[
        [
            "org_name",
            "student_name",
            "activity_name",
            "topic_name",
            "audio_url",
            "created_at",
            "duration",
        ]
    ].copy()
    display_df.columns = [
        "Organization",
        "Student",
        "Activity",
        "Topic",
        "Audio URL",
        "Created At",
        "Duration (ms)",
    ]

    # Format duration
    if "Duration (ms)" in display_df.columns:
        display_df["Duration (ms)"] = display_df["Duration (ms)"].fillna(0).astype(int)

    # Display as table with clickable links
    st.dataframe(
        display_df,
        column_config={
            "Audio URL": st.column_config.LinkColumn(
                "Audio URL", display_text="🔗 Download"
            ),
            "Created At": st.column_config.DatetimeColumn(
                "Created At", format="YYYY-MM-DD HH:mm"
            ),
        },
        hide_index=True,
        use_container_width=True,
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
        # Export filtered data to CSV
        csv_data = filtered_df.to_csv(index=False)
        st.download_button(
            label="📊 Download CSV",
            data=csv_data,
            file_name="filtered_audio_data.csv",
            mime="text/csv",
        )

# Footer
st.markdown("---")
st.caption(
    "Data is cached for 60 seconds. Click 'Refresh Data' to fetch latest records."
)
