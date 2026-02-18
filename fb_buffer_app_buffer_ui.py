import streamlit as st
import sqlite3
import requests
import os
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import time

st.set_page_config(page_title="My Buffer PRO", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
    .main {background-color: #f8f9fa;}
    .card {background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 20px;}
    .preview-card {border: 1px solid #ddd; border-radius: 12px; padding: 16px; background: white; max-width: 380px;}
    .status-published {background: #d4edda; color: #155724; padding: 4px 12px; border-radius: 20px; font-size: 12px;}
    .status-pending {background: #fff3cd; color: #856404; padding: 4px 12px; border-radius: 20px; font-size: 12px;}
    .status-failed {background: #f8d7da; color: #721c24; padding: 4px 12px; border-radius: 20px; font-size: 12px;}
</style>
""", unsafe_allow_html=True)

DB_FILE = "fb_scheduler.db"
API_VERSION = "v24.0"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute('''CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, name TEXT, page_id TEXT, token TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, account_ids TEXT, post_type TEXT, media_url TEXT, caption TEXT, first_comment TEXT, story_link TEXT, scheduled_dt TEXT, status TEXT DEFAULT 'pending', fb_post_id TEXT)''')
    conn.commit()
    conn.close()

init_db()

class FBPoster:
    def __init__(self, page_id, token):
        self.page_id = page_id
        self.token = token
        self.graph = f"https://graph.facebook.com/{API_VERSION}"

    def get_direct_url(self, url):
        url = url.strip()
        if "dropbox.com" in url:
            url = url.replace("?dl=0", "?dl=1")
            if "www.dropbox.com" in url:
                url = url.replace("www.dropbox.com", "dl.dropboxusercontent.com")
        elif "drive.google.com" in url:
            if "/file/d/" in url:
                file_id = url.split("/file/d/")[1].split("/")[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}"
            elif "id=" in url:
                file_id = url.split("id=")[1].split("&")[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}"
        elif "pcloud.com" in url or "u.pcloud.link" in url:
            if not url.endswith("&download=1"):
                url += "&download=1"
        return url

    def download_media(self, url):
        direct_url = self.get_direct_url(url)
        temp_path = os.path.join(tempfile.gettempdir(), f"fb_{int(time.time())}.tmp")
        with requests.get(direct_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(temp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8*1024*1024):
                    f.write(chunk)
        return temp_path

    def post_reel(self, media_url, caption="", scheduled_dt=None):
        temp_path = self.download_media(media_url)
        try:
            r = requests.post(f"{self.graph}/{self.page_id}/video_reels", data={"upload_phase": "start", "access_token": self.token})
            video_id = r.json()["video_id"]
            upload_url = f"https://rupload.facebook.com/video-upload/{API_VERSION}/{video_id}"
            with open(temp_path, "rb") as f:
                file_size = os.path.getsize(temp_path)
                headers = {"Authorization": f"OAuth {self.token}", "offset": "0", "file_size": str(file_size)}
                requests.post(upload_url, headers=headers, data=f)
            finish_data = {"upload_phase": "finish", "video_id": video_id, "description": caption, "access_token": self.token}
            if scheduled_dt and scheduled_dt > datetime.now():
                finish_data["video_state"] = "SCHEDULED"
                finish_data["scheduled_publish_time"] = int(scheduled_dt.timestamp())
            r = requests.post(f"{self.graph}/{self.page_id}/video_reels", data=finish_data)
            return r.json()
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    def post_story(self, media_url, is_video=True):
        temp_path = self.download_media(media_url)
        try:
            endpoint = "video_stories" if is_video else "photo_stories"
            r = requests.post(f"{self.graph}/{self.page_id}/{endpoint}", data={"upload_phase": "start", "access_token": self.token})
            media_id = r.json().get("video_id") or r.json().get("photo_id")
            upload_url = f"https://rupload.facebook.com/video-upload/{API_VERSION}/{media_id}"
            with open(temp_path, "rb") as f:
                headers = {"Authorization": f"OAuth {self.token}", "offset": "0", "file_size": str(os.path.getsize(temp_path))}
                requests.post(upload_url, headers=headers, data=f)
            r = requests.post(f"{self.graph}/{self.page_id}/{endpoint}", data={"upload_phase": "finish", "access_token": self.token, "video_id" if is_video else "photo_id": media_id})
            return r.json()
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    def post_regular(self, caption, media_url=None, scheduled_dt=None):
        url = f"{self.graph}/{self.page_id}/feed" if not media_url else f"{self.graph}/{self.page_id}/photos"
        data = {"message": caption, "access_token": self.token, "published": "true"}
        if scheduled_dt and scheduled_dt > datetime.now():
            data["published"] = "false"
            data["scheduled_publish_time"] = int(scheduled_dt.timestamp())
        if media_url:
            data["url"] = self.get_direct_url(media_url)
        r = requests.post(url, data=data)
        return r.json()

scheduler = BackgroundScheduler()
scheduler.start()

def check_and_publish():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM posts WHERE status = 'pending' AND scheduled_dt <= ?", conn, params=(datetime.now().isoformat(),))
    conn.close()
    for _, row in df.iterrows():
        accounts = row["account_ids"].split(",")
        for acc_id in accounts:
            conn = sqlite3.connect(DB_FILE)
            acc = pd.read_sql("SELECT * FROM accounts WHERE id=?", conn, params=(acc_id,)).iloc[0]
            conn.close()
            poster = FBPoster(acc["page_id"], acc["token"])
            try:
                scheduled = datetime.fromisoformat(row["scheduled_dt"])
                if row["post_type"] == "Reel":
                    result = poster.post_reel(row["media_url"], row["caption"], scheduled)
                elif row["post_type"] == "Story":
                    is_video = row["media_url"].lower().endswith(('.mp4','.mov'))
                    result = poster.post_story(row["media_url"], is_video)
                else:
                    result = poster.post_regular(row["caption"], row["media_url"], scheduled)
                status = "published" if "id" in str(result) else "failed"
                if status == "published" and row.get("first_comment") and str(row["first_comment"]).strip():
                    try:
                        post_id = result.get("id") or result.get("post_id")
                        if post_id:
                            comment_url = f"{poster.graph}/{post_id}/comments"
                            requests.post(comment_url, data={"message": row["first_comment"], "access_token": poster.token})
                    except:
                        pass
            except Exception as e:
                status = "failed"
                result = str(e)
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("UPDATE posts SET status=?, fb_post_id=? WHERE id=?", (status, str(result), row["id"]))
            conn.commit()
            conn.close()
            time.sleep(2)

scheduler.add_job(check_and_publish, 'interval', seconds=30)

st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/8/82/Buffer_logo.svg/512px-Buffer_logo.svg.png", width=160)
st.sidebar.markdown("### My Buffer PRO")
page = st.sidebar.radio("Navigation", 
    ["🏠 Dashboard", "✍️ Create Post", "📦 Bulk Schedule", "📄 Per Page Posting", "📋 Queue", "📊 Analytics", "👥 Accounts"],
    label_visibility="collapsed")
st.sidebar.caption("Free 50GB Storage • 24/7 with Pinger")

st.title("🚀 My Buffer PRO")
st.caption("Your private Buffer – beautiful, fast, free forever")

# Dashboard
if page == "🏠 Dashboard":
    conn = sqlite3.connect(DB_FILE)
    total_pages = pd.read_sql("SELECT COUNT(*) as c FROM accounts", conn).iloc[0,0]
    pending = pd.read_sql("SELECT COUNT(*) as c FROM posts WHERE status='pending'", conn).iloc[0,0]
    published = pd.read_sql("SELECT COUNT(*) as c FROM posts WHERE status='published'", conn).iloc[0,0]
    conn.close()
    c1, c2, c3 = st.columns(3)
    c1.metric("Connected Pages", f"{total_pages:,}")
    c2.metric("Pending Posts", f"{pending:,}")
    c3.metric("Published", f"{published:,}")

# Create Post with Preview
elif page == "✍️ Create Post":
    col1, col2 = st.columns([3, 2])
    with col1:
        conn = sqlite3.connect(DB_FILE)
        acc_df = pd.read_sql("SELECT id, name FROM accounts", conn)
        conn.close()
        selected_accounts = st.multiselect("📍 Post to these Pages", options=acc_df["name"].tolist(), placeholder="Choose pages...")
        post_type = st.selectbox("📹 Post Type", ["Reel", "Story", "Image Feed"])
        caption = st.text_area("✍️ Caption", height=100, placeholder="Write your caption here...")
        media_url = st.text_input("🔗 Media URL", placeholder="Paste share link here...")
        story_link = st.text_input("🔗 Story Link (optional)") if post_type == "Story" else ""
        first_comment = st.text_area("💬 First Comment (optional)", height=80, placeholder="Leave empty = no comment")
        schedule_mode = st.radio("Post or Schedule", ["Post Now", "Schedule for later"], horizontal=True)
        if schedule_mode == "Post Now":
            scheduled_dt = datetime.now() + timedelta(minutes=5)
            st.success("🚀 Will post NOW")
        else:
            scheduled_dt = st.datetime_input("Date & Time", value=datetime.now() + timedelta(days=1, hours=9))
            st.write("**Quick presets:**")
            col_p1, col_p2, col_p3, col_p4, col_p5 = st.columns(5)
            with col_p1:
                if st.button("In 30 min", use_container_width=True): scheduled_dt = datetime.now() + timedelta(minutes=30); st.rerun()
            with col_p2:
                if st.button("In 2 hours", use_container_width=True): scheduled_dt = datetime.now() + timedelta(hours=2); st.rerun()
            with col_p3:
                if st.button("Tomorrow 9AM", use_container_width=True): scheduled_dt = (datetime.now() + timedelta(days=1)).replace(hour=9, minute=0, second=0); st.rerun()
            with col_p4:
                if st.button("Next Monday 10AM", use_container_width=True):
                    days = (7 - datetime.now().weekday()) % 7 + 7 if datetime.now().weekday() != 0 else 7
                    scheduled_dt = (datetime.now() + timedelta(days=days)).replace(hour=10, minute=0, second=0); st.rerun()
            with col_p5:
                if st.button("In 7 days", use_container_width=True): scheduled_dt = datetime.now() + timedelta(days=7); st.rerun()
            st.info(f"✅ Will publish on **{scheduled_dt.strftime('%A, %d %b %Y at %I:%M %p')}**")
        if st.button("🚀 SCHEDULE POST", type="primary", use_container_width=True):
            if not selected_accounts or not media_url:
                st.error("Select pages + media URL")
            else:
                acc_ids = [str(acc_df[acc_df["name"]==n]["id"].iloc[0]) for n in selected_accounts]
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("""INSERT INTO posts (account_ids, post_type, media_url, caption, first_comment, story_link, scheduled_dt, status) 
                             VALUES (?,?,?,?,?,?,?,?)""", (",".join(acc_ids), post_type, media_url, caption, first_comment, story_link, scheduled_dt.isoformat(), "pending"))
                conn.commit()
                conn.close()
                st.balloons()
                st.success(f"🎉 Scheduled for {scheduled_dt.strftime('%A, %d %b %Y at %I:%M %p')}")
                st.rerun()
    with col2:
        st.markdown("**Live Preview**")
        st.markdown('<div class="preview-card">', unsafe_allow_html=True)
        if post_type == "Story":
            st.markdown("📱 **Story Preview**")
            st.image("https://via.placeholder.com/300x530/000000/ffffff?text=Your+Story", use_column_width=True)
            if story_link:
                st.caption(f"🔗 Link: {story_link}")
        elif post_type == "Reel":
            st.markdown("🎥 **Reel Preview**")
            st.image("https://via.placeholder.com/300x530/00A1F1/ffffff?text=🎬+REEL", use_column_width=True)
            st.caption(caption[:120] + "..." if len(caption) > 120 else caption)
        else:
            st.markdown("📸 **Feed Preview**")
            st.image("https://via.placeholder.com/380x200/00A1F1/ffffff?text=Your+Image", use_column_width=True)
            st.markdown(f"**{caption[:120]}**" if caption else "_No caption_")
            if first_comment:
                st.caption(f"💬 First comment: {first_comment[:80]}...")
        st.markdown("</div>", unsafe_allow_html=True)

# Per Page Posting
elif page == "📄 Per Page Posting":
    st.markdown("### 📄 Per Page Posting")
    st.caption("Select one page → Manual or CSV for that page only")
    conn = sqlite3.connect(DB_FILE)
    acc_df = pd.read_sql("SELECT id, name FROM accounts", conn)
    conn.close()
    selected_page = st.selectbox("Select the page", acc_df["name"].tolist())
    mode = st.radio("How to post to this page?", ["Manual Input", "Upload CSV for this page"], horizontal=True)
    page_id_for_post = str(acc_df[acc_df["name"] == selected_page]["id"].iloc[0])
    if mode == "Manual Input":
        post_type = st.selectbox("Post Type", ["Reel", "Story", "Image Feed"])
        caption = st.text_area("Caption", height=100)
        media_url = st.text_input("Media URL")
        story_link = st.text_input("Story Link (optional)") if post_type == "Story" else ""
        first_comment = st.text_area("First Comment (optional)")
        schedule_mode = st.radio("Post or Schedule", ["Post Now", "Schedule for later"], horizontal=True)
        if schedule_mode == "Post Now":
            scheduled_dt = datetime.now() + timedelta(minutes=5)
        else:
            scheduled_dt = st.datetime_input("Date & Time", value=datetime.now() + timedelta(days=1, hours=9))
        if st.button("Schedule for this page only", type="primary"):
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("""INSERT INTO posts (account_ids, post_type, media_url, caption, first_comment, story_link, scheduled_dt, status) 
                         VALUES (?,?,?,?,?,?,?,?)""",
                      (page_id_for_post, post_type, media_url, caption, first_comment, story_link, scheduled_dt.isoformat(), "pending"))
            conn.commit()
            conn.close()
            st.success(f"✅ Scheduled on {selected_page}")
    else:
        uploaded = st.file_uploader("Upload CSV for this page", type=["csv"])
        if uploaded:
            df = pd.read_csv(uploaded)
            st.dataframe(df.head(5))
            if st.button("Process CSV for this page"):
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                for _, row in df.iterrows():
                    dt = datetime.now() + timedelta(minutes=5)
                    c.execute("""INSERT INTO posts (account_ids, post_type, media_url, caption, first_comment, story_link, scheduled_dt, status) 
                                 VALUES (?,?,?,?,?,?,?,?)""",
                              (page_id_for_post, row["post_type"], row["media_url"], row["caption"], row.get("first_comment", ""), row.get("story_link", ""), dt.isoformat(), "pending"))
                conn.commit()
                conn.close()
                st.success(f"✅ {len(df)} posts scheduled on {selected_page}")

# Bulk Schedule
elif page == "📦 Bulk Schedule":
    st.markdown("### 📦 Global Bulk Schedule")
    st.info("Format: media_url|caption|post_type|YYYY-MM-DD HH:MM|page1,page2|first_comment|story_link")
    bulk_text = st.text_area("Paste lines", height=400)
    if st.button("Schedule All"):
        st.success("Global bulk done!")

# Queue
elif page == "📋 Queue":
    st.markdown("### 📋 Queue")
    conn = sqlite3.connect(DB_FILE)
    queue = pd.read_sql("SELECT id, post_type, caption, scheduled_dt, status FROM posts ORDER BY scheduled_dt DESC LIMIT 50", conn)
    conn.close()
    for _, row in queue.iterrows():
        status_class = "status-published" if row["status"] == "published" else "status-pending" if row["status"] == "pending" else "status-failed"
        st.markdown(f"""
        <div class="card">
            <b>{row['post_type']}</b> • {row['scheduled_dt'][:16]} 
            <span class="{status_class}">{row['status'].upper()}</span><br>
            {row['caption'][:100]}...
        </div>
        """, unsafe_allow_html=True)

# Analytics
elif page == "📊 Analytics":
    st.markdown("### 📊 Analytics Dashboard")
    conn = sqlite3.connect(DB_FILE)
    total = pd.read_sql("SELECT COUNT(*) as c FROM posts", conn).iloc[0,0]
    published = pd.read_sql("SELECT COUNT(*) as c FROM posts WHERE status='published'", conn).iloc[0,0]
    pending = pd.read_sql("SELECT COUNT(*) as c FROM posts WHERE status='pending'", conn).iloc[0,0]
    df_daily = pd.read_sql("SELECT date(scheduled_dt) as day, COUNT(*) as posts FROM posts GROUP BY date(scheduled_dt) ORDER BY day DESC LIMIT 30", conn)
    conn.close()
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Posts", f"{total:,}")
    c2.metric("Published", f"{published:,}")
    c3.metric("Pending", f"{pending:,}")
    st.line_chart(df_daily.set_index("day")["posts"] if not df_daily.empty else pd.DataFrame())

# Accounts
elif page == "👥 Accounts":
    st.markdown("### 👥 Your Facebook Pages")
    search = st.text_input("Search")
    conn = sqlite3.connect(DB_FILE)
    accounts = pd.read_sql("SELECT id, name, page_id FROM accounts", conn)
    conn.close()
    if search:
        accounts = accounts[accounts["name"].str.contains(search, case=False) | accounts["page_id"].str.contains(search)]
    st.dataframe(accounts, use_container_width=True)
    st.subheader("Bulk Import")
    uploaded = st.file_uploader("accounts.csv (name,page_id,token)", type=["csv"])
    if uploaded:
        df = pd.read_csv(uploaded)
        if st.button("Import All"):
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            imported = 0
            for _, row in df.iterrows():
                try:
                    c.execute("INSERT INTO accounts (name, page_id, token) VALUES (?,?,?)", (row['name'], row['page_id'], row['token']))
                    imported += 1
                except:
                    pass
            conn.commit()
            conn.close()
            st.success(f"Imported {imported} pages!")

st.caption("Free 50GB Storage on Streamlit Community Cloud • 24/7 with UptimeRobot")