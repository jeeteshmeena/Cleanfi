import os, re, json, asyncio, time, uuid, logging
from pathlib import Path
import shutil
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ButtonStyle
from pyrogram.errors import FloodWait
from pyrogram.handlers import RawUpdateHandler, MessageHandler
from pyrogram.file_id import FileId
from dotenv import load_dotenv
from mutagen import File as MFile
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from metadata import clean_and_apply_metadata, read_original_title

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USERBOT_API_ID = int(os.getenv("USERBOT_API_ID", str(API_ID)))
USERBOT_API_HASH = os.getenv("USERBOT_API_HASH", API_HASH)
USERBOT_SESSION_STRING = os.getenv("USERBOT_SESSION_STRING", "").strip()
USERBOT_SESSION_FILE = Path(os.getenv("USERBOT_SESSION_FILE", "./userbot.session"))
ADMIN_IDS_LIST = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
ADMINS = set(ADMIN_IDS_LIST)
OWNER_ID = int(os.getenv("OWNER_ID", str(ADMIN_IDS_LIST[0] if ADMIN_IDS_LIST else 0)))
TEMP = Path(os.getenv("TEMP_DIR", "./tmp")); TEMP.mkdir(parents=True, exist_ok=True)
STATE = Path(os.getenv("STATE_FILE", "./jobs.json"))
MIN_DELAY_SECONDS = 3
MAX_DELAY_SECONDS = 60
DEFAULT_DELAY_SECONDS = min(MAX_DELAY_SECONDS, max(MIN_DELAY_SECONDS, int(os.getenv("FILE_DELAY_SECONDS", "3"))))
MAX_QUEUE = max(1, int(os.getenv("MAX_QUEUED_JOBS", "20")))
DEFAULT_RETRIES = max(0, int(os.getenv("TRANSIENT_RETRIES", "5")))
MIN_FREE_DISK_GB = max(0, int(os.getenv("MIN_FREE_DISK_GB", "2")))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("cleanfi")

app = Client("cleanfi", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_app = None
user_login_client = None
user_login_task = None
user_login_qr = None
jobs, settings, sessions, running = {}, {}, {}, {}
state_lock = None
tg_lock = None
flood_until = 0.0
job_queue = None
queue_task = None
queued_jobs = set()
# Deterministic FIFO order for queued jobs. The first waiting job is position 1,
# the second waiting job is position 2, etc. (the currently running job is not
# counted as a waiting position, but remains the active job ahead of the queue.)
queue_order = []
job_tasks = {}
live_queue = None
live_task = None
live_seen_ids = set()


def save_sync():
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"settings": settings, "jobs": jobs}, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE)

async def save():
    global state_lock
    if state_lock is None:
        state_lock = asyncio.Lock()
    async with state_lock:
        await asyncio.to_thread(save_sync)

def load():
    global jobs, settings
    try:
        data = json.loads(STATE.read_text(encoding="utf-8"))
        settings = data.get("settings", {})
        jobs = data.get("jobs", {})
    except Exception:
        settings, jobs = {}, {}
    settings.setdefault("source", os.getenv("SOURCE_CHAT_ID", ""))
    settings.setdefault("authorized_users", [OWNER_ID])
    settings["authorized_users"] = sorted({OWNER_ID} | {int(x) for x in settings.get("authorized_users", [])})
    settings.setdefault("live", {"status": "stopped", "source": "", "target": "", "meta": {}, "stats": {"files_sent": 0, "bytes_downloaded": 0, "bytes_uploaded": 0}, "pending_link": None, "start_id": 0})
    if settings["live"].get("status") in {"running", "paused"}:
        settings["live"]["status"] = "stopped"
    jobs.setdefault("__live__", {"id":"__live__","owner":OWNER_ID,"source":settings["live"].get("source") or "","target":settings["live"].get("target") or "","start":int(settings["live"].get("start_id") or 0),"end":0,"total":0,"processed":[],"failed":[],"skipped":[],"failed_reasons":{},"status":"stopped","cancel_requested":False,"meta":settings["live"].get("meta") or {},"delay_seconds":int(settings.get("file_delay", DEFAULT_DELAY_SECONDS)),"stats":settings["live"].get("stats", {"files_sent":0,"bytes_downloaded":0,"bytes_uploaded":0}),"current":None})
    if not jobs["__live__"]["meta"]:
        jobs["__live__"]["meta"] = newmeta()
    settings["live"]["meta"] = jobs["__live__"]["meta"]
    settings["live"].setdefault("source", jobs["__live__"].get("source") or "")
    settings["live"].setdefault("target", jobs["__live__"].get("target") or "")
    settings["live"].setdefault("start_id", int(jobs["__live__"].get("start_id") or 0))
    settings["live"].setdefault("backlog_loaded", False)
    settings.setdefault("target", os.getenv("TARGET_CHAT_ID", ""))
    settings.setdefault("retries", DEFAULT_RETRIES)
    settings.setdefault("file_delay", DEFAULT_DELAY_SECONDS)
    settings["file_delay"] = min(MAX_DELAY_SECONDS, max(MIN_DELAY_SECONDS, int(settings["file_delay"])))
    settings.setdefault("min_free_gb", MIN_FREE_DISK_GB)
    gm = settings.setdefault("global_meta", {})
    for _k in ("artist", "genre", "year", "album", "album_artist", "comment", "cover_path"):
        gm.setdefault(_k, None)
    for j in jobs.values():
        j.setdefault("source", settings["source"]); j.setdefault("target", settings["target"])
        j.setdefault("processed", []); j.setdefault("failed", []); j.setdefault("skipped", [])
        j.setdefault("failed_reasons", {}); j.setdefault("started_at", None)
        j.setdefault("progress_message_id", None); j.setdefault("last_progress", 0)
        j.setdefault("current", None); j.setdefault("flood_until", 0)
        if j.get("status") in {"running", "queued", "cancelling"}:
            j["status"] = "paused"
        j.setdefault("retries", 0)
        j.setdefault("stats", {"files_sent": len(j.get("processed", [])), "bytes_downloaded": 0, "bytes_uploaded": 0, "started_at": j.get("started_at"), "finished_at": None})

def allowed(m):
    return bool(m.from_user and m.from_user.id in settings.get("authorized_users", []))

def is_owner(m):
    return bool(m.from_user and m.from_user.id == OWNER_ID)

def authorized_users():
    return sorted({int(x) for x in settings.get("authorized_users", [])})

def active(m):
    return sessions.get(m.from_user.id)

def get_delay(jid):
    value = jobs.get(jid, {}).get("delay_seconds", settings.get("file_delay", DEFAULT_DELAY_SECONDS))
    return min(MAX_DELAY_SECONDS, max(MIN_DELAY_SECONDS, int(value)))

def getjid(m):
    p = (m.text or "").split()
    return p[1] if len(p) > 1 and p[1] in jobs else active(m)

def infer_name(media):
    name = getattr(media, "file_name", None) or ""
    if name:
        return name
    mime = (getattr(media, "mime_type", None) or "").lower()
    ext = {"audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/flac": ".flac", "audio/ogg": ".ogg", "audio/opus": ".opus", "audio/wav": ".wav", "audio/x-wav": ".wav", "audio/aac": ".aac", "audio/x-ms-wma": ".wma"}.get(mime, ".bin")
    return f"message_{getattr(media, 'file_id', 'unknown')}{ext}"

END_POST_TEXT = "Hey, the story is complete. Hope you like it 🫶🏻.\n\nIf you're looking for another story, then try… @StoriesByJeetXNew"

GENRE_OPTIONS = ["Drama", "Fantasy", "Suspense & Thriller", "Horror", "Romance", "System", "Romantasy"]

def newmeta():
    g = settings.get("global_meta", {})
    return {"title_mode": "original", "artist": g.get("artist"), "genre": g.get("genre"), "year": g.get("year"),
            "album": g.get("album"), "album_artist": g.get("album_artist"), "comment": g.get("comment"), "cover_path": g.get("cover_path")}

async def tg_call(fn, jid=None, label="Telegram"):
    """Single-flight Telegram call with infinite FloodWait retry.
    FloodWait is never converted into a failed file.
    """
    global flood_until, tg_lock
    if tg_lock is None:
        tg_lock = asyncio.Lock()
    while True:
        async with tg_lock:
            try:
                result = await fn()
                flood_until = 0
                if jid and jobs.get(jid):
                    jobs[jid]["flood_until"] = 0
                return result
            except FloodWait as e:
                seconds = max(1, int(e.value))
                flood_until = time.time() + seconds
                if jid and jobs.get(jid):
                    jobs[jid]["flood_until"] = flood_until
                    jobs[jid]["flood_label"] = label
                    await save()
                print(f"FloodWait: {label}; sleeping {seconds}s; retrying the same operation")
        await asyncio.sleep(seconds + 1)

async def resolve_chat(value):
    value = str(value).strip()
    return await tg_call(lambda: app.get_chat(int(value) if re.fullmatch(r"-?\d+", value) else value), label="get_chat")

async def safe_progress(jid, force=False):
    j = jobs[jid]; now = time.time()
    if not force and now - j.get("last_progress", 0) < 2:
        return
    j["last_progress"] = now
    text = progress_text(jid)
    try:
        if j.get("progress_message_id"):
            await tg_call(lambda: app.edit_message_text(j["owner"], j["progress_message_id"], text), jid, "progress")
        else:
            msg = await tg_call(lambda: app.send_message(j["owner"], text), jid, "progress")
            j["progress_message_id"] = msg.id
        await save()
    except Exception as e:
        print(f"Progress update failed: {type(e).__name__}: {e}")

def flood_text(jid):
    until = jobs[jid].get("flood_until", 0)
    if until > time.time():
        return f"Waiting {max(1, int(until-time.time()))}s"
    return "Protected"

def progress_text(jid):
    j = jobs[jid]
    done = len(j["processed"]) + len(j["failed"]) + len(j["skipped"])
    total = j["total"]
    pct = int(done * 100 / total) if total else 100
    width = 16
    bar = "█" * int(width * pct / 100) + "░" * (width - int(width * pct / 100))
    elapsed = max(0, time.time() - (j.get("started_at") or time.time()))
    speed = done / (elapsed / 60) if done and elapsed else 0
    eta = ((total-done) / speed * 60) if speed else 0
    current = j.get("current")
    cur = f"\nCurrent: #{current}" if current else ""
    return (f"Cleanfi Processing\nJob: {jid}{cur}\n\n{bar} {pct}%\n"
            f"Files: {done} / {total}\nProcessed: {len(j['processed'])}\n"
            f"Failed: {len(j['failed'])}\nSkipped: {len(j['skipped'])}\n"
            f"Speed: {speed:.1f} files/min\nElapsed: {int(elapsed//60)}m {int(elapsed%60)}s\n"
            f"ETA: {int(eta//60)}m {int(eta%60)}s\nFloodWait: {flood_text(jid)}")

EMOJI = {
    "artist": "5373334855612375386",
    "cover": "5424885441100782420",
    "new": "5375464961822695044",
    "jobs": "5372926953978341366",
    "source": "5471978009449731768",
    "target": "5472105307985419058",
    "status": "5370771949842602821",
    "failed": "5370987174948774327",
    "help": "5370738629486319646",
    "delay": "5451732530048802485",
    "start": "5373026167722876724",
    "cancel": "5472309400536358507",
    "genre": "5359441070201513074",
}

def custom_emoji(emoji_id, alt):
    return f'<tg-emoji emoji-id="{emoji_id}">{alt}</tg-emoji>'

def ib(text, data, emoji=None, style=ButtonStyle.PRIMARY):
    kw = {"callback_data": data, "style": style}
    if emoji:
        kw["icon_custom_emoji_id"] = EMOJI[emoji]
    return InlineKeyboardButton(text, **kw)

def global_meta_kb():
    g = settings.get("global_meta", {})
    def v(k):
        x = g.get(k)
        return str(x) if x not in (None, "") else "Not set"
    return InlineKeyboardMarkup([
        [ib(f"Artist: {v('artist')}", "gm:artist", "artist", ButtonStyle.PRIMARY),
         ib(f"Album: {v('album')}", "gm:album", "genre", ButtonStyle.PRIMARY)],
        [ib(f"Album Artist: {v('album_artist')}", "gm:album_artist", "artist", ButtonStyle.PRIMARY),
         ib(f"Genre: {v('genre')}", "gm:genre", "genre", ButtonStyle.PRIMARY)],
        [ib(f"Year: {v('year')}", "gm:year", "status", ButtonStyle.PRIMARY),
         ib(f"Comment: {v('comment')}", "gm:comment", "help", ButtonStyle.PRIMARY)],
        [ib(f"Cover: {'Set' if g.get('cover_path') else 'Not set'}", "gm:cover", "cover", ButtonStyle.PRIMARY)],
        [ib("Back", "gm:back", "cancel", ButtonStyle.DANGER)],
    ])

def main_kb():
    return InlineKeyboardMarkup([
        [ib("Global Metadata", "m:metadata", "genre", ButtonStyle.PRIMARY)],
        [ib("Set Source", "m:source", "source", ButtonStyle.PRIMARY),
         ib("Set Target", "m:target", "target", ButtonStyle.PRIMARY)],
        [ib("Delay", "m:delay", "delay", ButtonStyle.PRIMARY),
         ib("New Job", "m:new", "new", ButtonStyle.SUCCESS)],
        [ib("Live Cleaner", "m:live", "start", ButtonStyle.SUCCESS), ib("Users", "m:users", "status", ButtonStyle.PRIMARY)],
        [ib("Jobs", "m:jobs", "jobs", ButtonStyle.PRIMARY), ib("Stats", "m:stats", "status", ButtonStyle.PRIMARY)],
        [ib("Status", "m:status", "status", ButtonStyle.PRIMARY),
         ib("Failed", "m:failed", "failed", ButtonStyle.DANGER)],
        [ib("Help", "m:help", "help", ButtonStyle.PRIMARY)],
    ])

def confirm_kb(kind):
    return InlineKeyboardMarkup([
        [ib("Confirm", f"confirm:{kind}", "start", ButtonStyle.SUCCESS),
         ib("Cancel", f"cancel_confirm:{kind}", "cancel", ButtonStyle.DANGER)]
    ])

def job_kb(jid):
    m = jobs[jid]["meta"]
    return InlineKeyboardMarkup([
        [ib(f"Artist: {m.get('artist') or 'Not set'}", f"s:artist:{jid}", "artist", ButtonStyle.PRIMARY)],
        [ib(f"Genre: {m.get('genre') or 'Not set'}", f"s:genre:{jid}", "genre", ButtonStyle.PRIMARY),
         ib("Choose", f"genre:{jid}", "genre", ButtonStyle.PRIMARY)],
        [ib(f"Year: {m.get('year') or 'Not set'}", f"s:year:{jid}", "status", ButtonStyle.PRIMARY)],
        [ib(f"Album: {m.get('album') or 'Not set'}", f"s:album:{jid}", "genre", ButtonStyle.PRIMARY)],
        [ib(f"Album Artist: {m.get('album_artist') or 'Not set'}", f"s:album_artist:{jid}", "artist", ButtonStyle.PRIMARY)],
        [ib(f"Comment: {m.get('comment') or 'Not set'}", f"s:comment:{jid}", "help", ButtonStyle.PRIMARY)],
        [ib(f"Start Post: {'Set' if jobs[jid].get('start_post', {}).get('image_path') else 'Not set'}", f"startpost:{jid}", "cover", ButtonStyle.PRIMARY),
         ib(f"Cover: {'Attached' if m.get('cover_path') else 'Not set'}", f"cover:{jid}", "cover", ButtonStyle.PRIMARY)],
        [ib("Clear Cover", f"clear:{jid}", "cancel", ButtonStyle.DANGER)],
        [ib("START", f"start:{jid}", "start", ButtonStyle.SUCCESS),
         ib("CANCEL JOB", f"cancel:{jid}", "cancel", ButtonStyle.DANGER)],
        [ib("Refresh", f"refresh:{jid}", "status", ButtonStyle.PRIMARY)],
    ])

def summary(jid):
    j = jobs[jid]; m = j["meta"]
    return (f"Cleanfi Job {jid}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\n"
            f"Source: {j.get('source') or 'Not set'}\nTarget: {j.get('target') or 'Not set'}\n\n"
            f"Artist: {m.get('artist') or '—'}\nGenre: {m.get('genre') or '—'}\nYear: {m.get('year') or '—'}\n"
            f"Album: {m.get('album') or '—'}\nAlbum Artist: {m.get('album_artist') or '—'}\n"
            f"Comment: {m.get('comment') or '—'}\nCover: {'Attached' if m.get('cover_path') else 'Not attached'}\n"
            f"Title: Original source title\nDelay: {get_delay(jid)}s between episodes\n\nProcessed: {len(j['processed'])}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}")

async def snapshot_job_cover(jid):
    src = settings.get("global_meta", {}).get("cover_path")
    if src and Path(src).is_file():
        dest = TEMP / jid / "cover.jpg"
        dest.parent.mkdir(exist_ok=True)
        shutil.copy2(src, dest)
        jobs[jid]["meta"]["cover_path"] = str(dest)

async def create_batch_job(owner, first_link, end_link):
    jid = uuid.uuid4().hex[:8]
    jobs[jid] = {"id": jid, "owner": owner, "first_link": first_link, "end_link": end_link, "start": None, "end": None,
                 "total": 0, "processed": [], "failed": [], "failed_reasons": {}, "skipped": [], "status": "configured",
                 "cancel_requested": False, "meta": newmeta(), "source": settings["source"], "target": settings["target"],
                 "created": time.time(), "started_at": None, "progress_message_id": None, "current": None,
                 "flood_until": 0, "queue_ids": None, "delay_seconds": int(settings.get("file_delay", DEFAULT_DELAY_SECONDS)),
                 "start_post": {"image_path": None, "caption": None, "sent": False}, "end_post_sent": False,
                  "stats": {"files_sent": 0, "bytes_downloaded": 0, "bytes_uploaded": 0, "started_at": None, "finished_at": None}}
    await snapshot_job_cover(jid)
    sessions[owner] = jid
    await save()
    return jid

async def fetch_batch_ids(jid):
    j = jobs[jid]
    first = await tg_call(lambda: app.get_messages(int(j["source"]), int(j["first_link"].rstrip("/").split("/")[-1])), jid, "batch first link")
    end = await tg_call(lambda: app.get_messages(int(j["source"]), int(j["end_link"].rstrip("/").split("/")[-1])), jid, "batch end link")
    first_id, end_id = first.id, end.id
    if first_id > end_id: first_id, end_id = end_id, first_id
    j["start"], j["end"] = first_id, end_id
    j["total"] = end_id - first_id + 1
    j["queue_ids"] = list(range(first_id, end_id + 1))
    await save()
    return j["queue_ids"]

async def create_job(owner, start, end):
    jid = uuid.uuid4().hex[:8]
    jobs[jid] = {"id": jid, "owner": owner, "start": start, "end": end, "total": end-start+1,
                 "processed": [], "failed": [], "failed_reasons": {}, "skipped": [], "status": "configured",
                 "cancel_requested": False, "meta": newmeta(), "source": settings["source"], "target": settings["target"],
                 "created": time.time(), "started_at": None, "progress_message_id": None,
                 "current": None, "flood_until": 0, "queue_ids": None,
                 "delay_seconds": int(settings.get("file_delay", DEFAULT_DELAY_SECONDS)),
                 "start_post": {"image_path": None, "caption": None, "sent": False},
                 "end_post_sent": False}
    await snapshot_job_cover(jid)
    sessions[owner] = jid
    await save()
    return jid

async def raw_update(_, update, users, chats):
    log.info("RAW TELEGRAM UPDATE: %s", type(update).__name__)

app.add_handler(RawUpdateHandler(raw_update), group=-1000)

async def post_start(jid):
    j = jobs[jid]
    post = j.get("start_post") or {}
    path = post.get("image_path")
    caption = (post.get("caption") or "").strip()
    if not path or not caption or post.get("sent"):
        return False
    if not Path(path).is_file():
        log.warning("Start post image missing for job %s: %s", jid, path)
        return False
    sent = await tg_call(
        lambda: app.send_photo(int(j["target"]), photo=path, caption=caption),
        jid, "start post"
    )
    post["message_id"] = sent.id
    post["sent"] = True
    await save()
    try:
        await tg_call(
            lambda: app.pin_chat_message(int(j["target"]), int(sent.id), disable_notification=True),
            jid, "pin start post"
        )
        post["pinned"] = True
        await save()
        log.info("Start post sent and pinned: job=%s message=%s target=%s", jid, sent.id, j["target"])
    except Exception:
        post["pinned"] = False
        await save()
        log.exception("Could not pin start post for job %s", jid)
        raise
    return True

async def post_end(jid):
    j = jobs[jid]
    if j.get("end_post_sent"):
        return
    await tg_call(lambda: app.send_message(int(j["target"]), END_POST_TEXT), jid, "end post")
    j["end_post_sent"] = True
    await save()


@app.on_message(filters.private & filters.text, group=-100)
async def entry_fallback(_, m):
    text = (m.text or "").strip().split(maxsplit=1)[0].lower()
    if text not in ("/start", "/menu"):
        return
    log.info("Incoming %s from user=%s", text, m.from_user.id if m.from_user else None)
    if not allowed(m):
        return
    await m.reply_text("Welcome to Cleanfi\n\nCleanfi is ready to process your stories.", reply_markup=main_kb())
    m.stop_propagation()

@app.on_message(filters.private & filters.command("menu"))
async def menu_cmd(_, m):
    if allowed(m):
        await m.reply_text("CLEANFI", reply_markup=main_kb())

@app.on_message(filters.private & filters.command("start"))
async def start_cmd(_, m):
    if allowed(m):
        await m.reply_text("CLEANFI\n\nAudiobook metadata cleaner & repacker.", reply_markup=main_kb())

@app.on_message(filters.private & filters.command("help"))
async def help_cmd(_, m):
    if allowed(m):
        await m.reply_text("CLEANFI\n\n/range START END\n/source @channel\n/target @channel\n/meta artist=\"Name\" genre=\"Romance\" year=2026\n/cover JOBID\n/startjob JOBID\n/status JOBID\n/delay SECONDS\n/cancel JOBID\n/retry JOBID\n/failed JOBID\n/test MESSAGE_ID\n/jobs", reply_markup=main_kb())

@app.on_message(filters.private & filters.command("adduser"))
async def adduser_cmd(_, m):
    if not is_owner(m): return
    p = (m.text or "").split(maxsplit=1)
    if len(p) != 2 or not re.fullmatch(r"\d+", p[1]): return await m.reply_text("Usage: /adduser USER_ID")
    uid = int(p[1]); users = set(authorized_users()); users.add(uid); settings["authorized_users"] = sorted(users); await save()
    await m.reply_text(f"User {uid} added successfully.", reply_markup=main_kb())

@app.on_message(filters.private & filters.command("deluser"))
async def deluser_cmd(_, m):
    if not is_owner(m): return
    p = (m.text or "").split(maxsplit=1)
    if len(p) != 2 or not re.fullmatch(r"\d+", p[1]): return await m.reply_text("Usage: /deluser USER_ID")
    uid = int(p[1])
    if uid == OWNER_ID: return await m.reply_text("Owner cannot be removed.")
    users = set(authorized_users()); users.discard(uid); settings["authorized_users"] = sorted(users); await save()
    await m.reply_text(f"User {uid} removed.", reply_markup=main_kb())

@app.on_message(filters.private & filters.command("users"))
async def users_cmd(_, m):
    if not is_owner(m): return
    users = authorized_users()
    await m.reply_text("Authorized Users\n\n" + ("\n".join(str(x) for x in users) or "No users."), reply_markup=main_kb())

@app.on_message(filters.private & filters.command("source"))
async def source_cmd(_, m):
    if not allowed(m): return
    p = (m.text or "").split(maxsplit=1)
    if len(p) != 2: return await m.reply_text("Usage: /source @channelusername")
    try:
        c = await resolve_chat(p[1]); settings["source"] = str(c.id); await save()
        await m.reply_text(f" Source set\n{c.title or c.first_name}\nID: {c.id}", reply_markup=main_kb())
    except Exception as e: await m.reply_text(f"Could not set source: {type(e).__name__}: {e}")

@app.on_message(filters.private & filters.command("target"))
async def target_cmd(_, m):
    if not allowed(m): return
    p = (m.text or "").split(maxsplit=1)
    if len(p) != 2: return await m.reply_text("Usage: /target @channelusername")
    try:
        c = await resolve_chat(p[1]); settings["target"] = str(c.id); await save()
        await m.reply_text(f"✦ Target set\n{c.title or c.first_name}\nID: {c.id}", reply_markup=main_kb())
    except Exception as e: await m.reply_text(f"Could not set target: {type(e).__name__}: {e}")

@app.on_message(filters.private & filters.command("delay"))
async def delay_cmd(_, m):
    if not allowed(m): return
    p = (m.text or "").split()
    if len(p) != 2 or not p[1].isdigit():
        return await m.reply_text(f"Usage: /delay 3  (allowed: {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS} seconds)")
    seconds = int(p[1])
    if seconds < MIN_DELAY_SECONDS or seconds > MAX_DELAY_SECONDS:
        return await m.reply_text(f"Delay must be between {MIN_DELAY_SECONDS} and {MAX_DELAY_SECONDS} seconds.")
    settings["file_delay"] = seconds
    await save()
    jid = active(m)
    if jid and jid in jobs and jobs[jid].get("status") in {"configured","paused","cancelled","completed","completed_with_failures"}:
        jobs[jid]["delay_seconds"] = seconds
        await save()
    await m.reply_text(f"Episode delay set to {seconds} seconds. New jobs will use this delay.")

@app.on_message(filters.private & filters.command("startpost"))
async def startpost_cmd(_, m):
    if not allowed(m): return
    p = (m.text or "").split(maxsplit=1)
    jid = p[1].strip() if len(p) == 2 else active(m)
    if not jid or jid not in jobs:
        return await m.reply_text("Create/select a job first. Usage: /startpost JOB_ID")
    if jobs[jid].get("status") in {"running", "queued"}:
        return await m.reply_text("Set the start post before starting the job.")
    sessions[m.from_user.id] = jid
    sessions[(m.from_user.id, "startpost")] = True
    await m.reply_text(f"Send the start image now with the story name as its caption.\nJob: {jid}")

@app.on_message(filters.private & (filters.photo | filters.document), group=-80)
async def cover_media(_, m):
    if not allowed(m):
        return
    log.info("COVER MEDIA RECEIVED user=%s type=%s global=%s job=%s active=%s", m.from_user.id, "photo" if m.photo else "document", bool(sessions.get((m.from_user.id, "global_cover"))), bool(sessions.get((m.from_user.id, "job_cover"))), sessions.get(m.from_user.id))
    # Start-post capture: preserve BOTH the image and its caption.
    if sessions.get((m.from_user.id, "startpost")):
        jid = sessions.get(m.from_user.id)
        if not jid or jid not in jobs:
            sessions.pop((m.from_user.id, "startpost"), None)
            return await m.reply_text("That job is no longer available. Open the job again and use Start Post.")
        if m.photo:
            image = m.photo
        elif m.document and (m.document.mime_type or "").startswith("image/"):
            image = m.document
        else:
            return await m.reply_text("Please send an image for the Start Post.")
        d = TEMP / jid
        d.mkdir(exist_ok=True)
        path = d / "start_post.jpg"
        try:
            await tg_call(lambda: m.download(file_name=str(path)), jid, "start post download")
            jobs[jid]["start_post"] = {
                "image_path": str(path),
                "caption": m.caption or "",
                "sent": False,
            }
            sessions.pop((m.from_user.id, "startpost"), None)
            sessions[m.from_user.id] = jid
            await save()
            log.info("START POST SAVED user=%s job=%s path=%s caption=%r", m.from_user.id, jid, path, m.caption or "")
            return await m.reply_text(
                f"Start Post saved successfully for Job {jid}.\n"
                "The image and caption will be sent to the target channel when the job starts and the post will be pinned.",
                reply_markup=job_kb(jid)
            )
        except Exception as e:
            log.exception("START POST SAVE FAILED user=%s job=%s", m.from_user.id, jid)
            return await m.reply_text(f"Start Post save failed: {type(e).__name__}: {e}", reply_markup=job_kb(jid))
    if m.document and not (m.document.mime_type or "").startswith("image/"):
        return
    if sessions.get((m.from_user.id, "global_cover")):
        path = TEMP / "global_cover.jpg"
        try:
            await tg_call(lambda: m.download(file_name=str(path)), None, "global cover download")
            sessions.pop((m.from_user.id, "global_cover"), None)
            sessions[(m.from_user.id, "pending_global")] = ("cover_path", str(path))
            log.info("GLOBAL COVER RECEIVED user=%s path=%s", m.from_user.id, path)
            await m.reply_text("Global cover received.\n\nConfirm?", reply_markup=confirm_kb("global"))
        except Exception as e:
            log.exception("GLOBAL COVER DOWNLOAD FAILED user=%s", m.from_user.id)
            await m.reply_text(f"Global cover download failed: {type(e).__name__}: {e}")
        return
    if sessions.get((m.from_user.id, "live_cover")):
        image = m.photo or (m.document if m.document and (m.document.mime_type or "").startswith("image/") else None)
        if not image:
            return await m.reply_text("Please send an image for the Live Cleaner cover / thumbnail.")
        d = TEMP / "live"
        d.mkdir(exist_ok=True)
        path = d / f"cover_{uuid.uuid4().hex}.jpg"
        try:
            await tg_call(lambda: m.download(file_name=str(path)), "__live__", "live cover download")
            live_record()["meta"]["cover_path"] = str(path)
            sessions.pop((m.from_user.id, "live_cover"), None)
            await save()
            log.info("LIVE COVER SAVED user=%s path=%s", m.from_user.id, path)
            return await m.reply_text("Live Cleaner cover / thumbnail saved.", reply_markup=live_meta_kb())
        except Exception as e:
            log.exception("LIVE COVER SAVE FAILED user=%s", m.from_user.id)
            return await m.reply_text(f"Live cover save failed: {type(e).__name__}: {e}", reply_markup=live_meta_kb())

    jid = sessions.get(m.from_user.id) if sessions.get((m.from_user.id, "job_cover")) else active(m)
    if not jid or jid not in jobs:
        return await m.reply_text("Create/select a job first, then use the Cover button.")
    d = TEMP / jid
    d.mkdir(exist_ok=True)
    path = d / "cover.jpg"
    try:
        await tg_call(lambda: m.download(file_name=str(path)), jid, "job cover download")
        jobs[jid]["meta"]["cover_path"] = str(path)
        sessions.pop((m.from_user.id, "job_cover"), None)
        sessions[m.from_user.id] = jid
        await save()
        log.info("JOB COVER SAVED user=%s job=%s path=%s", m.from_user.id, jid, path)
        await m.reply_text(f"Job {jid} cover saved successfully.", reply_markup=job_kb(jid))
    except Exception as e:
        log.exception("JOB COVER FAILED user=%s job=%s", m.from_user.id, jid)
        await m.reply_text(f"Job cover save failed: {type(e).__name__}: {e}", reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("cover"))
async def cover_cmd(_, m):
    if allowed(m):
        jid = getjid(m)
        if jid in jobs: await m.reply_text(f"Send the image with caption /cover {jid}")

@app.on_message(filters.private & filters.command("jobs"))
async def jobs_cmd(_, m):
    if allowed(m):
        rows = [f"{x} — {j['status']} — {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-20:]]
        await m.reply_text("✦ JOBS\n\n" + ("\n".join(rows) or "No jobs."), reply_markup=main_kb())

@app.on_message(filters.private & filters.command("status"))
async def status_cmd(_, m):
    if not allowed(m): return
    jid = getjid(m)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    await m.reply_text(progress_text(jid) if jobs[jid]["status"] in {"running","cancelling","queued"} else summary(jid), reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("failed"))
async def failed_cmd(_, m):
    if not allowed(m): return
    jid = getjid(m)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    j = jobs[jid]
    text = "\n".join(f"{i} — {j['failed_reasons'].get(str(i),'unknown')}" for i in j["failed"])
    await m.reply_text("✦ FAILED\n\n" + (text or "No failed files."))

@app.on_message(filters.private & filters.command("cancel"))
async def cancel_cmd(_, m):
    if not allowed(m): return
    jid = getjid(m)
    if jid not in jobs:
        return await m.reply_text("Unknown Job ID. Use /cancel JOB_ID or open Jobs.")
    j = jobs[jid]
    if j.get("status") in {"completed", "completed_with_failures", "cancelled", "failed_queue"}:
        return await m.reply_text(f"Job {jid} is already stopped.", reply_markup=job_kb(jid))
    j["cancel_requested"] = True
    if j.get("status") == "queued":
        queued_jobs.discard(jid)
        if jid in queue_order:
            queue_order.remove(jid)
        j["status"] = "cancelled"
        j["current"] = None
    else:
        j["status"] = "cancelling"
    await save()
    task = job_tasks.get(jid)
    if task and not task.done():
        task.cancel()
    log.info("CANCEL command user=%s job=%s status=%s", m.from_user.id, jid, j["status"])
    await m.reply_text(f"Cancellation requested for Job {jid}. The current operation will finish safely, then the job will stop.", reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("retry"))
async def retry_cmd(_, m):
    if not allowed(m): return
    jid = getjid(m)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    ids = jobs[jid]["failed"][:]
    if not ids: return await m.reply_text("No failed files.")
    jobs[jid]["failed"] = []; jobs[jid]["failed_reasons"] = {}; jobs[jid]["cancel_requested"] = False
    await save(); await launch(jid, m, ids)

@app.on_message(filters.private & filters.command("startjob"))
async def startjob_cmd(_, m):
    if allowed(m):
        jid = getjid(m)
        if jid in jobs: await launch(jid, m)

@app.on_message(filters.private & filters.command("test"))
async def test_cmd(_, m):
    if not allowed(m): return
    p = (m.text or "").split()
    if len(p) != 2 or not p[1].isdigit(): return await m.reply_text("Usage: /test MESSAGE_ID")
    if not settings.get("source") or not settings.get("target"): return await m.reply_text("Set source and target first.")
    jid = await create_job(m.from_user.id, int(p[1]), int(p[1]))
    await m.reply_text(summary(jid), reply_markup=job_kb(jid))

async def download_media_direct(media, path, jid, client=None, message=None):
    """Download source media using either Pyrogram or the Telegram user client."""
    client = client or app
    if isinstance(client, TelegramClient):
        if message is None:
            raise RuntimeError("Telethon source message is required")
        await client.download_media(message, file=path)
        written = Path(path).stat().st_size if Path(path).exists() else 0
    else:
        file_id = FileId.decode(media.file_id)
        written = 0
        with open(path, "wb") as fp:
            async for chunk in client.get_file(file_id, file_size=getattr(media, "file_size", 0) or 0):
                fp.write(chunk)
                written += len(chunk)
    if written <= 0:
        raise RuntimeError("Telegram returned an empty media download")
    if jid and jobs.get(jid):
        jobs[jid].setdefault("stats", {})["bytes_downloaded"] = jobs[jid].get("stats", {}).get("bytes_downloaded", 0) + written
    return path

async def process_file(jid, mid, message=None, meta_override=None, source_client=None):
    j = jobs[jid]
    meta = meta_override or j["meta"]
    if MIN_FREE_DISK_GB and __import__("shutil").disk_usage(TEMP).free < MIN_FREE_DISK_GB * 1024**3:
        return "failed", f"Low disk space: less than {MIN_FREE_DISK_GB} GB free"
    source_client = source_client or app
    msg = message or await tg_call(lambda: source_client.get_messages(int(j["source"]), mid), jid, "get_messages")
    if isinstance(source_client, TelegramClient):
        media = getattr(msg, "audio", None) or (msg.document if getattr(msg, "document", None) and str(getattr(msg.document, "mime_type", "") or "").startswith("audio/") else None)
        name = getattr(getattr(msg, "file", None), "name", None) or f"{mid}.mp3"
        caption = getattr(msg, "text", "") or ""
    else:
        media = msg.audio or (msg.document if msg.document and (getattr(msg.document, "mime_type", "") or "").startswith("audio/") else None)
        name = infer_name(media) if media else f"{mid}.mp3"
        caption = msg.caption or ""
    if not media: return "skip", "message has no audio media"
    ext = Path(name).suffix.lower()
    supported = {".mp3",".m4a",".mp4",".flac",".ogg",".opus",".wav",".aiff",".aif",".wma",".aac"}
    if ext not in supported: return "skip", f"unsupported container {ext or 'unknown'}"
    if ext == ".aac": return "skip", "raw AAC has no portable metadata container"
    d = TEMP / jid; d.mkdir(exist_ok=True)
    inp = d / f"{mid}_{Path(name).name}"; out = d / f"out_{mid}_{Path(name).name}"
    try:
        await download_media_direct(media, str(inp), jid, client=source_client, message=msg)
        title = read_original_title(str(inp))
        if title is None:
            try:
                f = MFile(str(inp), easy=True); title = (f.get("title") or [None])[0] if f else None
            except Exception: title = None
        title = title if title is not None else Path(name).stem
        await asyncio.to_thread(clean_and_apply_metadata, str(inp), str(out), title=title,
            artist=meta.get("artist"), genre=meta.get("genre"), year=meta.get("year"),
            cover=meta.get("cover_path"), album=meta.get("album"),
            album_artist=meta.get("album_artist"), comment=meta.get("comment"))
        upload_bytes = out.stat().st_size if out.exists() else 0
        kw = {"audio": str(out), "caption": caption, "file_name": name, "title": str(title)}
        if meta.get("artist"): kw["performer"] = str(meta["artist"])
        cp = meta.get("cover_path")
        if cp and Path(cp).is_file(): kw["thumb"] = cp
        await tg_call(lambda: app.send_audio(int(j["target"]), **kw), jid, "upload")
        j.setdefault("stats", {})["files_sent"] = j.get("stats", {}).get("files_sent", 0) + 1
        j["stats"]["bytes_uploaded"] = j.get("stats", {}).get("bytes_uploaded", 0) + upload_bytes
        return "ok", None
    finally:
        for p in (inp, out):
            try: p.unlink()
            except FileNotFoundError: pass

async def process_file_with_retry(jid, mid, message=None, meta_override=None, source_client=None):
    attempt = 0
    while True:
        try:
            return await process_file(jid, mid, message=message, meta_override=meta_override, source_client=source_client)
        except FloodWait:
            raise
        except Exception:
            attempt += 1
            jobs[jid]["retries"] = jobs[jid].get("retries", 0) + 1
            await save()
            if attempt > int(settings.get("retries", DEFAULT_RETRIES)):
                raise
            await asyncio.sleep(min(30, 2 ** attempt))

def live_record():
    live = settings.setdefault("live", {})
    live.setdefault("status", "stopped")
    live.setdefault("source", "")
    live.setdefault("target", "")
    live.setdefault("start_id", 0)
    live.setdefault("meta", jobs.get("__live__", {}).get("meta") or newmeta())
    live.setdefault("stats", jobs.get("__live__", {}).get("stats", {"files_sent": 0, "bytes_downloaded": 0, "bytes_uploaded": 0}))
    live.setdefault("queued", 0)
    live.setdefault("current", None)
    live.setdefault("backlog_loaded", False)
    live.setdefault("loading_backlog", False)
    live.setdefault("ingest_buffer", [])
    return live

def live_meta_kb():
    m = live_record().get("meta") or {}
    cover = "Attached" if m.get("cover_path") else "Not set"
    rows = [
        [ib(f"Artist: {m.get('artist') or 'Not set'}", "lmeta:artist", "artist")],
        [ib(f"Genre: {m.get('genre') or 'Not set'}", "lmeta:genre", "genre")],
        [ib(f"Year: {m.get('year') or 'Not set'}", "lmeta:year", "status")],
        [ib(f"Album: {m.get('album') or 'Not set'}", "lmeta:album", "genre")],
        [ib(f"Album Artist: {m.get('album_artist') or 'Not set'}", "lmeta:album_artist", "artist")],
        [ib(f"Comment: {m.get('comment') or 'Not set'}", "lmeta:comment", "help")],
        [ib(f"Cover / Thumbnail: {cover}", "lcover:set", "cover")],
        [ib("Clear Cover", "lcover:clear", "cancel", ButtonStyle.DANGER)]
    ]
    if live_record().get("pending_link"):
        rows.append([ib("Continue", "live:continue", "start", ButtonStyle.SUCCESS)])
    else:
        rows.append([ib("Back", "live:meta_back", "cancel")])
    return InlineKeyboardMarkup(rows)

def live_kb():
    status = live_record().get("status", "stopped")
    controls = ([ib("Pause", "live:pause", "delay"), ib("Stop", "live:stop", "cancel", ButtonStyle.DANGER)]
                if status == "running" else
                [ib("Resume", "live:start", "start", ButtonStyle.SUCCESS), ib("Stop", "live:stop", "cancel", ButtonStyle.DANGER)]
                if status == "paused" else
                [ib("START", "live:start", "start", ButtonStyle.SUCCESS)])
    return InlineKeyboardMarkup([controls, [ib("Live Source", "live:source", "source"), ib("Live Target", "live:target", "target")], [ib(f"Start ID: {live_record().get('start_id') or 'Not set'}", "live:startid", "status")], [ib("Live Metadata", "live:meta", "genre")], [ib("Refresh", "live:refresh", "status")], [ib("Back", "live:back", "cancel", ButtonStyle.DANGER)]])

def live_text():
    live = live_record(); s = live.get("stats", {})
    return (f"Live Cleaner\n\nStatus: {live.get('status','stopped').title()}\n"
            f"Source: {live.get('source') or 'Not set'}\nTarget: {live.get('target') or 'Not set'}\nStart ID: {live.get('start_id') or 'Not set'}\n"
            f"Queued: {live.get('queued', 0)}\nCurrent: {live.get('current') or 'Idle'}\n\n"
            f"Files sent: {s.get('files_sent', 0)}\nData uploaded: {s.get('bytes_uploaded', 0)/(1024**2):.2f} MB")

async def live_worker():
    global live_queue
    queue = live_queue
    while True:
        item = await queue.get()
        kind = item[0] if isinstance(item, tuple) else "audio"
        msg = item[1] if isinstance(item, tuple) else item
        source_client = item[2] if isinstance(item, tuple) and len(item) > 2 else app
        live = live_record()
        try:
            while live.get("status") == "paused":
                await asyncio.sleep(1)
            if live.get("status") != "running":
                continue
            live["current"] = msg.id
            live["queued"] = max(0, live.get("queued", 0) - 1)
            if kind == "link":
                live["status"] = "paused"
                jobs["__live__"]["status"] = "paused"
                live["pending_link"] = {"message_id": int(msg.id)}
                await save()
                await app.send_message(
                    OWNER_ID,
                    "Pocket FM show link reached in Live Cleaner.\n\nDo you want to change the Live metadata before the next files?",
                    reply_markup=InlineKeyboardMarkup([
                        [ib("Yes", "live:link_yes", "start", ButtonStyle.SUCCESS),
                         ib("Skip", "live:link_skip", "cancel")]
                    ])
                )
                continue
            queued_meta = dict(live.get("meta") or newmeta())
            result, reason = await process_file_with_retry(
                "__live__", msg.id, message=msg,
                meta_override=queued_meta, source_client=source_client
            )
            if result == "ok":
                live["stats"]["files_sent"] = live["stats"].get("files_sent", 0) + 1
            await save()
            if live.get("status") == "running":
                await asyncio.sleep(get_delay("__live__"))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Live cleaner failed for message %s", getattr(msg, "id", "?"))
        finally:
            live["current"] = None
            try:
                queue.task_done()
            except Exception:
                pass
            await save()

async def start_live():
    global live_queue, live_task, live_seen_ids
    live = live_record()
    if not live.get("source") or not live.get("target"):
        raise ValueError("Set Live Source and Live Target first.")
    start_id = int(live.get("start_id") or 0)
    if start_id <= 0:
        raise ValueError("Set Live Start ID first.")
    if user_app is None:
        raise ValueError("Connect the Live userbot first with /userbot.")
    await resolve_chat(live["target"])
    source_entity = await user_app.get_entity(int(live["source"]) if re.fullmatch(r"-?\d+", str(live["source"])) else live["source"])
    live["source"] = str(source_entity.id)
    jobs["__live__"]["source"] = live["source"]
    jobs["__live__"]["target"] = live["target"]
    jobs["__live__"]["start_id"] = start_id
    jobs["__live__"]["start"] = start_id
    live["status"] = "starting"
    live["pending_link"] = None
    live["loading_backlog"] = True
    live["ingest_buffer"] = []
    live["backlog_loaded"] = False
    jobs["__live__"]["status"] = "starting"
    if live_queue is None:
        live_queue = asyncio.Queue()
    if live_task is None or live_task.done():
        live_task = asyncio.create_task(live_worker())

    history = []
    async for msg in user_app.iter_messages(int(live["source"])):
        if int(msg.id) < start_id:
            break
        history.append(msg)
    buffered = list(live.get("ingest_buffer") or [])
    live["ingest_buffer"] = []
    merged = {int(m.id): m for m in history}
    for m in buffered:
        if int(m.id) >= start_id:
            merged[int(m.id)] = m
    live_seen_ids = set(merged)
    ordered = [merged[mid] for mid in sorted(merged)]
    for msg in ordered:
        body = " ".join(x for x in [(msg.text or ""), (msg.caption or "")] if x)
        is_link = bool(re.search(r"https?://pocketfm\.com/show(?:/|\b)", body, re.I))
        media = msg.audio or (msg.document if msg.document and (getattr(msg.document, "mime_type", "") or "").startswith("audio/") else None)
        if is_link:
            live["queued"] = live.get("queued", 0) + 1
            await live_queue.put(("link", msg, user_app))
        elif media:
            live["queued"] = live.get("queued", 0) + 1
            await live_queue.put(("audio", msg, user_app))
    live["loading_backlog"] = False
    live["backlog_loaded"] = True
    live["status"] = "running"
    jobs["__live__"]["status"] = "running"
    await save()
    log.info("LIVE BACKLOG LOADED source=%s start_id=%s messages=%s", live["source"], start_id, len(ordered))

async def pause_live():
    live_record()["status"] = "paused"
    jobs["__live__"]["status"] = "paused"
    await save()

async def stop_live():
    global live_task, live_queue
    live_record()["status"] = "stopped"; jobs["__live__"]["status"] = "stopped"
    live_record()["current"] = None
    if live_task and not live_task.done(): live_task.cancel()
    live_task = None; live_queue = None
    await save()

async def ingest_live_message(m, source_client):
    global live_seen_ids
    live = live_record()
    if live.get("status") not in {"starting", "running", "paused"} or live_queue is None:
        return
    try:
        if int(getattr(m.chat, "id", getattr(m, "chat_id", 0))) != int(live.get("source")) or int(m.id) < int(live.get("start_id") or 0):
            return
    except Exception:
        return
    mid = int(m.id)
    if mid in live_seen_ids:
        return
    live_seen_ids.add(mid)
    body = " ".join(x for x in [(getattr(m, "text", "") or ""), (getattr(m, "caption", "") or "")] if x)
    is_link = bool(re.search(r"https?://pocketfm\.com/show(?:/|\b)", body, re.I))
    media = getattr(m, "audio", None) or (m.document if getattr(m, "document", None) and (getattr(m.document, "mime_type", "") or "").startswith("audio/") else None)
    if live.get("loading_backlog"):
        live.setdefault("ingest_buffer", []).append(m)
        return
    if is_link:
        live["queued"] = live.get("queued", 0) + 1
        await live_queue.put(("link", m, source_client))
        log.info("LIVE LINK QUEUED source=%s message=%s queue=%s", m.chat.id, m.id, live["queued"])
        return
    if not media:
        return
    live["queued"] = live.get("queued", 0) + 1
    await live_queue.put(("audio", m, source_client))
    log.info("LIVE AUDIO QUEUED source=%s message=%s queue=%s", m.chat.id, m.id, live["queued"])

@app.on_message(filters.channel, group=-50)
async def live_channel_handler(_, m):
    if user_app is not None:
        return
    await ingest_live_message(m, app)

if user_app is not None:
    user_app.add_handler(
        MessageHandler(lambda client, message: ingest_live_message(message, user_app), filters.channel),
        group=-50
    )

@app.on_message(filters.private, group=-90)
async def field_input(_, m):
    if not allowed(m): return
    text = (m.text or "").strip()
    log.info("TEXT INPUT user=%s text=%s global_field=%s job_field=%s active=%s", m.from_user.id, text, sessions.get((m.from_user.id, "global_field")), sessions.get((m.from_user.id, "field")), sessions.get(m.from_user.id))
    if text.startswith("/"):
        return
    # Handle the two-step Batch link flow before generic metadata input.
    if sessions.get((m.from_user.id, "batch_first")):
        if await handle_batch_link_input(m, text):
            return
    if sessions.get((m.from_user.id, "batch_end")):
        if not re.match(r"^https?://t\.me/(?:c/\d+/|[A-Za-z0-9_]+)/\d+(?:\?.*)?$", text):
            return await m.reply_text("Please send a valid Telegram message link.")
        await finalize_batch_job(m, sessions[(m.from_user.id, "batch_end")], text)
        return
    live_field = sessions.get((m.from_user.id, "live_field"))
    if live_field:
        if live_field == "start_id":
            if not text.isdigit() or int(text) <= 0:
                return await m.reply_text("Live Start ID must be a positive Telegram message ID.")
            live_record()["start_id"] = int(text)
            live_record()["backlog_loaded"] = False
            live_record()["pending_link"] = None
            jobs["__live__"]["start_id"] = int(text)
            sessions.pop((m.from_user.id, "live_field"), None)
            await save()
            return await m.reply_text(f"Live Start ID set to {text}.\nLive Cleaner will start from this message ID.", reply_markup=live_kb())
        if live_field in {"source", "target"}:
            try:
                chat = await resolve_chat(text)
                settings["live"][live_field] = str(chat.id)
                jobs["__live__"][live_field] = str(chat.id)
                sessions.pop((m.from_user.id, "live_field"), None)
                await save()
                return await m.reply_text(f"Live {live_field.title()} set: {chat.title or chat.first_name}", reply_markup=live_kb())
            except Exception as e:
                return await m.reply_text(f"Could not set Live {live_field}: {type(e).__name__}: {e}")
        if live_field in {"artist","genre","year","album","album_artist","comment"}:
            live_record()["meta"][live_field] = text
            sessions.pop((m.from_user.id, "live_field"), None)
            await save()
            return await m.reply_text("Live metadata updated.", reply_markup=live_meta_kb())

    global_field = sessions.get((m.from_user.id, "global_field"))
    if global_field:
        if global_field in {"artist","album","album_artist","year","comment"}:
            sessions[(m.from_user.id, "pending_global")] = (global_field, text)
            sessions.pop((m.from_user.id, "global_field"), None)
            return await m.reply_text(
                f"Set global {global_field.replace('_',' ')} to:\n{text}\n\nConfirm?",
                reply_markup=confirm_kb("global")
            )
        if global_field in {"source", "target"}:
            try:
                c = await resolve_chat(text)
                sessions[(m.from_user.id, "pending_global")] = (global_field, str(c.id))
                sessions.pop((m.from_user.id, "global_field"), None)
                return await m.reply_text(
                    f"{global_field.title()} found:\n{c.title or c.first_name}\nID: {c.id}\n\nConfirm?",
                    reply_markup=confirm_kb("global")
                )
            except Exception as e:
                return await m.reply_text(f"Could not set {global_field}: {type(e).__name__}: {e}")
        if global_field == "delay":
            if not text.isdigit() or not (MIN_DELAY_SECONDS <= int(text) <= MAX_DELAY_SECONDS):
                return await m.reply_text(f"Delay must be between {MIN_DELAY_SECONDS} and {MAX_DELAY_SECONDS} seconds.")
            sessions[(m.from_user.id, "pending_global")] = ("delay", int(text))
            sessions.pop((m.from_user.id, "global_field"), None)
            return await m.reply_text(f"Set episode delay to {text} seconds.\n\nConfirm?", reply_markup=confirm_kb("global"))
        if global_field in {"status", "failed"}:
            jid = text
            sessions.pop((m.from_user.id, "global_field"), None)
            if jid not in jobs:
                return await m.reply_text("Unknown Job ID.", reply_markup=main_kb())
            if global_field == "status":
                return await m.reply_text(summary(jid), reply_markup=job_kb(jid))
            j = jobs[jid]
            failed = "\n".join(f"{i} - {j['failed_reasons'].get(str(i),'unknown')}" for i in j["failed"])
            return await m.reply_text("FAILED\n\n" + (failed or "No failed files."), reply_markup=main_kb())

    field = sessions.get((m.from_user.id, "field")); jid = sessions.get(m.from_user.id) or active(m)
    if not field or not jid or jid not in jobs: return
    if field in {"artist","genre","year","album","album_artist","comment"}:
        sessions[(m.from_user.id, "pending_job")] = (jid, field, text)
        sessions.pop((m.from_user.id, "field"), None)
        return await m.reply_text(
            f"Set {field.replace('_',' ')} for Job {jid} to:\n{text}\n\nConfirm?",
            reply_markup=confirm_kb("job")
        )

async def connect_saved_userbot():
    global user_app
    if USERBOT_SESSION_STRING:
        user_app = TelegramClient(StringSession(USERBOT_SESSION_STRING), USERBOT_API_ID, USERBOT_API_HASH)
    elif USERBOT_SESSION_FILE.exists():
        user_app = TelegramClient(str(USERBOT_SESSION_FILE), USERBOT_API_ID, USERBOT_API_HASH)
    else:
        return False
    await user_app.connect()
    if not await user_app.is_user_authorized():
        await user_app.disconnect()
        user_app = None
        return False
    user_app.add_event_handler(lambda event: ingest_live_message(event.message, user_app), events.NewMessage())
    me = await user_app.get_me()
    log.info("Live userbot connected as %s (%s)", getattr(me, "first_name", ""), me.id)
    return True

async def finish_userbot_qr():
    global user_login_client, user_login_task, user_login_qr, user_app
    try:
        await user_login_qr.wait()
        session = user_login_client.session.save()
        USERBOT_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        USERBOT_SESSION_FILE.write_text(session, encoding="utf-8")
        try:
            os.chmod(USERBOT_SESSION_FILE, 0o600)
        except Exception:
            pass
        user_app = user_login_client
        user_app.add_event_handler(lambda event: ingest_live_message(event.message, user_app), events.NewMessage())
        user_login_client = None
        user_login_qr = None
        await app.send_message(OWNER_ID, "Userbot login successful.\n\nLive Cleaner can now read channel history and live messages.")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("USERBOT QR LOGIN FAILED")
        try:
            if user_login_client: await user_login_client.disconnect()
        except Exception: pass
        user_login_client = None
        user_login_qr = None
        await app.send_message(OWNER_ID, f"Userbot login failed: {type(e).__name__}")
    finally:
        user_login_task = None

@app.on_message(filters.private & filters.command("userbot"))
async def userbot_cmd(_, m):
    global user_login_client, user_login_task, user_login_qr
    if not is_owner(m): return
    if user_app is not None and await user_app.is_user_authorized():
        me = await user_app.get_me()
        return await m.reply_text(f"Userbot connected.\\nAccount: {getattr(me, 'first_name', '')}\\nUser ID: {me.id}")
    if user_login_task and not user_login_task.done():
        return await m.reply_text("A userbot login is already waiting. Complete the Telegram QR login or use /userbot_cancel.")
    user_login_client = TelegramClient(StringSession(), USERBOT_API_ID, USERBOT_API_HASH)
    await user_login_client.connect()
    user_login_qr = await user_login_client.qr_login()
    await m.reply_text(
        "Live Userbot Login\\n\\n"
        "No phone number, OTP or 2FA password is entered into Cleanfi.\\n"
        "Tap the button below and approve the login in Telegram.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Telegram Login", url=user_login_qr.url)]])
    )
    user_login_task = asyncio.create_task(finish_userbot_qr())

@app.on_message(filters.private & filters.command("userbot_cancel"))
async def userbot_cancel_cmd(_, m):
    global user_login_client, user_login_task, user_login_qr
    if not is_owner(m): return
    if user_login_task and not user_login_task.done(): user_login_task.cancel()
    if user_login_client:
        try: await user_login_client.disconnect()
        except Exception: pass
    user_login_client = None; user_login_qr = None; user_login_task = None
    await m.reply_text("Userbot login cancelled.")

def main():
    load()
    log.info("Starting Cleanfi Telegram client...")
    async def runner():
        await app.start()
        try:
            if not await connect_saved_userbot():
                log.info("No connected Live userbot. Owner can use /userbot.")
            await asyncio.Event().wait()
        finally:
            if user_login_task and not user_login_task.done(): user_login_task.cancel()
            if user_app is not None:
                try: await user_app.disconnect()
                except Exception: pass
            if app.is_connected: await app.stop()
    app.run(runner())

if __name__ == "__main__":
    main()
