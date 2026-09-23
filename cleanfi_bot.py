import os, re, json, asyncio, time, uuid, logging
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ButtonStyle
from pyrogram.errors import FloodWait
from pyrogram.handlers import RawUpdateHandler
from pyrogram.file_id import FileId
from dotenv import load_dotenv
from mutagen import File as MFile
from metadata import clean_and_apply_metadata, read_original_title

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMINS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
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
jobs, settings, sessions, running = {}, {}, {}, {}
state_lock = None
tg_lock = None
flood_until = 0.0
job_queue = None
queue_task = None
queued_jobs = set()


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
    settings.setdefault("target", os.getenv("TARGET_CHAT_ID", ""))
    settings.setdefault("retries", DEFAULT_RETRIES)
    settings.setdefault("file_delay", DEFAULT_DELAY_SECONDS)
    settings["file_delay"] = min(MAX_DELAY_SECONDS, max(MIN_DELAY_SECONDS, int(settings["file_delay"])))
    settings.setdefault("min_free_gb", MIN_FREE_DISK_GB)
    settings.setdefault("global_meta", {"artist": None, "genre": None, "year": None, "album": None, "album_artist": None, "comment": None, "cover_path": None})
    for j in jobs.values():
        j.setdefault("source", settings["source"]); j.setdefault("target", settings["target"])
        j.setdefault("processed", []); j.setdefault("failed", []); j.setdefault("skipped", [])
        j.setdefault("failed_reasons", {}); j.setdefault("started_at", None)
        j.setdefault("progress_message_id", None); j.setdefault("last_progress", 0)
        j.setdefault("current", None); j.setdefault("flood_until", 0)
        if j.get("status") in {"running", "queued", "cancelling"}:
            j["status"] = "paused"
        j.setdefault("retries", 0)

def allowed(m):
    return bool(m.from_user and m.from_user.id in ADMINS)

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

def main_kb():
    return InlineKeyboardMarkup([
        [ib("Set Artist", "m:artist", "artist", ButtonStyle.PRIMARY),
         ib("Set Cover", "m:cover", "cover", ButtonStyle.PRIMARY)],
        [ib("Global Metadata", "m:meta", "genre", ButtonStyle.PRIMARY)],
        [ib("New Job", "m:new", "new", ButtonStyle.PRIMARY),
         ib("Jobs", "m:jobs", "jobs", ButtonStyle.PRIMARY)],
        [ib("Set Source", "m:source", "source", ButtonStyle.PRIMARY),
         ib("Set Target", "m:target", "target", ButtonStyle.PRIMARY)],
        [ib("Status", "m:status", "status", ButtonStyle.PRIMARY),
         ib("Failed", "m:failed", "failed", ButtonStyle.DANGER)],
        [ib("Help", "m:help", "help", ButtonStyle.PRIMARY)],
        [ib("Delay", "m:delay", "delay", ButtonStyle.PRIMARY)],
    ])

def confirm_kb(kind):
    return InlineKeyboardMarkup([
        [ib("Confirm", f"confirm:{kind}", "start", ButtonStyle.SUCCESS),
         ib("Cancel", f"cancel_confirm:{kind}", "cancel", ButtonStyle.DANGER)]
    ])

def global_meta_kb():
    return InlineKeyboardMarkup([
        [ib("Set Artist", "gm:artist", "artist", ButtonStyle.PRIMARY),
         ib("Set Album", "gm:album", "genre", ButtonStyle.PRIMARY)],
        [ib("Set Album Artist", "gm:album_artist", "artist", ButtonStyle.PRIMARY),
         ib("Set Genre", "gm:genre", "genre", ButtonStyle.PRIMARY)],
        [ib("Set Year", "gm:year", "status", ButtonStyle.PRIMARY),
         ib("Set Comment", "gm:comment", "help", ButtonStyle.PRIMARY)],
        [ib("Back", "gm:back", "cancel", ButtonStyle.DANGER)]
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
        [ib(f"Cover: {'Attached' if m.get('cover_path') else 'Not set'}", f"cover:{jid}", "cover", ButtonStyle.PRIMARY),
         ib("Clear", f"clear:{jid}", "cancel", ButtonStyle.DANGER)],
        [ib("START", f"start:{jid}", "start", ButtonStyle.SUCCESS),
         ib("Cancel", f"cancel:{jid}", "cancel", ButtonStyle.DANGER)],
    ])

def summary(jid):
    j = jobs[jid]; m = j["meta"]
    return (f"Cleanfi Job {jid}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\n"
            f"Source: {j.get('source') or 'Not set'}\nTarget: {j.get('target') or 'Not set'}\n\n"
            f"Artist: {m.get('artist') or '—'}\nGenre: {m.get('genre') or '—'}\nYear: {m.get('year') or '—'}\n"
            f"Album: {m.get('album') or '—'}\nAlbum Artist: {m.get('album_artist') or '—'}\n"
            f"Comment: {m.get('comment') or '—'}\nCover: {'Attached' if m.get('cover_path') else 'Not attached'}\n"
            f"Title: Original source title\nDelay: {get_delay(jid)}s between episodes\n\nProcessed: {len(j['processed'])}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}")

async def create_batch_job(owner, first_link, end_link):
    jid = uuid.uuid4().hex[:8]
    jobs[jid] = {"id": jid, "owner": owner, "first_link": first_link, "end_link": end_link, "start": None, "end": None,
                 "total": 0, "processed": [], "failed": [], "failed_reasons": {}, "skipped": [], "status": "configured",
                 "cancel_requested": False, "meta": newmeta(), "source": settings["source"], "target": settings["target"],
                 "created": time.time(), "started_at": None, "progress_message_id": None, "current": None,
                 "flood_until": 0, "queue_ids": None, "delay_seconds": int(settings.get("file_delay", DEFAULT_DELAY_SECONDS)),
                 "start_post": {"image_path": None, "caption": None, "sent": False}, "end_post_sent": False}
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
    await tg_call(lambda: app.send_photo(int(j["target"]), photo=path, caption=caption), jid, "start post")
    post["sent"] = True
    await save()
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

@app.on_message(filters.private & filters.photo)
async def startpost_photo(_, m):
    if not allowed(m): return
    if not sessions.get((m.from_user.id, "startpost")):
        return
    jid = sessions.get(m.from_user.id)
    if not jid or jid not in jobs:
        sessions.pop((m.from_user.id, "startpost"), None)
        return await m.reply_text("Unknown job. Create/select the job again.")
    caption = (m.caption or "").strip()
    if not caption:
        return await m.reply_text("Please send the image with the story name as the caption.")
    if len(caption) > 1024:
        return await m.reply_text("Story name/caption is too long. Telegram allows up to 1024 characters here.")
    d = TEMP / jid
    d.mkdir(exist_ok=True)
    path = d / "start_post.jpg"
    await tg_call(lambda: m.download(file_name=str(path)), jid, "start post image download")
    jobs[jid]["start_post"] = {"image_path": str(path), "caption": caption, "sent": False}
    sessions.pop((m.from_user.id, "startpost"), None)
    await save()
    await m.reply_text(f"Start post saved for job {jid}.\nCaption: {caption}")
    m.stop_propagation()

@app.on_message(filters.private & filters.command("batch"))
async def batch_cmd(_, m):
    if not allowed(m): return
    if not settings.get("source") or not settings.get("target"):
        return await m.reply_text("Set /source and /target first.")
    jid = await create_batch_job(m.from_user.id, "", "")
    sessions[(m.from_user.id, "batch_first")] = jid
    await m.reply_text("Batch Mode\n\nSend me the first audio post link (e.g., https://t.me/channel/123).")

@app.on_message(filters.private & filters.command("globalmeta"))
async def globalmeta_cmd(_, m):
    if not allowed(m): return
    pairs = re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))', (m.text or "")[11:].strip())
    if not pairs:
        g = settings.get("global_meta", {})
        return await m.reply_text("Global: " + json.dumps(g, ensure_ascii=False) + "\nUse /globalmeta artist=\"Name\" album_artist=\"Name\" album=\"Story\" genre=\"Romance\" year=2026")
    g = settings.setdefault("global_meta", {})
    for k,a,b,c in pairs:
        if k in {"artist","genre","year","album","album_artist","comment"}: g[k] = a or b or c
    await save()
    await m.reply_text("Global metadata saved. New jobs will inherit these values.")

@app.on_message(filters.private & filters.text)
async def batch_link_input(_, m):
    if not allowed(m): return
    text = (m.text or "").strip()
    if not text.startswith("https://t.me/"): return
    jid = sessions.get((m.from_user.id, "batch_first")) or sessions.get((m.from_user.id, "batch_end"))
    if not jid or jid not in jobs: return
    if sessions.get((m.from_user.id, "batch_first")) == jid:
        jobs[jid]["first_link"] = text
        sessions.pop((m.from_user.id, "batch_first"), None)
        sessions[(m.from_user.id, "batch_end")] = jid
        await save()
        return await m.reply_text("Batch Mode\n\nSend me the end audio post link (e.g., https://t.me/channel/123).")
    jobs[jid]["end_link"] = text
    sessions.pop((m.from_user.id, "batch_end"), None)
    await save()
    await m.reply_text("Batch configured. Press START to fetch all source messages and begin.", reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("meta"))
async def meta_cmd(_, m):
    if not allowed(m): return
    jid = active(m)
    if not jid: return await m.reply_text("Create a job first with /range.")
    pairs = re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))', (m.text or "")[5:].strip())
    if not pairs: return await m.reply_text('Example: /meta artist="Artist A" genre="Romance" year=2026 album="Book"')
    for k, a, b, c in pairs:
        if k in jobs[jid]["meta"]: jobs[jid]["meta"][k] = a or b or c
    await save(); await m.reply_text(summary(jid), reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.photo)
async def cover_photo(_, m):
    if not allowed(m): return
    if sessions.get((m.from_user.id, "global_cover")):
        path = TEMP / "global_cover.jpg"
        await tg_call(lambda: m.download(file_name=str(path)), None, "global cover download")
        settings.setdefault("global_meta", {})["cover_path"] = str(path)
        sessions.pop((m.from_user.id, "global_cover"), None)
        await save()
        return await m.reply_text("Global cover saved.", reply_markup=main_kb())
    if sessions.get((m.from_user.id, "startpost")):
        return
    p = (m.caption or "").split(); jid = p[1] if len(p) == 2 and p[0].lower() == "/cover" else active(m)
    if not jid or jid not in jobs: return
    d = TEMP / jid; d.mkdir(exist_ok=True); path = d / "cover.jpg"
    await tg_call(lambda: m.download(file_name=str(path)), jid, "cover download")
    jobs[jid]["meta"]["cover_path"] = str(path); await save()
    await m.reply_text(summary(jid), reply_markup=job_kb(jid))

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
    if jid in jobs:
        jobs[jid]["cancel_requested"] = True
        if jobs[jid].get("status") == "queued":
            queued_jobs.discard(jid)
            jobs[jid]["status"] = "cancelled"
        else:
            jobs[jid]["status"] = "cancelling"
        await save()
        await m.reply_text(f"Cancellation requested: {jid}")

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

async def download_media_direct(media, path, jid):
    """Download through Pyrogram's media generator so FloodWait is not swallowed by Message.download()."""
    file_id = FileId.decode(media.file_id)
    written = 0
    with open(path, "wb") as fp:
        async for chunk in app.get_file(file_id, file_size=getattr(media, "file_size", 0) or 0):
            fp.write(chunk)
            written += len(chunk)
    if written <= 0:
        raise RuntimeError("Telegram returned an empty media download")
    return path

async def process_file(jid, mid):
    j = jobs[jid]
    if MIN_FREE_DISK_GB and __import__("shutil").disk_usage(TEMP).free < MIN_FREE_DISK_GB * 1024**3:
        return "failed", f"Low disk space: less than {MIN_FREE_DISK_GB} GB free"
    msg = await tg_call(lambda: app.get_messages(int(j["source"]), mid), jid, "get_messages")
    media = msg.audio or (msg.document if msg.document and (getattr(msg.document, "mime_type", "") or "").startswith("audio/") else None)
    if not media: return "skip", "message has no audio media"
    name = infer_name(media)
    ext = Path(name).suffix.lower()
    supported = {".mp3",".m4a",".mp4",".flac",".ogg",".opus",".wav",".aiff",".aif",".wma",".aac"}
    if ext not in supported: return "skip", f"unsupported container {ext or 'unknown'}"
    if ext == ".aac": return "skip", "raw AAC has no portable metadata container"
    d = TEMP / jid; d.mkdir(exist_ok=True)
    inp = d / f"{mid}_{Path(name).name}"; out = d / f"out_{mid}_{Path(name).name}"
    try:
        await download_media_direct(media, str(inp), jid)
        title = read_original_title(str(inp))
        if title is None:
            try:
                f = MFile(str(inp), easy=True); title = (f.get("title") or [None])[0] if f else None
            except Exception: title = None
        title = title if title is not None else Path(name).stem
        await asyncio.to_thread(clean_and_apply_metadata, str(inp), str(out), title=title,
            artist=j["meta"].get("artist"), genre=j["meta"].get("genre"), year=j["meta"].get("year"),
            cover=j["meta"].get("cover_path"), album=j["meta"].get("album"),
            album_artist=j["meta"].get("album_artist"), comment=j["meta"].get("comment"))
        kw = {"audio": str(out), "caption": msg.caption or "", "file_name": name, "title": str(title)}
        if j["meta"].get("artist"): kw["performer"] = str(j["meta"]["artist"])
        cp = j["meta"].get("cover_path")
        if cp and Path(cp).is_file(): kw["thumb"] = cp
        await tg_call(lambda: app.send_audio(int(j["target"]), **kw), jid, "upload")
        return "ok", None
    finally:
        for p in (inp, out):
            try: p.unlink()
            except FileNotFoundError: pass

async def process_file_with_retry(jid, mid):
    attempt = 0
    while True:
        try:
            return await process_file(jid, mid)
        except FloodWait:
            raise
        except Exception:
            attempt += 1
            jobs[jid]["retries"] = jobs[jid].get("retries", 0) + 1
            await save()
            if attempt > int(settings.get("retries", DEFAULT_RETRIES)):
                raise
            await asyncio.sleep(min(30, 2 ** attempt))

async def launch(jid, m, ids=None):
    global job_queue, queue_task
    if not ids and jobs[jid].get("first_link") and jobs[jid].get("end_link"):
        ids = await fetch_batch_ids(jid)
    if jid in running or jid in queued_jobs:
        return await m.reply_text(f"Job {jid} is already running or queued.")
    if len(queued_jobs) >= MAX_QUEUE:
        return await m.reply_text(f"Queue is full ({MAX_QUEUE} jobs). Start it after a slot is available.")
    if jobs[jid].get("status") in {"completed", "completed_with_failures", "cancelled"} and not ids:
        # A completed job can be started again; processed/skipped IDs remain protected from duplicates.
        pass
    if job_queue is None:
        job_queue = asyncio.Queue()
    jobs[jid]["queue_ids"] = list(ids) if ids else None
    jobs[jid]["status"] = "queued"
    jobs[jid]["cancel_requested"] = False
    queued_jobs.add(jid)
    await job_queue.put(jid)
    if queue_task is None or queue_task.done():
        queue_task = asyncio.create_task(queue_worker())
    await save()
    position = list(queued_jobs).index(jid) + 1
    await m.reply_text(f"Job {jid} queued. Queue position: {position}\nOnly one job runs at a time; the next job starts automatically after this job ends.")

async def queue_worker():
    while True:
        jid = await job_queue.get()
        queued_jobs.discard(jid)
        if jid not in jobs:
            job_queue.task_done(); continue
        if jobs[jid].get("cancel_requested"):
            jobs[jid]["status"] = "cancelled"
            await save(); job_queue.task_done(); continue
        try:
            running[jid] = True
            await run_job(jid, jobs[jid]["queue_ids"])
        except Exception as e:
            j = jobs.get(jid)
            if j:
                j["status"] = "failed_queue"
                j["failed_reasons"]["__job__"] = f"{type(e).__name__}: {e}"
                await save()
                try: await app.send_message(j["owner"], f"Job {jid} stopped unexpectedly: {type(e).__name__}: {e}")
                except Exception: pass
        finally:
            running.pop(jid, None)
            if jid in jobs and jobs[jid].get("status") == "running":
                jobs[jid]["status"] = "completed_with_failures" if jobs[jid]["failed"] else "completed"
            await save()
            job_queue.task_done()

async def run_job(jid, ids=None):
    j = jobs[jid]
    j["status"] = "running"
    j["started_at"] = j.get("started_at") or time.time()
    j["flood_until"] = 0
    j["flood_label"] = None
    await save()
    ids = ids if ids is not None else list(range(j["start"], j["end"] + 1))
    completed = set(j["processed"]) | set(j["skipped"])
    ids = [i for i in ids if i not in completed]
    await safe_progress(jid, True)
    try:
        if j.get("start_post", {}).get("image_path") and not j.get("start_post", {}).get("sent"):
            try:
                await post_start(jid)
            except Exception as e:
                log.exception("Start post failed for job %s", jid)
                try:
                    await app.send_message(j["owner"], f"Start post failed for job {jid}: {type(e).__name__}: {e}\nThe file task will continue.")
                except Exception:
                    pass

        for mid in ids:
            if j.get("cancel_requested"):
                break
            j["current"] = mid
            await safe_progress(jid, True)

            result = None
            reason = None
            try:
                while True:
                    try:
                        result, reason = await process_file_with_retry(jid, mid)
                        break
                    except FloodWait as e:
                        seconds = max(1, int(e.value))
                        j["status"] = "paused"
                        j["flood_until"] = time.time() + seconds
                        j["flood_label"] = "Telegram FloodWait"
                        await save()
                        await safe_progress(jid, True)
                        log.warning("FloodWait on job %s for %ss; pausing and retrying message %s", jid, seconds, mid)
                        try:
                            await app.send_message(
                                j["owner"],
                                f"Job {jid} paused due to Telegram FloodWait.\nWaiting {seconds}s, then the same file will retry automatically."
                            )
                        except Exception:
                            pass
                        await asyncio.sleep(seconds + 1)
                        j["flood_until"] = 0
                        j["flood_label"] = None
                        j["status"] = "running"
                        await save()
                        await safe_progress(jid, True)

                if result == "ok":
                    if mid not in j["processed"]:
                        j["processed"].append(mid)
                elif result == "skip":
                    if mid not in j["skipped"]:
                        j["skipped"].append(mid)
                else:
                    if mid not in j["failed"]:
                        j["failed"].append(mid)
                    j["failed_reasons"][str(mid)] = reason or "unknown"

            except Exception as e:
                if mid not in j["failed"]:
                    j["failed"].append(mid)
                j["failed_reasons"][str(mid)] = f"{type(e).__name__}: {e}"
                log.exception("File processing failed: job=%s message=%s", jid, mid)

            finally:
                j["current"] = None
                await save()
                await safe_progress(jid, True)
                if not j.get("cancel_requested"):
                    await asyncio.sleep(get_delay(jid))

        if j.get("cancel_requested"):
            j["status"] = "cancelled"
        elif j["failed"]:
            j["status"] = "completed_with_failures"
        else:
            j["status"] = "completed"

    finally:
        j["current"] = None
        await save()
        await safe_progress(jid, True)
        try:
            await post_end(jid)
        except Exception:
            log.exception("End post failed for job %s", jid)
        if j.get("status") in {"completed", "completed_with_failures"}:
            try:
                await app.send_message(j["owner"], f"Job {jid} finished with status: {j['status']}.")
            except Exception:
                pass

@app.on_callback_query()
async def callbacks(_, q: CallbackQuery):
    if not q.from_user or q.from_user.id not in ADMINS:
        return await q.answer("Not authorized", show_alert=True)
    parts = q.data.split(":"); action = parts[0]

    if action == "m":
        sub = parts[1]
        await q.answer()
        if sub == "new":
            jid = await create_batch_job(q.from_user.id, "", "")
            sessions[(q.from_user.id, "batch_first")] = jid
            return await q.message.reply_text("Batch Mode\n\nSend me the first audio post link (e.g., https://t.me/channel/123).")
        if sub == "jobs":
            rows = [f"{x} - {j['status']} - {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-20:]]
            return await q.message.reply_text("JOBS\n\n" + ("\n".join(rows) or "No jobs."), reply_markup=main_kb())
        if sub == "artist":
            sessions[q.from_user.id] = None
            sessions[(q.from_user.id, "global_field")] = "artist"
            return await q.message.reply_text("Send global artist name.")
        if sub in {"source", "target", "delay"}:
            sessions[q.from_user.id] = None
            sessions[(q.from_user.id, "global_field")] = sub
            prompts = {"source":"Send source channel username or ID.", "target":"Send target channel username or ID.", "delay":"Send delay in seconds (3-60)."}
            return await q.message.reply_text(prompts[sub])
        if sub == "meta":
            return await q.message.reply_text("Global metadata", reply_markup=global_meta_kb())
        if sub == "cover":
            sessions[(q.from_user.id, "global_cover")] = True
            return await q.message.reply_text("Send the cover image now.")
        if sub == "status":
            sessions[(q.from_user.id, "global_field")] = "status"
            return await q.message.reply_text("Send the Job ID.")
        if sub == "failed":
            sessions[(q.from_user.id, "global_field")] = "failed"
            return await q.message.reply_text("Send the Job ID.")
        if sub == "help":
            return await q.message.reply_text("Use /batch to create a Batch job. Set Source and Target from the menu. Set Artist and Cover are global defaults. Delay controls the seconds between files.")

    if action == "gm":
        sub = parts[1]
        if sub == "back":
            await q.answer()
            return await q.message.edit_text("Welcome to Cleanfi", reply_markup=main_kb())
        if sub in {"artist","album","album_artist","genre","year","comment"}:
            sessions[q.from_user.id] = None
            sessions[(q.from_user.id, "global_field")] = sub
            if sub == "genre":
                rows = [[ib(x, f"gg:{i}", "genre", ButtonStyle.PRIMARY)] for i,x in enumerate(GENRE_OPTIONS)]
                rows.append([ib("Custom", "gg:custom", "genre", ButtonStyle.PRIMARY)])
                await q.answer()
                return await q.message.reply_text("Choose global genre:", reply_markup=InlineKeyboardMarkup(rows))
            await q.answer()
            return await q.message.reply_text(f"Send global {sub.replace('_',' ')}.")
    if action == "gg":
        value = parts[1]
        if value == "custom":
            sessions[q.from_user.id] = None
            sessions[(q.from_user.id, "global_field")] = "genre"
            await q.answer()
            return await q.message.reply_text("Send custom global genre.")
        settings.setdefault("global_meta", {})["genre"] = GENRE_OPTIONS[int(value)]
        await save()
        sessions.pop((q.from_user.id, "global_field"), None)
        await q.answer(f"Global genre set: {GENRE_OPTIONS[int(value)]}")
        return await q.message.reply_text("Global metadata saved.", reply_markup=global_meta_kb())
    if action == "confirm":
        kind = parts[1] if len(parts) > 1 else ""
        await q.answer("Confirmed")
        return await q.message.reply_text(f"{kind.title()} confirmed.", reply_markup=main_kb())
    if action == "cancel_confirm":
        await q.answer("Cancelled")
        return await q.message.reply_text("Cancelled.", reply_markup=main_kb())

    jid = parts[-1]
    if jid not in jobs: return await q.answer("Unknown job", show_alert=True)
    if action == "start":
        await q.answer()
        return await q.message.reply_text(
            f"Confirm start of Job {jid}?\n\n{summary(jid)}",
            reply_markup=InlineKeyboardMarkup([
                [ib("Confirm", f"confirm_start:{jid}", "start", ButtonStyle.SUCCESS),
                 ib("Cancel", f"cancel_confirm:{jid}", "cancel", ButtonStyle.DANGER)]
            ])
        )
    if action == "confirm_start":
        await q.answer("Starting")
        return await launch(jid, q.message)
    if action == "cancel_confirm":
        await q.answer("Cancelled")
        return await q.message.edit_text(summary(jid), reply_markup=job_kb(jid))
    if action == "cancel":
        jobs[jid]["cancel_requested"] = True
        if jobs[jid].get("status") == "queued":
            queued_jobs.discard(jid); jobs[jid]["status"] = "cancelled"
        else: jobs[jid]["status"] = "cancelling"
        await save(); return await q.answer("Cancellation requested")
    if action == "clear": jobs[jid]["meta"]["cover_path"] = None; await save(); return await q.message.edit_text(summary(jid), reply_markup=job_kb(jid))
    if action == "cover": return await q.answer(f"Send image with caption /cover {jid}", show_alert=True)
    if action == "genre":
        await q.answer()
        rows = [[InlineKeyboardButton(x, callback_data=f"g:{jid}:{i}")] for i,x in enumerate(GENRE_OPTIONS)]
        rows.append([InlineKeyboardButton("Custom", callback_data=f"gc:{jid}")])
        return await q.message.reply_text("Choose genre:", reply_markup=InlineKeyboardMarkup(rows))
    if action == "g":
        idx = int(parts[2])
        jobs[jid]["meta"]["genre"] = GENRE_OPTIONS[idx]
        await save()
        await q.answer(f"Genre: {GENRE_OPTIONS[idx]}")
        return await q.message.edit_text(summary(jid), reply_markup=job_kb(jid))
    if action == "gc":
        sessions[q.from_user.id] = jid
        sessions[(q.from_user.id, "field")] = "genre"
        await q.answer()
        return await q.message.reply_text("Send custom genre.")
    if action == "s":
        field = parts[1]; sessions[q.from_user.id] = jid; sessions[(q.from_user.id, "field")] = field
        await q.answer(); return await q.message.reply_text(f"Send {field} value as the next message.")

@app.on_message(filters.private & filters.text)
async def field_input(_, m):
    if not allowed(m): return
    text = (m.text or "").strip()
    if text.startswith("/"): return
    global_field = sessions.get((m.from_user.id, "global_field"))
    if global_field:
        if global_field in {"artist","album","album_artist","year","comment"}:
            settings.setdefault("global_meta", {})[global_field] = text
            await save()
            sessions.pop((m.from_user.id, "global_field"), None)
            return await m.reply_text(f"Global {global_field.replace('_',' ')} saved: {text}", reply_markup=global_meta_kb())
        if global_field in {"source", "target"}:
            try:
                c = await resolve_chat(text)
                settings[global_field] = str(c.id)
                await save()
                sessions.pop((m.from_user.id, "global_field"), None)
                return await m.reply_text(f"{global_field.title()} saved.\n{c.title or c.first_name}\nID: {c.id}", reply_markup=main_kb())
            except Exception as e:
                return await m.reply_text(f"Could not set {global_field}: {type(e).__name__}: {e}")
        if global_field == "delay":
            if not text.isdigit() or not (MIN_DELAY_SECONDS <= int(text) <= MAX_DELAY_SECONDS):
                return await m.reply_text(f"Delay must be between {MIN_DELAY_SECONDS} and {MAX_DELAY_SECONDS} seconds.")
            settings["file_delay"] = int(text)
            await save()
            sessions.pop((m.from_user.id, "global_field"), None)
            return await m.reply_text(f"Delay set to {text} seconds.", reply_markup=main_kb())
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

    field = sessions.get((m.from_user.id, "field")); jid = active(m)
    if not field or jid not in jobs: return
    if field in {"artist","genre","year","album","album_artist","comment"}:
        jobs[jid]["meta"][field] = text
        sessions.pop((m.from_user.id, "field"), None)
        await save()
        await m.reply_text(summary(jid), reply_markup=job_kb(jid))

def main():
    load()
    log.info("Starting Cleanfi Telegram client...")
    app.run()

if __name__ == "__main__":
    main()
