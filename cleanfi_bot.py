import os, re, json, asyncio, time, uuid
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
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
FILE_DELAY = max(1, int(os.getenv("FILE_DELAY_SECONDS", "3")))
MAX_QUEUE = max(1, int(os.getenv("MAX_QUEUED_JOBS", "20")))

app = Client("cleanfi", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
jobs, settings, sessions = {}, {}, {}
running = set(); queued_jobs = set()
state_lock = asyncio.Lock(); tg_lock = asyncio.Lock()
job_queue = None; queue_task = None
global_flood_until = 0.0; global_flood_label = ""


def save_sync():
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"settings": settings, "jobs": jobs}, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE)

async def save():
    async with state_lock:
        await asyncio.to_thread(save_sync)

def load():
    global jobs, settings
    try:
        data = json.loads(STATE.read_text(encoding="utf-8"))
        settings = data.get("settings", {}); jobs = data.get("jobs", {})
    except Exception:
        settings, jobs = {}, {}
    settings.setdefault("source", os.getenv("SOURCE_CHAT_ID", "")); settings.setdefault("target", os.getenv("TARGET_CHAT_ID", ""))
    settings.setdefault("file_delay", max(1, int(os.getenv("FILE_DELAY_SECONDS", "3"))))
    for j in jobs.values():
        j.setdefault("source", settings["source"]); j.setdefault("target", settings["target"])
        j.setdefault("processed", []); j.setdefault("failed", []); j.setdefault("skipped", []); j.setdefault("failed_reasons", {})
        j.setdefault("started_at", None); j.setdefault("progress_message_id", None); j.setdefault("last_progress", 0)
        j.setdefault("current", None); j.setdefault("flood_until", 0); j.setdefault("flood_label", "")
        j.setdefault("cancel_requested", False); j.setdefault("queue_ids", None)
        if j.get("status") in {"running", "queued", "cancelling"}: j["status"] = "paused"

def allowed(m): return bool(m.from_user and m.from_user.id in ADMINS)
def active(m): return sessions.get(m.from_user.id)
def getjid(m):
    p = (m.text or "").split(); return p[1] if len(p) > 1 and p[1] in jobs else active(m)
def delay(): return max(1, int(settings.get("file_delay", FILE_DELAY)))
def newmeta(): return {"title_mode":"original","artist":None,"genre":None,"year":None,"album":None,"album_artist":None,"comment":None,"cover_path":None}

def is_flood_error(exc):
    if isinstance(exc, FloodWait): return True
    s = f"{type(exc).__name__}: {exc}".upper()
    return "FLOOD_WAIT" in s or "FLOODWAIT" in s or "A WAIT OF" in s and "REQUIRED" in s

def flood_seconds(exc):
    if isinstance(exc, FloodWait): return max(1, int(exc.value))
    s = str(exc)
    m = re.search(r"(?:wait of|waiting)\s+(\d+)\s*seconds?", s, re.I)
    return max(1, int(m.group(1))) if m else 10

async def mark_flood(jid, seconds, label):
    global global_flood_until, global_flood_label
    until = time.time() + seconds
    global_flood_until = max(global_flood_until, until); global_flood_label = label
    if jid in jobs:
        jobs[jid]["flood_until"] = max(jobs[jid].get("flood_until", 0), until)
        jobs[jid]["flood_label"] = label
        await save()
    try:
        print(f"FloodWait: {label}; pausing {seconds}s; SAME file will be retried")
    except Exception: pass
    await asyncio.sleep(seconds + 1)

async def tg_call(fn, jid=None, label="Telegram"):
    while True:
        try:
            async with tg_lock:
                return await fn()
        except Exception as e:
            if not is_flood_error(e): raise
            await mark_flood(jid, flood_seconds(e), label)

async def resolve_chat(value):
    value = str(value).strip()
    return await tg_call(lambda: app.get_chat(int(value) if re.fullmatch(r"-?\d+", value) else value), label="get_chat")

def flood_text(jid):
    until = max(global_flood_until, jobs[jid].get("flood_until", 0))
    if until > time.time(): return f"Waiting {max(1, int(until-time.time()))}s"
    return "Protected"

def progress_text(jid):
    j=jobs[jid]; done=len(j["processed"])+len(j["failed"])+len(j["skipped"]); total=j["total"]
    pct=int(done*100/total) if total else 100; width=16; filled=int(width*pct/100)
    bar="█"*filled+"░"*(width-filled); elapsed=max(0,time.time()-(j.get("started_at") or time.time()))
    speed=done/(elapsed/60) if done and elapsed else 0; eta=((total-done)/speed*60) if speed else 0
    cur=f"\nCurrent: #{j['current']}" if j.get("current") else ""
    return (f"Cleanfi Processing\nJob: {jid}{cur}\n\n{bar} {pct}%\nFiles: {done} / {total}\n"
            f"Processed: {len(j['processed'])}\nFailed: {len(j['failed'])}\nSkipped: {len(j['skipped'])}\n"
            f"Speed: {speed:.1f} files/min\nElapsed: {int(elapsed//60)}m {int(elapsed%60)}s\n"
            f"ETA: {int(eta//60)}m {int(eta%60)}s\nFloodWait: {flood_text(jid)}\nDelay: {delay()}s/file")

async def safe_progress(jid, force=False):
    j=jobs[jid]; now=time.time()
    if not force and now-j.get("last_progress",0)<2: return
    j["last_progress"]=now
    try:
        if j.get("progress_message_id"):
            await tg_call(lambda: app.edit_message_text(j["owner"],j["progress_message_id"],progress_text(jid)),jid,"progress")
        else:
            msg=await tg_call(lambda: app.send_message(j["owner"],progress_text(jid)),jid,"progress"); j["progress_message_id"]=msg.id
        await save()
    except Exception as e: print(f"Progress update failed: {type(e).__name__}: {e}")

def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("New Job",callback_data="m:new"),InlineKeyboardButton("Jobs",callback_data="m:jobs")],
        [InlineKeyboardButton("Source",callback_data="m:source"),InlineKeyboardButton("Target",callback_data="m:target")],
        [InlineKeyboardButton("Settings",callback_data="m:settings"),InlineKeyboardButton("Status",callback_data="m:status")],
        [InlineKeyboardButton("Failed",callback_data="m:failed"),InlineKeyboardButton("Help",callback_data="m:help")]
    ])

def settings_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Delay: {delay()}s",callback_data="set:delay")],
        [InlineKeyboardButton("Source",callback_data="m:source"),InlineKeyboardButton("Target",callback_data="m:target")],
        [InlineKeyboardButton("Back",callback_data="m:back")]
    ])

def job_kb(jid):
    m=jobs[jid]["meta"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Artist: {m.get('artist') or '—'}",callback_data=f"s:artist:{jid}")],
        [InlineKeyboardButton(f"Genre: {m.get('genre') or '—'}",callback_data=f"s:genre:{jid}")],
        [InlineKeyboardButton(f"Year: {m.get('year') or '—'}",callback_data=f"s:year:{jid}")],
        [InlineKeyboardButton(f"Album: {m.get('album') or '—'}",callback_data=f"s:album:{jid}")],
        [InlineKeyboardButton(f"Album Artist: {m.get('album_artist') or '—'}",callback_data=f"s:album_artist:{jid}")],
        [InlineKeyboardButton(f"Comment: {m.get('comment') or '—'}",callback_data=f"s:comment:{jid}")],
        [InlineKeyboardButton(f"Cover: {'Attached' if m.get('cover_path') else 'Not set'}",callback_data=f"cover:{jid}"),InlineKeyboardButton("Clear",callback_data=f"clear:{jid}")],
        [InlineKeyboardButton("START",callback_data=f"start:{jid}"),InlineKeyboardButton("Cancel",callback_data=f"cancel:{jid}")]
    ])

def summary(jid):
    j=jobs[jid]; m=j["meta"]
    return (f"Cleanfi Job {jid}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\n"
            f"Source: {j.get('source') or 'Not set'}\nTarget: {j.get('target') or 'Not set'}\n\n"
            f"Artist: {m.get('artist') or '—'}\nGenre: {m.get('genre') or '—'}\nYear: {m.get('year') or '—'}\n"
            f"Album: {m.get('album') or '—'}\nAlbum Artist: {m.get('album_artist') or '—'}\nComment: {m.get('comment') or '—'}\n"
            f"Cover: {'Attached' if m.get('cover_path') else 'Not attached'}\nTitle: Original source title\n\n"
            f"Processed: {len(j['processed'])}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}")

async def create_job(owner,start,end):
    jid=uuid.uuid4().hex[:8]
    jobs[jid]={"id":jid,"owner":owner,"start":start,"end":end,"total":end-start+1,"processed":[],"failed":[],"failed_reasons":{},"skipped":[],"status":"configured","cancel_requested":False,"meta":newmeta(),"source":settings["source"],"target":settings["target"],"created":time.time(),"started_at":None,"progress_message_id":None,"last_progress":0,"current":None,"flood_until":0,"flood_label":"","queue_ids":None}
    sessions[owner]=jid; await save(); return jid

@app.on_message(filters.private & filters.command("start"))
async def start_cmd(_,m):
    if allowed(m): await m.reply_text("CLEANFI\n\nAudiobook metadata cleaner and repacker.",reply_markup=main_kb())

@app.on_message(filters.private & filters.command("help"))
async def help_cmd(_,m):
    if allowed(m): await m.reply_text("CLEANFI\n\n/range START END\n/source @channel\n/target @channel\n/meta artist=\"Name\" genre=\"Romance\" year=2026\n/cover JOBID\n/startjob JOBID\n/status JOBID\n/cancel JOBID\n/retry JOBID\n/failed JOBID\n/test MESSAGE_ID\n/jobs\n/settings",reply_markup=main_kb())

@app.on_message(filters.private & filters.command("source"))
async def source_cmd(_,m):
    if not allowed(m): return
    p=(m.text or "").split(maxsplit=1)
    if len(p)!=2:return await m.reply_text("Usage: /source @channelusername")
    try:
        c=await resolve_chat(p[1]); settings["source"]=str(c.id); await save(); await m.reply_text(f"Source set\n{c.title or c.first_name}\nID: {c.id}",reply_markup=main_kb())
    except Exception as e: await m.reply_text(f"Could not set source: {type(e).__name__}: {e}")

@app.on_message(filters.private & filters.command("target"))
async def target_cmd(_,m):
    if not allowed(m): return
    p=(m.text or "").split(maxsplit=1)
    if len(p)!=2:return await m.reply_text("Usage: /target @channelusername")
    try:
        c=await resolve_chat(p[1]); settings["target"]=str(c.id); await save(); await m.reply_text(f"Target set\n{c.title or c.first_name}\nID: {c.id}",reply_markup=main_kb())
    except Exception as e: await m.reply_text(f"Could not set target: {type(e).__name__}: {e}")

@app.on_message(filters.private & filters.command("range"))
async def range_cmd(_,m):
    if not allowed(m):return
    p=(m.text or "").split()
    if len(p)!=3 or not p[1].isdigit() or not p[2].isdigit() or int(p[1])>int(p[2]):return await m.reply_text("Usage: /range 1250 1300")
    if not settings.get("source") or not settings.get("target"):return await m.reply_text("Set source and target first.",reply_markup=main_kb())
    jid=await create_job(m.from_user.id,int(p[1]),int(p[2])); await m.reply_text(summary(jid),reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("meta"))
async def meta_cmd(_,m):
    if not allowed(m):return
    jid=active(m)
    if not jid:return await m.reply_text("Create a job first with /range.")
    pairs=re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))',(m.text or "")[5:].strip())
    if not pairs:return await m.reply_text('Example: /meta artist="Artist A" genre="Romance" year=2026')
    for k,a,b,c in pairs:
        if k in jobs[jid]["meta"]:jobs[jid]["meta"][k]=a or b or c
    await save();await m.reply_text(summary(jid),reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.photo)
async def cover_photo(_,m):
    if not allowed(m):return
    p=(m.caption or "").split();jid=p[1] if len(p)==2 and p[0].lower()=="/cover" else active(m)
    if not jid or jid not in jobs:return
    d=TEMP/jid;d.mkdir(exist_ok=True);path=d/"cover.jpg"
    await tg_call(lambda:m.download(file_name=str(path)),jid,"cover download");jobs[jid]["meta"]["cover_path"]=str(path);await save();await m.reply_text(summary(jid),reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("cover"))
async def cover_cmd(_,m):
    if allowed(m):
        jid=getjid(m)
        if jid in jobs:await m.reply_text(f"Send the image with caption /cover {jid}")

@app.on_message(filters.private & filters.command("jobs"))
async def jobs_cmd(_,m):
    if allowed(m):
        rows=[f"{x} — {j['status']} — {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-20:]]
        await m.reply_text("CLEANFI JOBS\n\n"+("\n".join(rows) or "No jobs."),reply_markup=main_kb())

@app.on_message(filters.private & filters.command("status"))
async def status_cmd(_,m):
    if not allowed(m):return
    jid=getjid(m)
    if jid not in jobs:return await m.reply_text("Unknown job.")
    await m.reply_text(progress_text(jid) if jobs[jid]["status"] in {"running","cancelling","queued"} else summary(jid),reply_markup=job_kb(jid))

@app.on_message(filters.private & filters.command("failed"))
async def failed_cmd(_,m):
    if not allowed(m):return
    jid=getjid(m)
    if jid not in jobs:return await m.reply_text("Unknown job.")
    j=jobs[jid];text="\n".join(f"{i} — {j['failed_reasons'].get(str(i),'unknown')}" for i in j["failed"])
    await m.reply_text("FAILED\n\n"+(text or "No failed files."))

@app.on_message(filters.private & filters.command("cancel"))
async def cancel_cmd(_,m):
    if not allowed(m):return
    jid=getjid(m)
    if jid not in jobs:return await m.reply_text("Unknown job.")
    j=jobs[jid];j["cancel_requested"]=True
    if j.get("status")=="queued":queued_jobs.discard(jid);j["status"]="cancelled"
    else:j["status"]="cancelling"
    await save();await m.reply_text(f"Cancellation requested: {jid}")

@app.on_message(filters.private & filters.command("retry"))
async def retry_cmd(_,m):
    if not allowed(m):return
    jid=getjid(m)
    if jid not in jobs:return await m.reply_text("Unknown job.")
    ids=jobs[jid]["failed"][:]
    if not ids:return await m.reply_text("No failed files.")
    jobs[jid]["failed"]=[];jobs[jid]["failed_reasons"]={};jobs[jid]["cancel_requested"]=False;await save();await launch(jid,ids)

@app.on_message(filters.private & filters.command("startjob"))
async def startjob_cmd(_,m):
    if allowed(m):
        jid=getjid(m)
        if jid in jobs:await launch(jid)

@app.on_message(filters.private & filters.command("settings"))
async def settings_cmd(_,m):
    if allowed(m):await m.reply_text(f"CLEANFI SETTINGS\n\nDelay between files: {delay()} seconds\nFloodWait: automatic pause and same-file retry\nQueue: FIFO, one job at a time",reply_markup=settings_kb())

@app.on_message(filters.private & filters.command("test"))
async def test_cmd(_,m):
    if not allowed(m):return
    p=(m.text or "").split()
    if len(p)!=2 or not p[1].isdigit():return await m.reply_text("Usage: /test MESSAGE_ID")
    if not settings.get("source") or not settings.get("target"):return await m.reply_text("Set source and target first.")
    jid=await create_job(m.from_user.id,int(p[1]),int(p[1]));await m.reply_text(summary(jid),reply_markup=job_kb(jid))

async def process_file_once(jid,mid):
    j=jobs[jid]
    msg=await tg_call(lambda:app.get_messages(int(j["source"]),mid),jid,"get_messages")
    media=msg.audio or (msg.document if msg.document and (getattr(msg.document,"mime_type","") or "").startswith("audio/") else None)
    if not media:return "skip","message has no audio media"
    name=getattr(media,"file_name",None) or f"message_{mid}.bin";ext=Path(name).suffix.lower()
    supported={".mp3",".m4a",".mp4",".flac",".ogg",".opus",".wav",".aiff",".aif",".wma",".aac"}
    if ext not in supported:return "skip",f"unsupported container {ext or 'unknown'}"
    d=TEMP/jid;d.mkdir(exist_ok=True);inp=d/f"{mid}_{Path(name).name}";out=d/f"out_{mid}_{Path(name).name}"
    try:
        await tg_call(lambda:msg.download(file_name=str(inp)),jid,"download")
        title=read_original_title(str(inp))
        if title is None:
            try:
                f=MFile(str(inp),easy=True);title=(f.get("title") or [None])[0] if f else None
            except Exception:title=None
        title=title if title is not None else Path(name).stem
        await asyncio.to_thread(clean_and_apply_metadata,str(inp),str(out),title=title,artist=j["meta"].get("artist"),genre=j["meta"].get("genre"),year=j["meta"].get("year"),cover=j["meta"].get("cover_path"),album=j["meta"].get("album"),album_artist=j["meta"].get("album_artist"),comment=j["meta"].get("comment"))
        kw={"audio":str(out),"caption":msg.caption or "","file_name":name,"title":str(title)}
        if j["meta"].get("artist"):kw["performer"]=str(j["meta"]["artist"])
        cp=j["meta"].get("cover_path")
        if cp and Path(cp).is_file():kw["thumb"]=cp
        await tg_call(lambda:app.send_audio(int(j["target"]),**kw),jid,"upload")
        return "ok",None
    finally:
        for p in (inp,out):
            try:p.unlink()
            except FileNotFoundError:pass

async def process_file(jid,mid):
    # Hard rule: FloodWait retries the exact same message forever and can NEVER enter failed[].
    while True:
        try:return await process_file_once(jid,mid)
        except Exception as e:
            if is_flood_error(e):
                await mark_flood(jid,flood_seconds(e),"file processing");continue
            raise

async def launch(jid,ids=None):
    global job_queue,queue_task
    if jid in running or jid in queued_jobs:return
    if len(queued_jobs)>=MAX_QUEUE:raise RuntimeError(f"Queue is full ({MAX_QUEUE} jobs)")
    if job_queue is None:job_queue=asyncio.Queue()
    jobs[jid]["queue_ids"]=list(ids) if ids is not None else None;jobs[jid]["status"]="queued";jobs[jid]["cancel_requested"]=False
    queued_jobs.add(jid);await job_queue.put(jid)
    if queue_task is None or queue_task.done():queue_task=asyncio.create_task(queue_worker())
    await save()

async def queue_worker():
    while True:
        jid=await job_queue.get();queued_jobs.discard(jid)
        try:
            if jid not in jobs:continue
            j=jobs[jid]
            if j.get("cancel_requested"):j["status"]="cancelled";continue
            running.add(jid);await run_job(jid,j.get("queue_ids"))
        except Exception as e:
            if jid in jobs:
                jobs[jid]["status"]="failed_queue";jobs[jid]["failed_reasons"]["__job__"]=f"{type(e).__name__}: {e}";await save()
        finally:
            running.discard(jid);await save();job_queue.task_done()

async def run_job(jid,ids=None):
    j=jobs[jid];j["status"]="running";j["started_at"]=j.get("started_at") or time.time();await save();ids=ids if ids is not None else list(range(j["start"],j["end"]+1))
    completed=set(j["processed"])|set(j["skipped"]);ids=[i for i in ids if i not in completed];await safe_progress(jid,True)
    for mid in ids:
        if j.get("cancel_requested"):break
        j["current"]=mid;await safe_progress(jid,True)
        try:
            result,reason=await process_file(jid,mid)
            if result=="ok":
                if mid not in j["processed"]:j["processed"].append(mid)
            elif result=="skip":
                if mid not in j["skipped"]:j["skipped"].append(mid)
            else:
                if mid not in j["failed"]:j["failed"].append(mid)
                j["failed_reasons"][str(mid)]=reason or "unknown"
        except Exception as e:
            # Only non-FloodWait errors can reach this branch.
            if mid not in j["failed"]:j["failed"].append(mid)
            j["failed_reasons"][str(mid)]=f"{type(e).__name__}: {e}"
        finally:
            j["current"]=None;await save();await safe_progress(jid,True)
        if j.get("cancel_requested"):break
        await asyncio.sleep(delay())
    j["status"]="cancelled" if j.get("cancel_requested") else ("completed_with_failures" if j["failed"] else "completed")
    await save();await safe_progress(jid,True)
    try:await app.send_message(j["owner"],summary(jid))
    except Exception:pass

@app.on_callback_query()
async def callbacks(_,q:CallbackQuery):
    if not q.from_user or q.from_user.id not in ADMINS:return await q.answer("Not authorized",show_alert=True)
    parts=q.data.split(":");action=parts[0]
    if action=="m":
        sub=parts[1];await q.answer()
        if sub=="back":return await q.message.edit_text("CLEANFI",reply_markup=main_kb())
        if sub=="new":return await q.message.reply_text("Use /range START END to create a job.")
        if sub=="jobs":return await q.message.reply_text("Use /jobs for recent jobs.")
        if sub=="source":return await q.message.reply_text("Use /source @channelusername")
        if sub=="target":return await q.message.reply_text("Use /target @channelusername")
        if sub=="settings":return await q.message.edit_text(f"CLEANFI SETTINGS\n\nDelay between files: {delay()} seconds",reply_markup=settings_kb())
        if sub=="status":return await q.message.reply_text("Use /status JOB_ID")
        if sub=="failed":return await q.message.reply_text("Use /failed JOB_ID")
        if sub=="help":return await q.message.reply_text("Use /help for commands.")
    if action=="set" and parts[1]=="delay":
        await q.answer();sessions[q.from_user.id]="__settings__";sessions[(q.from_user.id,"field")]= "delay";return await q.message.reply_text(f"Send delay in seconds. Current: {delay()}s\nMinimum: 1 second.")
    jid=parts[-1]
    if jid not in jobs:return await q.answer("Unknown job",show_alert=True)
    if action=="start":
        await q.answer("Queued");await launch(jid);return
    if action=="cancel":
        jobs[jid]["cancel_requested"]=True
        if jobs[jid].get("status")=="queued":queued_jobs.discard(jid);jobs[jid]["status"]="cancelled"
        else:jobs[jid]["status"]="cancelling"
        await save();return await q.answer("Cancellation requested")
    if action=="clear":jobs[jid]["meta"]["cover_path"]=None;await save();return await q.message.edit_text(summary(jid),reply_markup=job_kb(jid))
    if action=="cover":return await q.answer(f"Send image with caption /cover {jid}",show_alert=True)
    if action=="s":
        field=parts[1];sessions[q.from_user.id]=jid;sessions[(q.from_user.id,"field")]=field;await q.answer();return await q.message.reply_text(f"Send {field} value as the next message.")

@app.on_message(filters.private & filters.text)
async def field_input(_,m):
    if not allowed(m):return
    field=sessions.get((m.from_user.id,"field"));jid=active(m)
    if not field or (m.text or "").startswith("/"):return
    if field=="delay":
        try:v=int((m.text or "").strip());assert v>=1
        except Exception:return await m.reply_text("Send a whole number of seconds, minimum 1.")
        settings["file_delay"]=v;sessions.pop((m.from_user.id,"field"),None);sessions.pop(m.from_user.id,None);await save();return await m.reply_text(f"Delay updated to {v} seconds per file.",reply_markup=settings_kb())
    if jid in jobs and field in {"artist","genre","year","album","album_artist","comment"}:
        jobs[jid]["meta"][field]=m.text.strip();sessions.pop((m.from_user.id,"field"),None);await save();await m.reply_text(summary(jid),reply_markup=job_kb(jid))

def main():load();app.run()
if __name__=="__main__":main()
