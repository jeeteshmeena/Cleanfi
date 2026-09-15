import os
import re
import json
import asyncio
import time
import uuid
from pathlib import Path
from typing import Optional
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, RPCError
from dotenv import load_dotenv
from metadata import clean_and_apply_metadata

load_dotenv()
API_ID=int(os.environ["API_ID"]); API_HASH=os.environ["API_HASH"]; BOT_TOKEN=os.environ["BOT_TOKEN"]
SOURCE_CHAT_ID=int(os.environ["SOURCE_CHAT_ID"]); TARGET_CHAT_ID=int(os.environ["TARGET_CHAT_ID"])
ADMINS={int(x.strip()) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip()}
WORKERS=max(1,int(os.getenv("MAX_WORKERS","2"))); TEMP=Path(os.getenv("TEMP_DIR","./tmp")); TEMP.mkdir(parents=True,exist_ok=True)
STATE=Path(os.getenv("STATE_FILE","./jobs.json")); lock=asyncio.Lock(); jobs={}; running={}; sessions={}

app=Client("cleanfi",api_id=API_ID,api_hash=API_HASH,bot_token=BOT_TOKEN)

def save():
    STATE.write_text(json.dumps(jobs,ensure_ascii=False,indent=2),encoding="utf-8")

def load():
    global jobs
    if STATE.exists():
        try: jobs=json.loads(STATE.read_text(encoding="utf-8"))
        except Exception: jobs={}

def allowed(m): return m.from_user and m.from_user.id in ADMINS

def parse_range(s):
    p=s.split()
    if len(p)!=3 or not p[1].isdigit() or not p[2].isdigit(): return None
    a,b=map(int,p[1:]); return (a,b) if a<=b else None

def meta_kb(jid):
    j=jobs[jid]; return InlineKeyboardMarkup([
      [InlineKeyboardButton(f"Artist: {j['meta'].get('artist') or '—'}",callback_data=f"noop:{jid}"),InlineKeyboardButton("Set",callback_data=f"set:artist:{jid}")],
      [InlineKeyboardButton(f"Genre: {j['meta'].get('genre') or '—'}",callback_data=f"noop:{jid}"),InlineKeyboardButton("Set",callback_data=f"set:genre:{jid}")],
      [InlineKeyboardButton(f"Year: {j['meta'].get('year') or '—'}",callback_data=f"noop:{jid}"),InlineKeyboardButton("Set",callback_data=f"set:year:{jid}")],
      [InlineKeyboardButton(f"Cover: {'Yes' if j['meta'].get('cover_path') else 'No'}",callback_data=f"set:cover:{jid}"),InlineKeyboardButton("Album",callback_data=f"set:album:{jid}")],
      [InlineKeyboardButton("Keep Original Title",callback_data=f"noop:{jid}"),InlineKeyboardButton("START",callback_data=f"start:{jid}")],
      [InlineKeyboardButton("Cancel",callback_data=f"cancel:{jid}")]])

@app.on_message(filters.command("start") & filters.private)
async def start(_,m):
    if not allowed(m): return
    await m.reply_text("✦ Cleanfi\n\nUse /range START END to select messages, then configure metadata and press START.\n\n/help for commands.")

@app.on_message(filters.command("help") & filters.private)
async def help_cmd(_,m):
    if not allowed(m): return
    await m.reply_text("/range START END — create a job\n/meta key=value ... — configure current job\n/jobs — list jobs\n/status [JOB] — status\n/startjob JOB — start\n/cancel JOB — cancel\n/retry JOB — retry failed items\n/failed JOB — failed message IDs\n/test MESSAGE_ID — process one file\n\nKeys: artist, genre, year, album, album_artist, comment. Title stays original unless explicitly extended later.\nSend a cover image with caption /cover JOB to attach artwork.")

@app.on_message(filters.command("range") & filters.private)
async def range_cmd(_,m):
    if not allowed(m): return
    r=parse_range(m.text or "")
    if not r: return await m.reply_text("Usage: /range 1250 1300")
    a,b=r; jid=str(uuid.uuid4())[:8]
    jobs[jid]={"id":jid,"owner":m.from_user.id,"start":a,"end":b,"next":a,"total":b-a+1,"done":0,"failed":[],"skipped":[],"status":"configured","meta":{"title_mode":"original","artist":None,"genre":None,"year":None,"album":None,"album_artist":None,"comment":None,"cover_path":None},"created":time.time()}
    save(); sessions[m.from_user.id]=jid
    await m.reply_text(f"✦ Job {jid}\nRange: {a} → {b}\n\nConfigure metadata below. This job has its own metadata profile.",reply_markup=meta_kb(jid))

@app.on_message(filters.command("meta") & filters.private)
async def meta_cmd(_,m):
    if not allowed(m): return
    jid=sessions.get(m.from_user.id)
    if not jid or jid not in jobs: return await m.reply_text("Create/select a job first with /range.")
    text=(m.text or "")[5:].strip(); pairs=re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))',text)
    if not pairs: return await m.reply_text('Example: /meta artist="Artist A" genre="Romance" year=2026')
    allowed_keys={"artist","genre","year","album","album_artist","comment"}
    for k,a,b,c in pairs:
        if k in allowed_keys: jobs[jid]["meta"][k]=a or b or c
    save(); await m.reply_text("Metadata updated for this job.",reply_markup=meta_kb(jid))

@app.on_message(filters.photo & filters.private)
async def cover_photo(_,m):
    if not allowed(m): return
    caption=(m.caption or "").split()
    if len(caption)!=2 or caption[0].lower()!="/cover": return
    jid=caption[1]
    if jid not in jobs: return await m.reply_text("Unknown job.")
    p=TEMP/jid; p.mkdir(exist_ok=True); path=p/"cover.jpg"
    await m.download(file_name=str(path)); jobs[jid]["meta"]["cover_path"]=str(path); save()
    await m.reply_text(f"Cover attached to job {jid}.",reply_markup=meta_kb(jid))

@app.on_message(filters.command("jobs") & filters.private)
async def jobs_cmd(_,m):
    if not allowed(m): return
    if not jobs: return await m.reply_text("No jobs.")
    lines=[f"{j['id']}  {j['start']}-{j['end']}  {j['status']}  {j['done']}/{j['total']}" for j in list(jobs.values())[-15:]]
    await m.reply_text("\n".join(lines))

@app.on_message(filters.command("status") & filters.private)
async def status(_,m):
    if not allowed(m): return
    jid=(m.text or "").split()[1] if len((m.text or "").split())>1 else sessions.get(m.from_user.id)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    j=jobs[jid]; await m.reply_text(f"Job {jid}\nStatus: {j['status']}\nProgress: {j['done']}/{j['total']}\nFailed: {len(j['failed'])}\nSkipped: {len(j['skipped'])}")

@app.on_message(filters.command("startjob") & filters.private)
async def startjob(_,m):
    if not allowed(m): return
    jid=(m.text or "").split()[1] if len((m.text or "").split())>1 else sessions.get(m.from_user.id)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    await launch(jid,m)

@app.on_message(filters.command("cancel") & filters.private)
async def cancel(_,m):
    if not allowed(m): return
    jid=(m.text or "").split()[1] if len((m.text or "").split())>1 else sessions.get(m.from_user.id)
    if jid in jobs:
        jobs[jid]["cancel_requested"]=True; jobs[jid]["status"]="cancelling"; save(); await m.reply_text(f"Cancellation requested for {jid}.")

@app.on_message(filters.command("failed") & filters.private)
async def failed(_,m):
    if not allowed(m): return
    jid=(m.text or "").split()[1] if len((m.text or "").split())>1 else sessions.get(m.from_user.id)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    await m.reply_text("Failed IDs:\n"+(", ".join(map(str,jobs[jid]["failed"])) or "None"))

@app.on_message(filters.command("retry") & filters.private)
async def retry(_,m):
    if not allowed(m): return
    jid=(m.text or "").split()[1] if len((m.text or "").split())>1 else sessions.get(m.from_user.id)
    if jid not in jobs: return await m.reply_text("Unknown job.")
    j=jobs[jid]; ids=j["failed"][:]; j["retry_ids"]=ids; j["failed"]=[]; j["status"]="queued"; j["cancel_requested"]=False; save(); await launch(jid,m,retry_ids=ids)

@app.on_message(filters.command("test") & filters.private)
async def test(_,m):
    if not allowed(m): return
    p=(m.text or "").split();
    if len(p)!=2 or not p[1].isdigit(): return await m.reply_text("Usage: /test MESSAGE_ID")
    jid=str(uuid.uuid4())[:8]; mid=int(p[1]); jobs[jid]={"id":jid,"owner":m.from_user.id,"start":mid,"end":mid,"next":mid,"total":1,"done":0,"failed":[],"skipped":[],"status":"configured","meta":{"title_mode":"original","artist":None,"genre":None,"year":None,"album":None,"album_artist":None,"comment":None,"cover_path":None},"created":time.time()}; sessions[m.from_user.id]=jid; save(); await m.reply_text(f"Test job {jid}",reply_markup=meta_kb(jid))

async def safe_download(msg,path):
    while True:
        try: return await msg.download(file_name=str(path))
        except FloodWait as e: await asyncio.sleep(e.value+1)

async def process_one(jid,mid):
    j=jobs[jid]
    msg=await app.get_messages(SOURCE_CHAT_ID,mid)
    media=msg.audio or msg.document or msg.video
    if not media: return "skipped","not-media"
    name=getattr(media,"file_name",None) or f"message_{mid}.bin"
    ext=Path(name).suffix.lower()
    if ext not in {".mp3",".m4a",".mp4",".flac",".ogg",".opus",".wav",".aiff",".aif",".wma",".aac"}: return "skipped","unsupported-extension"
    d=TEMP/jid; d.mkdir(exist_ok=True); inp=d/f"{mid}_{Path(name).name}"; out=d/f"out_{mid}_{Path(name).name}"
    try:
        await safe_download(msg,inp)
        cover=j["meta"].get("cover_path")
        clean_and_apply_metadata(str(inp),str(out),title=None,artist=j["meta"].get("artist"),genre=j["meta"].get("genre"),year=j["meta"].get("year"),cover=cover,album=j["meta"].get("album"),album_artist=j["meta"].get("album_artist"),comment=j["meta"].get("comment"))
        await upload_media(jid,out,name,msg.caption)
        return "done",None
    finally:
        for x in (inp,out):
            try: x.unlink()
            except FileNotFoundError: pass

async def upload_media(jid,path,name,caption):
    while True:
        try:
            await app.send_audio(TARGET_CHAT_ID,str(path),caption=caption,file_name=name)
            return
        except FloodWait as e: await asyncio.sleep(e.value+1)

async def launch(jid,m,retry_ids=None):
    if jid in running: return await m.reply_text("Job is already running.")
    running[jid]=asyncio.create_task(run_job(jid,m,retry_ids)); await m.reply_text(f"Job {jid} queued. Use /status {jid}.")

async def run_job(jid,m,retry_ids=None):
    j=jobs[jid]; j["status"]="running"; save(); sem=asyncio.Semaphore(WORKERS)
    ids=retry_ids or list(range(j["start"],j["end"]+1))
    async def one(mid):
        async with sem:
            if j.get("cancel_requested"): return
            try: result,reason=await process_one(jid,mid)
            except FloodWait as e: await asyncio.sleep(e.value+1); result,reason=await process_one(jid,mid)
            except Exception as e: result,reason="failed",f"{type(e).__name__}: {e}"
            if result=="done": j["done"]+=1
            elif result=="skipped": j["skipped"].append(mid)
            else: j["failed"].append(mid)
            j["next"]=mid+1; save()
    await asyncio.gather(*(one(x) for x in ids))
    j["status"]="cancelled" if j.get("cancel_requested") else ("completed" if not j["failed"] else "completed_with_failures"); save(); running.pop(jid,None)
    await m.reply_text(f"✦ Job {jid} finished\nProcessed: {j['done']}\nSkipped: {len(j['skipped'])}\nFailed: {len(j['failed'])}")

@app.on_callback_query()
async def callbacks(_,q:CallbackQuery):
    if not q.from_user or q.from_user.id not in ADMINS: return await q.answer("Not authorized",show_alert=True)
    action,*rest=q.data.split(":"); jid=rest[-1] if rest else None
    if jid not in jobs: return await q.answer("Unknown job",show_alert=True)
    if action=="start": await q.answer(); await launch(jid,q.message); return
    if action=="cancel": jobs[jid]["cancel_requested"]=True; jobs[jid]["status"]="cancelling"; save(); await q.answer("Cancellation requested"); return
    if action=="noop": return await q.answer()
    if action=="set":
        field=rest[0]; sessions[q.from_user.id]=jid
        sessions[(q.from_user.id,"field")]=field
        await q.answer(); await q.message.reply_text(f"Send the new {field} value as a message.\nFor cover, send an image with caption /cover {jid}.")

@app.on_message(filters.text & filters.private)
async def field_input(_,m):
    if not allowed(m): return
    field=sessions.get((m.from_user.id,"field")); jid=sessions.get(m.from_user.id)
    if not field or jid not in jobs or (m.text or "").startswith("/"): return
    if field in {"artist","genre","year","album","album_artist","comment"}:
        jobs[jid]["meta"][field]=m.text.strip(); save(); sessions.pop((m.from_user.id,"field"),None); await m.reply_text("Updated.",reply_markup=meta_kb(jid))

load()
app.run()
