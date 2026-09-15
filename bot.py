import os, re, json, asyncio, time, uuid
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from metadata import clean_and_apply_metadata

load_dotenv()
API_ID=int(os.environ["API_ID"]); API_HASH=os.environ["API_HASH"]; BOT_TOKEN=os.environ["BOT_TOKEN"]
SOURCE_CHAT_ID=int(os.environ["SOURCE_CHAT_ID"]); TARGET_CHAT_ID=int(os.environ["TARGET_CHAT_ID"])
ADMINS={int(x.strip()) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip()}
WORKERS=max(1,int(os.getenv("MAX_WORKERS","2"))); TEMP=Path(os.getenv("TEMP_DIR","./tmp")); TEMP.mkdir(parents=True,exist_ok=True)
STATE=Path(os.getenv("STATE_FILE","./jobs.json")); jobs={}; running={}; sessions={}
app=Client("cleanfi",api_id=API_ID,api_hash=API_HASH,bot_token=BOT_TOKEN)

def save(): STATE.write_text(json.dumps(jobs,ensure_ascii=False,indent=2),encoding="utf-8")
def load():
 global jobs
 if STATE.exists():
  try: jobs=json.loads(STATE.read_text(encoding="utf-8"))
  except Exception: jobs={}
def allowed(m): return bool(m.from_user and m.from_user.id in ADMINS)
def jid_for(m): return sessions.get(m.from_user.id)
def getjid(m):
 p=(m.text or "").split(); return p[1] if len(p)>1 and p[1] in jobs else jid_for(m)
def new_meta(): return {"title_mode":"original","artist":None,"genre":None,"year":None,"album":None,"album_artist":None,"comment":None,"cover_path":None}
def kb(jid):
 j=jobs[jid]; x=j["meta"]
 return InlineKeyboardMarkup([
 [InlineKeyboardButton(f"Artist: {x.get('artist') or '—'}",callback_data=f"set:artist:{jid}"),InlineKeyboardButton("Edit",callback_data=f"set:artist:{jid}")],
 [InlineKeyboardButton(f"Genre: {x.get('genre') or '—'}",callback_data=f"set:genre:{jid}"),InlineKeyboardButton("Edit",callback_data=f"set:genre:{jid}")],
 [InlineKeyboardButton(f"Year: {x.get('year') or '—'}",callback_data=f"set:year:{jid}"),InlineKeyboardButton("Edit",callback_data=f"set:year:{jid}")],
 [InlineKeyboardButton(f"Album: {x.get('album') or '—'}",callback_data=f"set:album:{jid}"),InlineKeyboardButton("Edit",callback_data=f"set:album:{jid}")],
 [InlineKeyboardButton(f"Album Artist: {x.get('album_artist') or '—'}",callback_data=f"set:album_artist:{jid}")],
 [InlineKeyboardButton(f"Cover: {'✓' if x.get('cover_path') else '—'}",callback_data=f"coverhelp:{jid}"),InlineKeyboardButton("Clear Cover",callback_data=f"clearcover:{jid}")],
 [InlineKeyboardButton("Keep Original Title",callback_data=f"noop:{jid}"),InlineKeyboardButton("START",callback_data=f"start:{jid}")],
 [InlineKeyboardButton("Cancel",callback_data=f"cancel:{jid}")]])

def job_text(jid):
 j=jobs[jid]; x=j["meta"]
 return f"✦ Cleanfi Job {jid}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\n\nArtist: {x.get('artist') or '—'}\nGenre: {x.get('genre') or '—'}\nYear: {x.get('year') or '—'}\nAlbum: {x.get('album') or '—'}\nAlbum Artist: {x.get('album_artist') or '—'}\nCover: {'Attached' if x.get('cover_path') else 'Not attached'}\nTitle: Original source title\n\nProgress: {j['done']}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}"

@app.on_message(filters.private & filters.command("start"))
async def start(_,m):
 if allowed(m): await m.reply_text("✦ Cleanfi\n\n/range START END\n/status [JOB]\n/jobs\n/retry JOB\n/failed JOB\n/cancel JOB\n/test MESSAGE_ID\n/help")
@app.on_message(filters.private & filters.command("help"))
async def help_cmd(_,m):
 if allowed(m): await m.reply_text("Create a job with /range 1250 1300. Configure it with the inline buttons, or /meta artist=\"Name\" genre=\"Romance\" year=2026.\n\nFor artwork: send the image with caption /cover JOBID. The same image is embedded as the audio's cover where the container supports artwork and is also sent to Telegram as its thumbnail/cover preview.\n\nTitle stays unchanged by default. Every job has an independent metadata profile.")
@app.on_message(filters.private & filters.command("range"))
async def range_cmd(_,m):
 if not allowed(m): return
 p=(m.text or "").split()
 if len(p)!=3 or not p[1].isdigit() or not p[2].isdigit() or int(p[1])>int(p[2]): return await m.reply_text("Usage: /range 1250 1300")
 a,b=map(int,p[1:]); jid=uuid.uuid4().hex[:8]
 jobs[jid]={"id":jid,"owner":m.from_user.id,"start":a,"end":b,"total":b-a+1,"done":0,"failed":[],"failed_reasons":{},"skipped":[],"status":"configured","cancel_requested":False,"meta":new_meta(),"created":time.time()}
 sessions[m.from_user.id]=jid; save(); await m.reply_text(job_text(jid),reply_markup=kb(jid))
@app.on_message(filters.private & filters.command("meta"))
async def meta_cmd(_,m):
 if not allowed(m): return
 jid=jid_for(m)
 if not jid: return await m.reply_text("Create a job first with /range.")
 text=(m.text or "")[5:].strip(); pairs=re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))',text)
 if not pairs: return await m.reply_text('Example: /meta artist="Artist A" genre="Romance" year=2026 album="Book"')
 for k,a,b,c in pairs:
  if k in {"artist","genre","year","album","album_artist","comment"}: jobs[jid]["meta"][k]=a or b or c
 save(); await m.reply_text(job_text(jid),reply_markup=kb(jid))
@app.on_message(filters.private & filters.photo)
async def cover(_,m):
 if not allowed(m): return
 cap=(m.caption or "").split(); jid=cap[1] if len(cap)==2 and cap[0].lower()=="/cover" else None
 if not jid or jid not in jobs: return
 p=TEMP/jid; p.mkdir(exist_ok=True); path=p/"cover.jpg"; await m.download(file_name=str(path))
 jobs[jid]["meta"]["cover_path"]=str(path); save(); await m.reply_text(job_text(jid),reply_markup=kb(jid))
@app.on_message(filters.private & filters.command("jobs"))
async def jobs_cmd(_,m):
 if not allowed(m): return
 await m.reply_text("\n".join(f"{j['id']}  {j['start']}-{j['end']}  {j['status']}  {j['done']}/{j['total']}" for j in list(jobs.values())[-20:]) or "No jobs.")
@app.on_message(filters.private & filters.command("status"))
async def status(_,m):
 if not allowed(m): return
 jid=getjid(m)
 if jid not in jobs: return await m.reply_text("Unknown job.")
 await m.reply_text(job_text(jid))
@app.on_message(filters.private & filters.command("failed"))
async def failed(_,m):
 if not allowed(m): return
 jid=getjid(m)
 if jid not in jobs: return await m.reply_text("Unknown job.")
 j=jobs[jid]; await m.reply_text("Failed:\n"+"\n".join(f"{mid} — {j['failed_reasons'].get(str(mid),'unknown')}" for mid in j['failed']) if j['failed'] else "No failed items.")
@app.on_message(filters.private & filters.command("cancel"))
async def cancel(_,m):
 if not allowed(m): return
 jid=getjid(m)
 if jid in jobs: jobs[jid]["cancel_requested"]=True; jobs[jid]["status"]="cancelling"; save(); await m.reply_text(f"Cancellation requested for {jid}.")
@app.on_message(filters.private & filters.command("retry"))
async def retry(_,m):
 if not allowed(m): return
 jid=getjid(m)
 if jid not in jobs: return await m.reply_text("Unknown job.")
 ids=jobs[jid]["failed"][:]
 if not ids: return await m.reply_text("No failed items to retry.")
 jobs[jid]["failed"]=[]; jobs[jid]["failed_reasons"]={}; jobs[jid]["cancel_requested"]=False; jobs[jid]["status"]="queued"; save(); await launch(jid,m,ids)
@app.on_message(filters.private & filters.command("test"))
async def test(_,m):
 if not allowed(m): return
 p=(m.text or "").split()
 if len(p)!=2 or not p[1].isdigit(): return await m.reply_text("Usage: /test MESSAGE_ID")
 jid=uuid.uuid4().hex[:8]; mid=int(p[1]); jobs[jid]={"id":jid,"owner":m.from_user.id,"start":mid,"end":mid,"total":1,"done":0,"failed":[],"failed_reasons":{},"skipped":[],"status":"configured","cancel_requested":False,"meta":new_meta(),"created":time.time()}; sessions[m.from_user.id]=jid; save(); await m.reply_text(job_text(jid),reply_markup=kb(jid))

async def wait_retry(fn):
 while True:
  try: return await fn()
  except FloodWait as e: await asyncio.sleep(e.value+1)
async def process_one(jid,mid):
 j=jobs[jid]; msg=await wait_retry(lambda: app.get_messages(SOURCE_CHAT_ID,mid))
 media=msg.audio or (msg.document if msg.document and (getattr(msg.document,"mime_type","") or "").startswith("audio/") else None)
 if not media: return "skipped","message has no supported audio media"
 name=getattr(media,"file_name",None) or f"message_{mid}.bin"; ext=Path(name).suffix.lower()
 supported={".mp3",".m4a",".mp4",".flac",".ogg",".opus",".wav",".aiff",".aif",".wma",".aac"}
 if ext not in supported: return "skipped",f"unsupported container {ext or 'unknown'}"
 d=TEMP/jid; d.mkdir(exist_ok=True); inp=d/f"{mid}_{Path(name).name}"; out=d/f"out_{mid}_{Path(name).name}"
 try:
  await wait_retry(lambda: msg.download(file_name=str(inp)))
  # Preserve source title. Telegram audio title may differ from embedded title, so use filename stem only when no tag exists.
  source_title=None
  try:
   from mutagen import File as MFile
   f=MFile(str(inp),easy=True); source_title=(f.get("title") or [None])[0] if f else None
  except Exception: pass
  await asyncio.to_thread(clean_and_apply_metadata,str(inp),str(out),title=source_title,artist=j["meta"].get("artist"),genre=j["meta"].get("genre"),year=j["meta"].get("year"),cover=j["meta"].get("cover_path"),album=j["meta"].get("album"),album_artist=j["meta"].get("album_artist"),comment=j["meta"].get("comment"))
  await upload_audio(jid,out,name,msg)
  return "done",None
 finally:
  for x in (inp,out):
   try: x.unlink()
   except FileNotFoundError: pass
async def upload_audio(jid,path,name,msg):
 # Telegram's audio upload gets a thumbnail via thumb=, while the embedded cover is carried inside the file.
 j=jobs[jid]; thumb=j["meta"].get("cover_path")
 kwargs={"audio":str(path),"caption":msg.caption or "","file_name":name}
 if thumb and Path(thumb).is_file(): kwargs["thumb"]=thumb
 await wait_retry(lambda: app.send_audio(TARGET_CHAT_ID,**kwargs))
async def launch(jid,m,ids=None):
 if jid in running: return await m.reply_text("Job is already running.")
 running[jid]=asyncio.create_task(run_job(jid,m,ids)); await m.reply_text(f"Job {jid} queued.")
async def run_job(jid,m,ids=None):
 j=jobs[jid]; j["status"]="running"; save(); ids=ids or list(range(j["start"],j["end"]+1)); q=asyncio.Queue()
 for mid in ids: await q.put(mid)
 async def worker():
  while not q.empty():
   mid=await q.get()
   try:
    if j.get("cancel_requested"): continue
    result,reason=await process_one(jid,mid)
    if result=="done": j["done"]+=1
    elif result=="skipped": j["skipped"].append(mid)
    else: j["failed"].append(mid); j["failed_reasons"][str(mid)]=reason or "unknown"
    save()
   except Exception as e:
    j["failed"].append(mid); j["failed_reasons"][str(mid)]=f"{type(e).__name__}: {e}"; save()
   finally: q.task_done()
 tasks=[asyncio.create_task(worker()) for _ in range(min(WORKERS,len(ids)))]
 await q.join(); [t.cancel() for t in tasks]
 j["status"]="cancelled" if j.get("cancel_requested") else ("completed" if not j["failed"] else "completed_with_failures"); save(); running.pop(jid,None)
 await m.reply_text(job_text(jid))
@app.on_callback_query()
async def cb(_,q:CallbackQuery):
 if not q.from_user or q.from_user.id not in ADMINS: return await q.answer("Not authorized",show_alert=True)
 a,*r=q.data.split(":"); jid=r[-1] if r else None
 if jid not in jobs: return await q.answer("Unknown job",show_alert=True)
 if a=="start": await q.answer(); await launch(jid,q.message); return
 if a=="cancel": jobs[jid]["cancel_requested"]=True; jobs[jid]["status"]="cancelling"; save(); return await q.answer("Cancellation requested")
 if a=="noop": return await q.answer("Title remains original")
 if a=="coverhelp": return await q.answer("Send image: /cover JOBID",show_alert=True)
 if a=="clearcover": jobs[jid]["meta"]["cover_path"]=None; save(); return await q.message.edit_text(job_text(jid),reply_markup=kb(jid))
 if a=="set":
  field=r[0]; sessions[q.from_user.id]=jid; sessions[(q.from_user.id,"field")]=field; await q.answer(); return await q.message.reply_text(f"Send {field} value as the next message.")
@app.on_message(filters.private & filters.text)
async def input_field(_,m):
 if not allowed(m): return
 field=sessions.get((m.from_user.id,"field")); jid=jid_for(m)
 if not field or jid not in jobs or (m.text or "").startswith("/"): return
 if field in {"artist","genre","year","album","album_artist","comment"}:
  jobs[jid]["meta"][field]=m.text.strip(); sessions.pop((m.from_user.id,"field"),None); save(); await m.reply_text(job_text(jid),reply_markup=kb(jid))

load(); app.run()
