import os,re,json,asyncio,time,uuid
from pathlib import Path
from pyrogram import Client,filters
from pyrogram.types import InlineKeyboardMarkup,InlineKeyboardButton,CallbackQuery
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from mutagen import File as MFile
from metadata import clean_and_apply_metadata,read_original_title

load_dotenv(); API_ID=int(os.environ['API_ID']); API_HASH=os.environ['API_HASH']; BOT_TOKEN=os.environ['BOT_TOKEN']
ADMINS={int(x.strip()) for x in os.getenv('ADMIN_IDS','').split(',') if x.strip()}; WORKERS=max(1,min(4,int(os.getenv('MAX_WORKERS','2'))))
TEMP=Path(os.getenv('TEMP_DIR','./tmp')); TEMP.mkdir(parents=True,exist_ok=True); STATE=Path(os.getenv('STATE_FILE','./jobs.json')); jobs={}; settings={}; running={}; sessions={}; lock=asyncio.Lock(); app=Client('cleanfi',api_id=API_ID,api_hash=API_HASH,bot_token=BOT_TOKEN)

def save_sync():
 t=STATE.with_suffix('.tmp'); t.write_text(json.dumps({'settings':settings,'jobs':jobs},ensure_ascii=False,indent=2),encoding='utf-8'); t.replace(STATE)
async def save():
 async with lock: await asyncio.to_thread(save_sync)
def load():
 global jobs,settings
 try:
  d=json.loads(STATE.read_text(encoding='utf-8')); settings=d.get('settings',{}); jobs=d.get('jobs',{})
 except Exception: settings={}; jobs={}
 settings.setdefault('source',os.getenv('SOURCE_CHAT_ID','')); settings.setdefault('target',os.getenv('TARGET_CHAT_ID',''))
 for j in jobs.values():
  j.setdefault('source',settings['source']); j.setdefault('target',settings['target']); j.setdefault('processed',[]); j.setdefault('failed',[]); j.setdefault('skipped',[]); j.setdefault('failed_reasons',{}); j.setdefault('progress_message_id',None); j.setdefault('started_at',None)
  if j.get('status') in {'running','queued','cancelling'}: j['status']='paused'
def allowed(m): return bool(m.from_user and m.from_user.id in ADMINS)
def active(m): return sessions.get(m.from_user.id)
def getjid(m):
 p=(m.text or '').split(); return p[1] if len(p)>1 and p[1] in jobs else active(m)
def newmeta(): return {'title_mode':'original','artist':None,'genre':None,'year':None,'album':None,'album_artist':None,'comment':None,'cover_path':None}
async def tg(fn):
 while True:
  try:return await fn()
  except FloodWait as e: await asyncio.sleep(int(e.value)+1)
async def resolve(v): return await tg(lambda:app.get_chat(int(v) if re.fullmatch(r'-?\d+',str(v).strip()) else str(v).strip()))
async def cinfo(label,v):
 try:
  c=await resolve(v); return f"{label}: {c.title or c.first_name}\nID: {c.id}"+(f"\nUsername: @{c.username}" if getattr(c,'username',None) else '')
 except Exception as e:return f'{label}: {v or "Not configured"}\nError: {e}'
def main_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton('✦ New Job',callback_data='m:new'),InlineKeyboardButton('◈ Jobs',callback_data='m:jobs')],[InlineKeyboardButton('▸ Source',callback_data='m:source'),InlineKeyboardButton('▸ Target',callback_data='m:target')],[InlineKeyboardButton('✧ Status',callback_data='m:status'),InlineKeyboardButton('✧ Failed',callback_data='m:failed')],[InlineKeyboardButton('❖ Help',callback_data='m:help')]])
def job_kb(x):
 m=jobs[x]['meta'];return InlineKeyboardMarkup([[InlineKeyboardButton(f"Artist: {m.get('artist') or '—'}",callback_data=f's:artist:{x}')],[InlineKeyboardButton(f"Genre: {m.get('genre') or '—'}",callback_data=f's:genre:{x}')],[InlineKeyboardButton(f"Year: {m.get('year') or '—'}",callback_data=f's:year:{x}')],[InlineKeyboardButton(f"Album: {m.get('album') or '—'}",callback_data=f's:album:{x}')],[InlineKeyboardButton(f"Album Artist: {m.get('album_artist') or '—'}",callback_data=f's:album_artist:{x}')],[InlineKeyboardButton(f"Comment: {m.get('comment') or '—'}",callback_data=f's:comment:{x}')],[InlineKeyboardButton(f"Cover: {'Attached' if m.get('cover_path') else 'Not set'}",callback_data=f'cover:{x}'),InlineKeyboardButton('Clear',callback_data=f'clear:{x}')],[InlineKeyboardButton('✦ START',callback_data=f'start:{x}'),InlineKeyboardButton('Cancel',callback_data=f'cancel:{x}')]])
def summary(x):
 j=jobs[x];m=j['meta'];return f"✦ Cleanfi Job {x}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\nSource: {j.get('source') or 'Not set'}\nTarget: {j.get('target') or 'Not set'}\n\nArtist: {m.get('artist') or '—'}\nGenre: {m.get('genre') or '—'}\nYear: {m.get('year') or '—'}\nAlbum: {m.get('album') or '—'}\nAlbum Artist: {m.get('album_artist') or '—'}\nCover: {'Attached' if m.get('cover_path') else 'Not attached'}\nTitle: Original source title\n\nProcessed: {len(j['processed'])}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}"
def prog(x):
 j=jobs[x];done=len(j['processed'])+len(j['failed'])+len(j['skipped']);total=j['total'];pct=int(done*100/total) if total else 100;w=14;bar='█'*int(w*pct/100)+'░'*(w-int(w*pct/100));el=max(0,time.time()-(j.get('started_at') or time.time()));sp=done/(el/60) if done and el else 0;eta=((total-done)/sp*60) if sp else 0;return f"✦ Cleanfi Processing\nJob: {x}\n\n{bar} {pct}%\nFiles: {done} / {total}\n✓ Processed: {len(j['processed'])}\n✗ Failed: {len(j['failed'])}\n⊘ Skipped: {len(j['skipped'])}\nSpeed: {sp:.1f} files/min\nElapsed: {int(el//60)}m {int(el%60)}s\nETA: {int(eta//60)}m {int(eta%60)}s\nFloodWait: Protected"
async def pupdate(x,force=False):
 j=jobs[x];now=time.time()
 if not force and now-j.get('last_progress',0)<2:return
 j['last_progress']=now
 try:
  if j.get('progress_message_id'):await tg(lambda:app.edit_message_text(j['owner'],j['progress_message_id'],prog(x)))
  else:j['progress_message_id']=(await tg(lambda:app.send_message(j['owner'],prog(x)))).id
  await save()
 except Exception:pass

@app.on_message(filters.private&filters.command('start'))
async def start(_,m):
 if allowed(m):await m.reply_text('✦ CLEANFI\n\nAudiobook metadata cleaner & repacker.',reply_markup=main_kb())
@app.on_message(filters.private&filters.command('help'))
async def help(_,m):
 if allowed(m):await m.reply_text('✦ CLEANFI\n\n/range START END\n/source @channel\n/target @channel\n/meta artist="Name" genre="Romance" year=2026\n/cover JOBID\n/startjob JOBID\n/status JOBID\n/cancel JOBID\n/retry JOBID\n/failed JOBID\n/test MESSAGE_ID\n/jobs',reply_markup=main_kb())
@app.on_message(filters.private&filters.command('source'))
async def source(_,m):
 if not allowed(m):return
 p=(m.text or '').split(maxsplit=1)
 if len(p)!=2:return await m.reply_text('Usage: /source @channelusername')
 try:c=await resolve(p[1]);settings['source']=str(c.id);await save();await m.reply_text(await cinfo('Source',settings['source']),reply_markup=main_kb())
 except Exception as e:await m.reply_text(f'Could not set source: {e}')
@app.on_message(filters.private&filters.command('target'))
async def target(_,m):
 if not allowed(m):return
 p=(m.text or '').split(maxsplit=1)
 if len(p)!=2:return await m.reply_text('Usage: /target @channelusername')
 try:c=await resolve(p[1]);settings['target']=str(c.id);await save();await m.reply_text(await cinfo('Target',settings['target']),reply_markup=main_kb())
 except Exception as e:await m.reply_text(f'Could not set target: {e}')
@app.on_message(filters.private&filters.command('range'))
async def range_cmd(_,m):
 if not allowed(m):return
 p=(m.text or '').split()
 if len(p)!=3 or not p[1].isdigit() or not p[2].isdigit() or int(p[1])>int(p[2]):return await m.reply_text('Usage: /range 1250 1300')
 if not settings.get('source') or not settings.get('target'):return await m.reply_text('Set /source and /target first.',reply_markup=main_kb())
 a,b=map(int,p[1:]);x=uuid.uuid4().hex[:8];jobs[x]={'id':x,'owner':m.from_user.id,'start':a,'end':b,'total':b-a+1,'processed':[],'failed':[],'failed_reasons':{},'skipped':[],'status':'configured','cancel_requested':False,'meta':newmeta(),'source':settings['source'],'target':settings['target'],'created':time.time(),'started_at':None,'progress_message_id':None};sessions[m.from_user.id]=x;await save();await m.reply_text(summary(x),reply_markup=job_kb(x))
@app.on_message(filters.private&filters.command('meta'))
async def metacmd(_,m):
 if not allowed(m):return
 x=active(m)
 if not x:return await m.reply_text('Create a job first with /range.')
 pairs=re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))',(m.text or '')[5:].strip())
 if not pairs:return await m.reply_text('Example: /meta artist="Artist A" genre="Romance" year=2026 album="Book"')
 for k,a,b,c in pairs:
  if k in jobs[x]['meta']:jobs[x]['meta'][k]=a or b or c
 await save();await m.reply_text(summary(x),reply_markup=job_kb(x))
@app.on_message(filters.private&filters.photo)
async def coverphoto(_,m):
 if not allowed(m):return
 p=(m.caption or '').split();x=p[1] if len(p)==2 and p[0].lower()=='/cover' else active(m)
 if not x or x not in jobs:return
 d=TEMP/x;d.mkdir(exist_ok=True);path=d/'cover.jpg';await tg(lambda:m.download(file_name=str(path)));jobs[x]['meta']['cover_path']=str(path);await save();await m.reply_text(summary(x),reply_markup=job_kb(x))
@app.on_message(filters.private&filters.command('cover'))
async def covercmd(_,m):
 if allowed(m):
  x=getjid(m)
  if x in jobs:await m.reply_text(f'Send the image with caption /cover {x}')
@app.on_message(filters.private&filters.command('jobs'))
async def jobs_cmd(_,m):
 if allowed(m):await m.reply_text('✦ JOBS\n\n'+('\n'.join(f"{x} — {j["status"]} — {len(j["processed"])}/{j["total"]}" for x,j in list(jobs.items())[-20:]) or 'No jobs.'),reply_markup=main_kb())
@app.on_message(filters.private&filters.command('status'))
async def status(_,m):
 if not allowed(m):return
 x=getjid(m)
 if x not in jobs:return await m.reply_text('Unknown job.')
 await m.reply_text(prog(x) if jobs[x]['status'] in {'running','queued','cancelling'} else summary(x),reply_markup=job_kb(x))
@app.on_message(filters.private&filters.command('failed'))
async def failed(_,m):
 if not allowed(m):return
 x=getjid(m)
 if x not in jobs:return await m.reply_text('Unknown job.')
 j=jobs[x];await m.reply_text('✦ FAILED\n\n'+('\n'.join(f"{i} — {j['failed_reasons'].get(str(i),'unknown')}" for i in j['failed']) if j['failed'] else 'No failed files.'))
@app.on_message(filters.private&filters.command('cancel'))
async def cancel(_,m):
 if not allowed(m):return
 x=getjid(m)
 if x in jobs:jobs[x]['cancel_requested']=True;jobs[x]['status']='cancelling';await save();await m.reply_text(f'Cancellation requested: {x}')
@app.on_message(filters.private&filters.command('retry'))
async def retrycmd(_,m):
 if not allowed(m):return
 x=getjid(m)
 if x not in jobs:return await m.reply_text('Unknown job.')
 ids=jobs[x]['failed'][:]
 if not ids:return await m.reply_text('No failed files.')
 jobs[x]['failed']=[];jobs[x]['failed_reasons']={};jobs[x]['cancel_requested']=False;await save();await launch(x,m,ids)
@app.on_message(filters.private&filters.command('startjob'))
async def startjob(_,m):
 if allowed(m):
  x=getjid(m)
  if x in jobs:await launch(x,m)
@app.on_message(filters.private&filters.command('test'))
async def test(_,m):
 if not allowed(m):return
 p=(m.text or '').split()
 if len(p)!=2 or not p[1].isdigit():return await m.reply_text('Usage: /test MESSAGE_ID')
 if not settings.get('source') or not settings.get('target'):return await m.reply_text('Set source and target first.')
 x=uuid.uuid4().hex[:8];i=int(p[1]);jobs[x]={'id':x,'owner':m.from_user.id,'start':i,'end':i,'total':1,'processed':[],'failed':[],'failed_reasons':{},'skipped':[],'status':'configured','cancel_requested':False,'meta':newmeta(),'source':settings['source'],'target':settings['target'],'created':time.time(),'started_at':None,'progress_message_id':None};sessions[m.from_user.id]=x;await save();await m.reply_text(summary(x),reply_markup=job_kb(x))

async def process(x,mid):
 j=jobs[x];msg=await tg(lambda:app.get_messages(int(j['source']),mid));media=msg.audio or (msg.document if msg.document and (getattr(msg.document,'mime_type','') or '').startswith('audio/') else None)
 if not media:return 'skip','no audio'
 name=getattr(media,'file_name',None) or f'message_{mid}.bin';ext=Path(name).suffix.lower()
 if ext not in {'.mp3','.m4a','.mp4','.flac','.ogg','.opus','.wav','.aiff','.aif','.wma','.aac'}:return 'skip','unsupported container '+(ext or 'unknown')
 d=TEMP/x;d.mkdir(exist_ok=True);inp=d/f'{mid}_{Path(name).name}';out=d/f'out_{mid}_{Path(name).name}'
 try:
  await tg(lambda:msg.download(file_name=str(inp)));title=read_original_title(str(inp))
  if title is None:
   try:f=MFile(str(inp),easy=True);title=(f.get('title') or [None])[0] if f else None
   except Exception:title=None
  title=title if title is not None else Path(name).stem
  await asyncio.to_thread(clean_and_apply_metadata,str(inp),str(out),title=title,artist=j['meta'].get('artist'),genre=j['meta'].get('genre'),year=j['meta'].get('year'),cover=j['meta'].get('cover_path'),album=j['meta'].get('album'),album_artist=j['meta'].get('album_artist'),comment=j['meta'].get('comment'))
  kw={'audio':str(out),'caption':msg.caption or '','file_name':name,'title':str(title)}
  if j['meta'].get('artist'):kw['performer']=str(j['meta']['artist'])
  cp=j['meta'].get('cover_path')
  if cp and Path(cp).is_file():kw['thumb']=cp
  await tg(lambda:app.send_audio(int(j['target']),**kw));return 'ok',None
 finally:
  for p in (inp,out):
   try:p.unlink()
   except FileNotFoundError:pass
async def launch(x,m,ids=None):
 if x in running:return await m.reply_text('Job is already running.')
 running[x]=asyncio.create_task(run(x,m,ids));await m.reply_text(f'Job {x} queued. FloodWait protection enabled.')
async def run(x,m,ids=None):
 j=jobs[x];j['status']='running';j['started_at']=time.time();await save();ids=ids or list(range(j['start'],j['end']+1));ids=[i for i in ids if i not in j['processed'] and i not in j['skipped']];q=asyncio.Queue()
 for i in ids:await q.put(i)
 await pupdate(x,True)
 async def worker():
  while True:
   try:i=q.get_nowait()
   except asyncio.QueueEmpty:return
   try:
    if j['cancel_requested']:continue
    r,e=await process(x,i)
    if r=='ok':j['processed'].append(i)
    elif r=='skip':j['skipped'].append(i)
    else:j['failed'].append(i);j['failed_reasons'][str(i)]=e
   except Exception as e:
    if i not in j['failed']:j['failed'].append(i)
    j['failed_reasons'][str(i)]=f'{type(e).__name__}: {e}'
   finally:await save();await pupdate(x);q.task_done()
 ts=[asyncio.create_task(worker()) for _ in range(min(WORKERS,max(1,len(ids))))];await q.join()
 for t in ts:t.cancel()
 j['status']='cancelled' if j['cancel_requested'] else ('completed' if not j['failed'] else 'completed_with_failures');await save();await pupdate(x,True);running.pop(x,None);await m.reply_text('✦ JOB FINISHED\n\n'+summary(x),reply_markup=job_kb(x))
@app.on_callback_query()
async def cb(_,q:CallbackQuery):
 if not q.from_user or q.from_user.id not in ADMINS:return await q.answer('Not authorized',show_alert=True)
 p=q.data.split(':');a=p[0]
 if a=='m':
  s=p[1]
  if s=='new':return await q.answer('Use /range START END')
  if s=='source':return await q.message.reply_text(await cinfo('Source',settings.get('source')))
  if s=='target':return await q.message.reply_text(await cinfo('Target',settings.get('target')))
  if s=='jobs':return await q.message.reply_text('\n'.join(f"{x} — {j['status']} — {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-20:]) or 'No jobs.')
  if s=='status':return await q.message.reply_text('\n'.join(f"{x} — {j['status']} — {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-10:]) or 'No jobs.')
  if s=='failed':return await q.answer('Use /failed JOBID')
  return await q.message.reply_text('Use /help for commands.')
 x=p[-1]
 if x not in jobs:return await q.answer('Unknown job',show_alert=True)
 if a=='start':await q.answer();return await launch(x,q.message)
 if a=='cancel':jobs[x]['cancel_requested']=True;jobs[x]['status']='cancelling';await save();return await q.answer('Cancellation requested')
 if a=='clear':jobs[x]['meta']['cover_path']=None;await save();return await q.message.edit_text(summary(x),reply_markup=job_kb(x))
 if a=='cover':return await q.answer(f'Send image with caption /cover {x}',show_alert=True)
 if a=='s':sessions[q.from_user.id]=x;sessions[(q.from_user.id,'field')]=p[1];await q.answer();return await q.message.reply_text('Send '+p[1]+' value as the next message.')
@app.on_message(filters.private&filters.text)
async def field_input(_,m):
 if not allowed(m):return
 f=sessions.get((m.from_user.id,'field'));x=active(m)
 if not f or x not in jobs or (m.text or '').startswith('/'):return
 if f in jobs[x]['meta']:jobs[x]['meta'][f]=m.text;sessions.pop((m.from_user.id,'field'),None);await save();await m.reply_text(summary(x),reply_markup=job_kb(x))
load();app.run()
