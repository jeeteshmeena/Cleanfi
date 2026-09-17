import os,re,json,asyncio,time,uuid,shutil,logging
from pathlib import Path
from collections import deque
from pyrogram import Client,filters
from pyrogram.types import InlineKeyboardMarkup,InlineKeyboardButton,CallbackQuery
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from mutagen import File as MFile
from metadata import clean_and_apply_metadata,read_original_title

load_dotenv()
log=logging.getLogger("cleanfi.core")
API_ID=int(os.environ['API_ID']);API_HASH=os.environ['API_HASH'];BOT_TOKEN=os.environ['BOT_TOKEN']
ADMINS={int(x) for x in os.getenv('ADMIN_IDS','').split(',') if x.strip()}
TEMP=Path(os.getenv('TEMP_DIR','./tmp'));TEMP.mkdir(parents=True,exist_ok=True)
STATE=Path(os.getenv('STATE_FILE','./jobs.json'))
DEFAULT_DELAY=max(1,int(os.getenv('FILE_DELAY_SECONDS','3')));MAX_QUEUE=max(1,int(os.getenv('MAX_QUEUED_JOBS','20')));DEFAULT_RETRIES=max(0,int(os.getenv('TRANSIENT_RETRIES','5')))

# A fresh in-memory MTProto session avoids stale local .session update state.
# The bot token authenticates the bot on every startup; no user session file is used.
app=Client('cleanfi-runtime',api_id=API_ID,api_hash=API_HASH,bot_token=BOT_TOKEN,in_memory=True)

jobs={};settings={};sessions={};queue=deque();queue_task=None;running=None;tg_lock=asyncio.Lock();state_lock=asyncio.Lock()

def save_sync():
 p=STATE.with_suffix('.tmp');p.write_text(json.dumps({'settings':settings,'jobs':jobs,'queue':list(queue)},ensure_ascii=False,indent=2),encoding='utf-8');p.replace(STATE)
async def save():
 async with state_lock: await asyncio.to_thread(save_sync)

def meta(): return {'title_mode':'original','artist':None,'genre':None,'year':None,'album':None,'album_artist':None,'comment':None,'cover_path':None}

def load():
 global jobs,settings,queue
 try:d=json.loads(STATE.read_text(encoding='utf-8'));settings=d.get('settings',{});jobs=d.get('jobs',{});queue=deque(d.get('queue',[]))
 except Exception:settings={};jobs={};queue=deque()
 settings.setdefault('source',os.getenv('SOURCE_CHAT_ID',''));settings.setdefault('target',os.getenv('TARGET_CHAT_ID',''));settings.setdefault('delay',DEFAULT_DELAY);settings.setdefault('mode','balanced');settings.setdefault('retries',DEFAULT_RETRIES);settings.setdefault('min_free_gb',2)
 for j in jobs.values():
  j.setdefault('processed',[]);j.setdefault('failed',[]);j.setdefault('skipped',[]);j.setdefault('failed_reasons',{});j.setdefault('meta',meta());j.setdefault('source',settings['source']);j.setdefault('target',settings['target']);j.setdefault('flood_events',0);j.setdefault('flood_seconds',0);j.setdefault('retries',0);j.setdefault('cancel',False);j.setdefault('progress_message_id',None);j.setdefault('current',None);j.setdefault('started_at',None);j.setdefault('last_progress',0)
  if j.get('status') in ('running','queued','cancelling'):j['status']='paused'
 queue=deque(x for x in queue if x in jobs and jobs[x].get('status') not in ('completed','completed_with_failures','cancelled'))
 for x,j in jobs.items():
  if j.get('status')=='paused' and not j.get('cancel') and x not in queue:queue.append(x)

def ok(m):return bool(m.from_user and m.from_user.id in ADMINS)
def jidof(m):
 p=(m.text or '').split();return p[1] if len(p)>1 and p[1] in jobs else sessions.get(m.from_user.id)
def delay():return max(1,int(settings.get('delay',DEFAULT_DELAY)))
def mode_delay():return {'conservative':max(5,delay()),'balanced':delay(),'fast':max(1,delay()-1)}.get(settings.get('mode','balanced'),delay())
def flood_secs(e):
 if isinstance(e,FloodWait):return max(1,int(e.value))
 m=re.search(r'(?:wait of|waiting)\s+(\d+)\s*seconds?',str(e),re.I);return int(m.group(1)) if m else 10
def is_flood(e):return isinstance(e,FloodWait) or 'FLOOD_WAIT' in str(e).upper() or ('A WAIT OF' in str(e).upper() and 'REQUIRED' in str(e).upper())

async def tg(fn,x=None,label='Telegram'):
 while True:
  if x and jobs.get(x,{}).get('cancel'):raise asyncio.CancelledError()
  try:
   async with tg_lock:return await fn()
  except Exception as e:
   if not is_flood(e):raise
   seconds=flood_secs(e)
   log.warning("FloodWait %ss during %s",seconds,label)
   if not x:
    await asyncio.sleep(seconds+1)
    continue
   j=jobs[x];j['flood_until']=time.time()+seconds;j['flood_label']=label;j['flood_events']=j.get('flood_events',0)+1;j['flood_seconds']=j.get('flood_seconds',0)+seconds;await save()
   while time.time()<j['flood_until']:
    if j.get('cancel'):raise asyncio.CancelledError()
    await asyncio.sleep(min(5,max(0,j['flood_until']-time.time())))
   j['flood_until']=0;await save()

def bar(x):
 j=jobs[x];done=len(j['processed'])+len(j['failed'])+len(j['skipped']);t=j['total'];pct=int(done*100/t) if t else 100;w=16;n=int(w*pct/100);el=max(0,time.time()-(j.get('started_at') or time.time()));sp=done/(el/60) if done and el else 0;eta=((t-done)/sp*60) if sp else 0;fu=max(0,int(j.get('flood_until',0)-time.time()));return f"Cleanfi Processing\nJob: {x}\nStatus: {j['status']}\nCurrent: {j.get('current') or '—'}\n\n{'█'*n+'░'*(w-n)} {pct}%\nFiles: {done} / {t}\nProcessed: {len(j['processed'])}\nFailed: {len(j['failed'])}\nSkipped: {len(j['skipped'])}\nSpeed: {sp:.1f} files/min\nElapsed: {int(el//60)}m {int(el%60)}s\nETA: {int(eta//60)}m {int(eta%60)}s\nFloodWait: {'Waiting '+str(fu)+'s' if fu else 'Protected'}\nDelay: {delay()}s/file\nFloodWait events: {j.get('flood_events',0)}\nRetries: {j.get('retries',0)}"

async def progress(x,force=False):
 if x not in jobs:return
 j=jobs[x];now=time.time()
 if not force and now-j.get('last_progress',0)<3:return
 j['last_progress']=now
 try:
  if j.get('progress_message_id'):
   await tg(lambda:app.edit_message_text(j['owner'],j['progress_message_id'],bar(x)),x,'progress')
  else:
   sent=await tg(lambda:app.send_message(j['owner'],bar(x)),x,'progress')
   j['progress_message_id']=sent.id
 except asyncio.CancelledError:raise
 except Exception as e:log.warning("Progress update failed: %s",e)

def menu():return InlineKeyboardMarkup([[InlineKeyboardButton('New Job',callback_data='m:new'),InlineKeyboardButton('Jobs',callback_data='m:jobs')],[InlineKeyboardButton('Queue',callback_data='m:queue'),InlineKeyboardButton('Settings',callback_data='m:settings')],[InlineKeyboardButton('Source',callback_data='m:source'),InlineKeyboardButton('Target',callback_data='m:target')],[InlineKeyboardButton('Status',callback_data='m:status'),InlineKeyboardButton('Help',callback_data='m:help')]])
def sk():return InlineKeyboardMarkup([[InlineKeyboardButton(f"Delay: {delay()}s",callback_data='set:delay'),InlineKeyboardButton(f"Mode: {settings.get('mode','balanced').title()}",callback_data='set:mode')],[InlineKeyboardButton(f"Retries: {settings.get('retries',DEFAULT_RETRIES)}",callback_data='set:retries')],[InlineKeyboardButton('Back',callback_data='m:back')]])
def jk(x):
 m=jobs[x]['meta'];return InlineKeyboardMarkup([[InlineKeyboardButton(f"Artist: {m.get('artist') or '—'}",callback_data=f's:artist:{x}')],[InlineKeyboardButton(f"Genre: {m.get('genre') or '—'}",callback_data=f's:genre:{x}')],[InlineKeyboardButton(f"Year: {m.get('year') or '—'}",callback_data=f's:year:{x}')],[InlineKeyboardButton(f"Album: {m.get('album') or '—'}",callback_data=f's:album:{x}')],[InlineKeyboardButton(f"Album Artist: {m.get('album_artist') or '—'}",callback_data=f's:album_artist:{x}')],[InlineKeyboardButton(f"Comment: {m.get('comment') or '—'}",callback_data=f's:comment:{x}')],[InlineKeyboardButton(f"Cover: {'Attached' if m.get('cover_path') else 'Not set'}",callback_data=f'cover:{x}'),InlineKeyboardButton('Clear Cover',callback_data=f'clear:{x}')],[InlineKeyboardButton('START',callback_data=f'start:{x}'),InlineKeyboardButton('Cancel',callback_data=f'cancel:{x}')]])
def summary(x):
 j=jobs[x];m=j['meta'];return f"Cleanfi Job {x}\nRange: {j['start']} → {j['end']}\nStatus: {j['status']}\nSource: {j['source']}\nTarget: {j['target']}\n\nArtist: {m.get('artist') or '—'}\nGenre: {m.get('genre') or '—'}\nYear: {m.get('year') or '—'}\nAlbum: {m.get('album') or '—'}\nAlbum Artist: {m.get('album_artist') or '—'}\nComment: {m.get('comment') or '—'}\nCover: {'Attached' if m.get('cover_path') else 'Not attached'}\nTitle: Original source title\n\nProcessed: {len(j['processed'])}/{j['total']} | Failed: {len(j['failed'])} | Skipped: {len(j['skipped'])}\nFloodWait events: {j.get('flood_events',0)} | Wait: {j.get('flood_seconds',0)}s"
async def create(owner,a,b):
 x=uuid.uuid4().hex[:8];jobs[x]={'id':x,'owner':owner,'start':a,'end':b,'total':b-a+1,'processed':[],'failed':[],'failed_reasons':{},'skipped':[],'status':'configured','cancel':False,'meta':meta(),'source':settings['source'],'target':settings['target'],'created':time.time(),'started_at':None,'progress_message_id':None,'last_progress':0,'current':None,'flood_until':0,'flood_label':'','flood_events':0,'flood_seconds':0,'retries':0};sessions[owner]=x;await save();return x
async def launch(x,ids=None):
 global queue_task
 if x not in jobs or jobs[x]['status'] in ('running','queued'):return
 if sum(1 for q in queue if jobs.get(q,{}).get('status')=='queued')>=MAX_QUEUE:raise RuntimeError('Queue is full')
 jobs[x]['queue_ids']=list(ids) if ids is not None else jobs[x].get('queue_ids');jobs[x]['status']='queued';jobs[x]['cancel']=False
 if x not in queue:queue.append(x)
 await save()
 if queue_task is None or queue_task.done():queue_task=asyncio.create_task(worker(),name='cleanfi-queue-worker')
def qtext():return 'CLEANFI QUEUE\n\n'+('\n'.join(f"{i}. {x} — {jobs[x]['status']} — {len(jobs[x]['processed'])}/{jobs[x]['total']}" for i,x in enumerate(queue,1) if x in jobs) or 'Queue is empty.')
async def worker():
 global running
 while queue:
  x=queue.popleft()
  if x not in jobs:continue
  if jobs[x].get('cancel'):jobs[x]['status']='cancelled';continue
  running=x;jobs[x]['status']='running';await save()
  try:await run(x,jobs[x].get('queue_ids'))
  except asyncio.CancelledError:raise
  except Exception as e:
   log.exception('Queue job %s crashed',x);jobs[x]['status']='failed_queue';jobs[x]['failed_reasons']['__job__']=f'{type(e).__name__}: {e}';await save()
   try:await tg(lambda:app.send_message(jobs[x]['owner'],f'Cleanfi job {x} stopped بسبب an internal error. Use /status {x} and /retry {x}.') ,None,'job error')
   except Exception:pass
  running=None;await save()

def infer_name(media):
 name=media.file_name or ''
 if name:return name
 mime=(getattr(media,'mime_type',None) or '').lower()
 return {'audio/mpeg':f'message_{media.file_id}.mp3','audio/mp4':f'message_{media.file_id}.m4a','audio/flac':f'message_{media.file_id}.flac','audio/ogg':f'message_{media.file_id}.ogg','audio/wav':f'message_{media.file_id}.wav','audio/x-wav':f'message_{media.file_id}.wav','audio/aac':f'message_{media.file_id}.aac','audio/x-ms-wma':f'message_{media.file_id}.wma'}.get(mime,f'message_{media.file_id}.bin')

async def process(x,mid):
 j=jobs[x];attempt=0
 while True:
  try:
   if shutil.disk_usage(TEMP).free<settings.get('min_free_gb',2)*1024**3:return 'failed','Low disk space'
   msg=await tg(lambda:app.get_messages(int(j['source']),mid),x,'get_messages')
   media=msg.audio or (msg.document if msg.document and (msg.document.mime_type or '').lower().startswith('audio/') else None)
   if not media:return 'skip','no audio media'
   name=infer_name(media);ext=Path(name).suffix.lower()
   if ext not in {'.mp3','.m4a','.mp4','.flac','.ogg','.opus','.wav','.aiff','.aif','.wma','.aac'}:return 'skip','unsupported '+ext
   if ext=='.aac':return 'skip','raw AAC has no portable metadata container'
   d=TEMP/x;d.mkdir(exist_ok=True);inp=d/f'{mid}_{Path(name).name}';out=d/f'out_{mid}_{Path(name).name}'
   try:
    await tg(lambda:msg.download(file_name=str(inp)),x,'download');title=read_original_title(str(inp))
    if title is None:
     try:f=MFile(str(inp),easy=True);title=(f.get('title') or [None])[0] if f else None
     except Exception:title=None
    title=title if title is not None else Path(name).stem
    await asyncio.to_thread(clean_and_apply_metadata,str(inp),str(out),title=title,artist=j['meta'].get('artist'),genre=j['meta'].get('genre'),year=j['meta'].get('year'),cover=j['meta'].get('cover_path'),album=j['meta'].get('album'),album_artist=j['meta'].get('album_artist'),comment=j['meta'].get('comment'))
    kw={'audio':str(out),'caption':msg.caption or '','file_name':name,'title':str(title)}
    if j['meta'].get('artist'):kw['performer']=str(j['meta']['artist'])
    if j['meta'].get('cover_path') and Path(j['meta']['cover_path']).is_file():kw['thumb']=j['meta']['cover_path']
    await tg(lambda:app.send_audio(int(j['target']),**kw),x,'upload');return 'ok',None
   finally:
    for p in (inp,out):
     try:p.unlink()
     except FileNotFoundError:pass
  except asyncio.CancelledError:raise
  except Exception as e:
   if is_flood(e):continue
   attempt+=1;j['retries']+=1;await save()
   if attempt>settings.get('retries',DEFAULT_RETRIES):return 'failed',f'{type(e).__name__}: {e}'
   await asyncio.sleep(min(30,2**attempt))

async def run(x,ids=None):
 j=jobs[x];j['started_at']=j.get('started_at') or time.time();ids=ids if ids is not None else list(range(j['start'],j['end']+1));done=set(j['processed'])|set(j['skipped']);ids=[i for i in ids if i not in done];await progress(x,True)
 for mid in ids:
  if j.get('cancel'):break
  j['current']=mid;await save();await progress(x,True)
  try:r,reason=await process(x,mid)
  except asyncio.CancelledError:break
  if r=='ok' and mid not in j['processed']:j['processed'].append(mid)
  elif r=='skip' and mid not in j['skipped']:j['skipped'].append(mid)
  elif r=='failed' and mid not in j['failed']:j['failed'].append(mid);j['failed_reasons'][str(mid)]=reason
  j['current']=None;await save();await progress(x,True)
  if not j.get('cancel'):await asyncio.sleep(mode_delay())
 j['status']='cancelled' if j.get('cancel') else ('completed_with_failures' if j['failed'] else ('completed_with_skips' if j['skipped'] else 'completed'));j['completed_at']=time.time();await save();await progress(x,True)
 try:await tg(lambda:app.send_message(j['owner'],summary(x)),None,'summary')
 except Exception as e:log.warning('Summary send failed: %s',e)

@app.on_message(filters.private&filters.command(['start','menu']))
async def menu_cmd(_,m):
 if ok(m):
  log.info('Handling %s from admin=%s',m.text,m.from_user.id)
  await tg(lambda:m.reply_text('CLEANFI\n\nAudiobook metadata cleaner and repacker.',reply_markup=menu()),None,'command reply')

@app.on_message(filters.private&filters.command('range'))
async def range_cmd(_,m):
 if not ok(m):return
 p=(m.text or '').split()
 if len(p)!=3 or not p[1].isdigit() or not p[2].isdigit() or int(p[1])>int(p[2]):return await m.reply_text('Usage: /range START END')
 if not settings['source'] or not settings['target']:return await m.reply_text('Set source and target first.')
 x=await create(m.from_user.id,int(p[1]),int(p[2]));await m.reply_text(summary(x),reply_markup=jk(x))
@app.on_message(filters.private&filters.command('source'))
async def source(_,m):
 if ok(m) and len((m.text or '').split())==2:
  try:c=await tg(lambda:app.get_chat((m.text.split())[1]),None,'get_chat');settings['source']=str(c.id);await save();await m.reply_text(f'Source set: {c.title or c.first_name}\nID: {c.id}',reply_markup=menu())
  except Exception as e:await m.reply_text(f'Could not set source: {e}')
@app.on_message(filters.private&filters.command('target'))
async def target(_,m):
 if ok(m) and len((m.text or '').split())==2:
  try:c=await tg(lambda:app.get_chat((m.text.split())[1]),None,'get_chat');settings['target']=str(c.id);await save();await m.reply_text(f'Target set: {c.title or c.first_name}\nID: {c.id}',reply_markup=menu())
  except Exception as e:await m.reply_text(f'Could not set target: {e}')
@app.on_message(filters.private&filters.command('startjob'))
async def startjob(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs:await launch(x);await m.reply_text(qtext())
@app.on_message(filters.private&filters.command('cancel'))
async def cancel(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs:jobs[x]['cancel']=True;jobs[x]['status']='cancelling';await save();await m.reply_text(f'Cancellation requested: {x}')
@app.on_message(filters.private&filters.command('retry'))
async def retry(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs and jobs[x]['failed']:
   ids=jobs[x]['failed'][:];jobs[x]['failed']=[];jobs[x]['failed_reasons']={};jobs[x]['cancel']=False;await save();await launch(x,ids);await m.reply_text(f'Retry queued: {x}')
@app.on_message(filters.private&filters.command('jobs'))
async def jobs_cmd(_,m):
 if ok(m):await m.reply_text('CLEANFI JOBS\n\n'+('\n'.join(f"{x} — {j['status']} — {len(j['processed'])}/{j['total']}" for x,j in list(jobs.items())[-30:]) or 'No jobs.'),reply_markup=menu())
@app.on_message(filters.private&filters.command('queue'))
async def queue_cmd(_,m):
 if ok(m):await m.reply_text(qtext(),reply_markup=menu())
@app.on_message(filters.private&filters.command('status'))
async def status(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs:await m.reply_text(bar(x),reply_markup=jk(x))
@app.on_message(filters.private&filters.command('failed'))
async def failed(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs:await m.reply_text('FAILED\n\n'+('\n'.join(f"{i} — {jobs[x]['failed_reasons'].get(str(i),'unknown')}" for i in jobs[x]['failed']) or 'No failed files.'))
@app.on_message(filters.private&filters.command('settings'))
async def settings_cmd(_,m):
 if ok(m):await m.reply_text(f"CLEANFI SETTINGS\n\nDelay: {delay()}s/file\nMode: {settings['mode']}\nTransient retries: {settings['retries']}\nFloodWait: pause + countdown + same-file retry\nQueue: persistent FIFO, one job at a time",reply_markup=sk())
@app.on_message(filters.private&filters.command('meta'))
async def meta_cmd(_,m):
 if ok(m):
  x=jidof(m)
  if x in jobs:
   for k,a,b,c in re.findall(r'(\w+)=(?:"([^"]*)"|\'([^\']*)\'|(\S+))',(m.text or '')[5:].strip()):
    if k in jobs[x]['meta']:jobs[x]['meta'][k]=a or b or c
   await save();await m.reply_text(summary(x),reply_markup=jk(x))
@app.on_message(filters.private&filters.photo)
async def cover(_,m):
 if ok(m):
  p=(m.caption or '').split();x=p[1] if len(p)==2 and p[0].lower()=='/cover' else sessions.get(m.from_user.id)
  if x in jobs:
   d=TEMP/x;d.mkdir(exist_ok=True);path=d/'cover.jpg';await tg(lambda:m.download(file_name=str(path)),x,'cover');jobs[x]['meta']['cover_path']=str(path);await save();await m.reply_text(summary(x),reply_markup=jk(x))
@app.on_callback_query()
async def cb(_,q:CallbackQuery):
 if not ok(q):return await q.answer('Not authorized',show_alert=True)
 p=q.data.split(':');a=p[0]
 if a=='m':
  await q.answer();s=p[1]
  if s=='back':return await q.message.edit_text('CLEANFI',reply_markup=menu())
  if s=='new':return await q.message.reply_text('Use /range START END')
  if s=='jobs':return await q.message.reply_text('Use /jobs')
  if s=='queue':return await q.message.edit_text(qtext(),reply_markup=menu())
  if s=='settings':return await q.message.edit_text('CLEANFI SETTINGS',reply_markup=sk())
  if s=='source':return await q.message.reply_text('Use /source @channel')
  if s=='target':return await q.message.reply_text('Use /target @channel')
  if s=='status':return await q.message.reply_text('Use /status JOBID')
  return await q.message.reply_text('Use /help')
 if a=='set':
  field=p[1];sessions[q.from_user.id]='__settings__';sessions[(q.from_user.id,'field')]=field;await q.answer();return await q.message.reply_text('Send delay seconds.' if field=='delay' else ('Send conservative, balanced, or fast.' if field=='mode' else 'Send retry count 0-20.'))
 x=p[-1]
 if x not in jobs:return await q.answer('Unknown job',show_alert=True)
 if a=='start':await launch(x);return await q.answer('Queued')
 if a=='cancel':jobs[x]['cancel']=True;jobs[x]['status']='cancelling';await save();return await q.answer('Cancellation requested')
 if a=='clear':jobs[x]['meta']['cover_path']=None;await save();return await q.message.edit_text(summary(x),reply_markup=jk(x))
 if a=='cover':return await q.answer(f'Send image with caption /cover {x}',show_alert=True)
 if a=='s':sessions[q.from_user.id]=x;sessions[(q.from_user.id,'field')]=p[1];await q.answer();return await q.message.reply_text(f'Send {p[1]} value.')
@app.on_message(filters.private&filters.text)
async def input_text(_,m):
 if not ok(m):return
 f=sessions.get((m.from_user.id,'field'));sid=sessions.get(m.from_user.id)
 if not f or (m.text or '').startswith('/'):return
 try:
  if f=='delay':settings['delay']=max(1,int(m.text.strip()))
  elif f=='retries':settings['retries']=max(0,min(20,int(m.text.strip())))
  elif f=='mode':
   v=m.text.strip().lower()
   if v not in ('conservative','balanced','fast'):raise ValueError()
   settings['mode']=v
  elif sid in jobs and f in jobs[sid]['meta']:jobs[sid]['meta'][f]=m.text.strip()
  else:return
 except Exception:return await m.reply_text('Invalid value.')
 sessions.pop((m.from_user.id,'field'),None);sessions.pop(m.from_user.id,None);await save();await m.reply_text('Updated.',reply_markup=sk() if sid=='__settings__' else jk(sid))
@app.on_message(filters.private&filters.command('help'))
async def help_cmd(_,m):
 if ok(m):await m.reply_text('CLEANFI\n\n/menu\n/range START END\n/source @channel\n/target @channel\n/meta artist="Name" genre="Romance" year=2026\n/cover JOBID\n/startjob JOBID\n/queue\n/status JOBID\n/cancel JOBID\n/retry JOBID\n/failed JOBID\n/jobs\n/settings\n\nFloodWait is never counted as a file failure. The same file waits and retries automatically.')
