import asyncio, shlex
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import CFG
from storage import STORE
from worker import process_job
app=Client('cleanfi',api_id=CFG.api_id,api_hash=CFG.api_hash,bot_token=CFG.bot_token)
DRAFT={}; RUN={}
def ok(m): return m.from_user and m.from_user.id in CFG.admin_ids

def panel(jid): return InlineKeyboardMarkup([[InlineKeyboardButton('✦ Set Metadata',callback_data=f'meta:{jid}'),InlineKeyboardButton('▶ Start',callback_data=f'start:{jid}')],[InlineKeyboardButton('▣ Status',callback_data=f'status:{jid}'),InlineKeyboardButton('× Cancel',callback_data=f'cancel:{jid}')]])
@app.on_message(filters.command('start'))
async def start(_,m):
 if ok(m): await m.reply('✦ Cleanfi\nSelect a range and configure metadata.',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('✦ New Job',callback_data='new')],[InlineKeyboardButton('▣ Help',callback_data='help')]]))
@app.on_message(filters.command('help'))
async def help(_,m):
 if ok(m): await m.reply('Commands:\n/range START END\n/meta artist="..." genre="..." year=2026 [cover=/path] [title="..."]\n/startjob\n/status [JOB]\n/cancel JOB\n/retry JOB\n/failed JOB\n/test MESSAGE_ID')
@app.on_message(filters.command('range'))
async def range_cmd(_,m):
 if not ok(m):return
 try:a,b=map(int,m.command[1:3]); assert a>0 and b>=a
 except: return await m.reply('Usage: /range START END')
 DRAFT[m.from_user.id]={'start':a,'end':b,'meta':{'title':None,'artist':None,'genre':None,'year':None,'cover':None,'album':None,'album_artist':None,'comment':None}}
 await m.reply(f'✦ Range: {a} → {b}\nConfigure metadata:',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('✦ Set Metadata',callback_data='draftmeta'),InlineKeyboardButton('▶ Create Job',callback_data='draftcreate')]]))
@app.on_message(filters.command('meta'))
async def meta(_,m):
 if not ok(m):return
 d=DRAFT.get(m.from_user.id)
 if not d:return await m.reply('Set /range first.')
 s=m.text.partition(' ')[2]; parts=shlex.split(s)
 for p in parts:
  if '=' not in p:continue
  k,v=p.split('=',1); k=k.strip().lstrip('-').lower(); d['meta'][k]=v
 await m.reply('✦ Metadata saved for this range.',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('▶ Create Job',callback_data='draftcreate')]]))
@app.on_callback_query()
async def cb(_,q):
 if q.from_user.id not in CFG.admin_ids:return await q.answer('Unauthorized',show_alert=True)
 data=q.data
 if data=='help':return await q.message.edit('Use /range START END, then /meta key=value, then /startjob.')
 if data=='new':return await q.message.reply('Use /range START END to create a job.')
 if data=='draftcreate':
  d=DRAFT.get(q.from_user.id)
  if not d:return await q.answer('No draft',show_alert=True)
  jid=await STORE.create(q.from_user.id,d['start'],d['end'],d['meta']); DRAFT.pop(q.from_user.id,None); return await q.message.edit(f'✦ Job #{jid}\nRange: {STORE.get(jid)["start"]} → {STORE.get(jid)["end"]}',reply_markup=panel(jid))
 if ':' in data:
  typ,jid=data.split(':',1); job=STORE.get(jid)
  if not job:return await q.answer('Job not found',show_alert=True)
  if typ=='start': RUN[jid]=asyncio.create_task(process_job(app,jid)); return await q.answer('Started')
  if typ=='status':return await q.answer(f'{job["status"]} | done {len(job["done"])} | failed {len(job["failed"])}',show_alert=True)
  if typ=='cancel':
   t=RUN.get(jid)
   if t:t.cancel()
   job['status']='cancelled'; await STORE.save(); return await q.answer('Cancelled')
@app.on_message(filters.command('startjob'))
async def startjob(_,m):
 if not ok(m):return
 # latest queued job for this admin
 jobs=[(int(k),v) for k,v in STORE.data['jobs'].items() if v['user_id']==m.from_user.id and v['status']=='queued']
 if not jobs:return await m.reply('No queued job.')
 jid=str(max(jobs)[0]); RUN[jid]=asyncio.create_task(process_job(app,jid)); await m.reply(f'▶ Job #{jid} started.')
@app.on_message(filters.command('status'))
async def status(_,m):
 if not ok(m):return
 jid=m.command[1] if len(m.command)>1 else None
 jobs=[STORE.get(jid)] if jid else list(STORE.data['jobs'].values())
 lines=[f'{j.get("status")} | {len(j.get("done",[]))} done | {len(j.get("failed",[]))} failed' for j in jobs if j]
 await m.reply('\n'.join(lines) or 'No jobs.')
app.run()
