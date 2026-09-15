import asyncio, os, time
from pathlib import Path
from pyrogram.errors import FloodWait
from config import CFG
from metadata import clean_and_apply_metadata
from storage import STORE

async def process_job(app,jid):
    job=STORE.get(jid)
    if not job:return
    job['status']='running'; await STORE.save(); start=time.time()
    Path(CFG.temp_dir).mkdir(parents=True,exist_ok=True)
    for mid in range(job['start'],job['end']+1):
        if mid in job['done']: continue
        try:
            msg=await app.get_messages(CFG.source_chat_id,mid)
            media=msg.audio or msg.document
            if not media: continue
            name=getattr(media,'file_name',None) or f'{mid}.audio'; src=str(Path(CFG.temp_dir)/f'{jid}_{mid}_{name}')
            await app.download_media(msg,src)
            out=str(Path(CFG.temp_dir)/f'{jid}_{mid}_clean_{name}')
            m=job['meta']; await asyncio.to_thread(clean_and_apply_metadata,src,out,**m)
            while True:
                try:
                    await app.send_audio(CFG.target_chat_id,out,caption=msg.caption or '') if name.lower().endswith(('.mp3','.m4a','.aac','.flac','.ogg','.opus','.wav')) else await app.send_document(CFG.target_chat_id,out,caption=msg.caption or '')
                    break
                except FloodWait as e: await asyncio.sleep(e.value)
            job['done'].append(mid); await STORE.save()
        except Exception as e: job['failed'].append({'id':mid,'error':str(e)}); await STORE.save()
        finally:
            for x in (src if 'src' in locals() else None,out if 'out' in locals() else None):
                try: Path(x).unlink(missing_ok=True)
                except Exception: pass
    job['status']='completed'; job['elapsed']=round(time.time()-start,1); await STORE.save()
