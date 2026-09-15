import json, asyncio
from pathlib import Path
class JobStore:
    def __init__(self,path='jobs.json'): self.path=Path(path); self.lock=asyncio.Lock(); self.data=self._load()
    def _load(self):
        try:return json.loads(self.path.read_text())
        except Exception:return {'jobs':{}}
    async def save(self):
        async with self.lock:self.path.write_text(json.dumps(self.data,ensure_ascii=False,indent=2))
    async def create(self,user_id,start,end,meta):
        jid=str(max([int(x) for x in self.data['jobs']] or [0])+1); self.data['jobs'][jid]={'user_id':user_id,'start':start,'end':end,'meta':meta,'status':'queued','done':[],'failed':[]}; await self.save(); return jid
    def get(self,jid):return self.data['jobs'].get(str(jid))
STORE=JobStore()
