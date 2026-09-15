import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

def _ids(value): return {int(x.strip()) for x in value.split(',') if x.strip()}
@dataclass(frozen=True)
class Config:
    api_id:int=int(os.getenv('API_ID','0')); api_hash:str=os.getenv('API_HASH',''); bot_token:str=os.getenv('BOT_TOKEN','')
    source_chat_id:int=int(os.getenv('SOURCE_CHAT_ID','0')); target_chat_id:int=int(os.getenv('TARGET_CHAT_ID','0'))
    admin_ids:set=frozenset(_ids(os.getenv('ADMIN_IDS',''))); max_workers:int=int(os.getenv('MAX_WORKERS','2')); temp_dir:str=os.getenv('TEMP_DIR','./tmp')
CFG=Config()
