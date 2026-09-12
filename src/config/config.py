"""Secret loading via Doppler.

`get_doppler_env()` (memoised with lru_cache, so the fetch happens once per
process) returns a `ConfigType` populated from the `tpf_macro` Doppler project,
config `dev`. Every controller/model calls it for DB creds and API keys.

Requires a `DOPPLER_TOKEN` in the environment (a service token). `load_dotenv()`
lets that come from a local `.env` in development.
"""

from functools import lru_cache
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
logger = logging.getLogger(__name__)
from dopplersdk import DopplerSDK
from dotenv import load_dotenv
from custom_types.config import ConfigType



def doppler_secret()->ConfigType:
    try:
        load_dotenv()
        
        token = os.getenv("DOPPLER_TOKEN")
        
        if not token:
            
            logger.info(f"Failed to fetch Doppler secrets")
            raise RuntimeError(f"Failed to fetch Doppler secrets")
            
        
        doppler = DopplerSDK()
        doppler.set_access_token(token)

        # Optional - unlike every other secret above, REDIS_PASSWORD may not
        # exist in Doppler yet (Redis without requirepass is a valid setup),
        # so a missing secret here must not take down the whole config load.
   

        return ConfigType(
            supabase_key= doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_KEY").value['raw'],
            supabase_url =  doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_URL").value['raw'],
            news_api= doppler.secrets.get(project="tpf_macro", config="dev",name="NEWS_API").value['raw'],
            news_key= doppler.secrets.get(project="tpf_macro", config="dev",name="NEWS_KEY").value['raw'],
            db_server = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_SERVER").value['raw'],
            db_port = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_PORT").value['raw'],
            db_name = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_NAME").value['raw'],
            db_password = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_PASSWORD").value['raw'],
            db_user = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_USER").value['raw'],
            twitter_token= doppler.secrets.get(project="tpf_macro", config="dev", name="TWITTER_TOKEN").value['raw'],
            twitter_proxy= doppler.secrets.get(project="tpf_macro", config="dev", name="PROXY_SERVER").value['raw'],
            news_token = doppler.secrets.get(project="tpf_macro", config="dev", name="NEWS_TOKEN").value['raw'],
            lse_key=doppler.secrets.get(project="tpf_macro", config="dev", name="LSE_KEY").value['raw'],
            modelrail_key=doppler.secrets.get(project="tpf_macro", config="dev", name="MODELRAIL_KEY").value['raw'],
            redis_password=doppler.secrets.get(project="tpf_macro", config="dev", name="REDIS_PASSWORD").value['raw'],
        )
        
        
    except Exception as e:
        logger.error(f"Error loading doppler secret: {e}")
        

@lru_cache     
def get_doppler_env():
    
    return doppler_secret()

