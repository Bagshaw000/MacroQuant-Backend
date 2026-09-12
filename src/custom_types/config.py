"""Typed container for the Doppler-backed secrets.

config/config.py::doppler_secret() fills one of these from the `tpf_macro`
Doppler project; every controller reads its keys off the instance returned by
get_doppler_env(). Adding a secret means adding a field here AND a
doppler.secrets.get(...) line in config.py.
"""

from pydantic import BaseModel


class ConfigType(BaseModel):
    supabase_url:str          # Supabase project URL
    supabase_key:str          # Supabase service key
    news_api:str              # news provider host / base
    news_key:str              # news provider API key
    db_server:str             # Postgres host
    db_port:str               # Postgres port
    db_name:str               # Postgres database name
    db_password:str
    db_user:str
    twitter_token:str         # Scweet auth token (controller/twitter.py)
    twitter_proxy:str         # outbound proxy for scraping
    news_token: str           # marketaux token (controller/news.py)
    lse_key: str              # LSE economic-calendar API key
    modelrail_key: str        # ModelRail LLM API key (controller/llm.py)
    redis_password: str       # Redis AUTH password (set REDIS_PASSWORD in Doppler)
