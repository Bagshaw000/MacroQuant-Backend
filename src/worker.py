"""arq worker entry point: defines the scheduled (cron) background jobs
that keep Redis/Postgres populated with market data - COT positioning,
currency snapshots, economic calendar events, LSE indicators (CPI/PPI/
UNEMP/GDP/inflation/retail), and news sentiment.

Run via arq against this module (`arq worker.WorkerSettings`); arq reads
`WorkerSettings.cron_jobs` and `WorkerSettings.redis_settings` to know what
to run and where its own job queue lives.

# BROKEN IMPORT: `from .cot import COT` below expects a class named `COT`
# in src/cot.py, but that file only defines `COTNew` (which does have the
# `update_cot()` method this worker calls) - there is no `COT` symbol in
# src/cot.py at all. As it stands, importing this module raises
# `ImportError: cannot import name 'COT' from 'cot'`. Looks like a rename
# that missed this import; needs either `from .cot import COTNew as COT`
# or updating the reference to `COTNew` directly.
"""

import asyncio
import logging
logger = logging.getLogger(__name__)
import os
import sys

# worker.py sits directly in src/, so src/ itself is what needs to be on the
# path for the bare `model.*` / `controller.*` / `custom_types.*` imports used
# throughout this codebase (one dirname, not two).
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings
from arq import cron
from cot import COT
from controller.cot import COTController
from model.market_overview import MarketOverview
from controller.macro import MacroController
from controller.cross_section import CrossSectionController
from controller.economic_event import EconomicEventController
# from controller.gdp import GDPController
# from controller.unemp import UNEMPController
from controller.news import NewsSentimentController
# from controller.cross_section import CrossSectionController
from controller.lse_ import LSEController
from logging_config import configure_logging
from database.redis_ import REDIS_HOST, REDIS_PASSWORD

try:
    # Check if a loop already exists
    asyncio.get_event_loop()
except RuntimeError:
    if sys.platform == 'win32':
        # Windows-specific network engine
        # Windows' default asyncio loop (ProactorEventLoop) is
        # incompatible with some libraries used here (e.g. certain
        # psycopg/asyncpg internals); the Selector-based policy avoids that.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        # Ultra-efficient Linux native initialization
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)



# Controller/model instances are constructed once at module import time and
# reused across every job run - arq workers are long-running processes, so
# this avoids re-establishing DB/Redis connections on every scheduled
# invocation.
cot_model = COT()
cot_ctrl = COTController()
market_ovw = MarketOverview()
# cpi_crtl = CPIController()
# ppi_ctrl = PPIController()
# unemp_ctrl = UNEMPController()
# gdp_ctrl = GDPController()
econ_event_ctrl = EconomicEventController()
news_sentiment_ctrl = NewsSentimentController()
lse_ctrl = LSEController()
macro_ctrl = MacroController()
cross_section_ctrl = CrossSectionController()

# Each job below is an arq task function: arq always calls it with a `ctx`
# dict (job context - not used by any of these), and the function's job is
# just to delegate to the matching controller/model and log completion.

async def warm_cot_cache(ctx):
    # Populate cot_ttf:* in Redis from whatever is already in Postgres. Cheap
    # no-op once the keys are present (setup_redis SCANs first). This is what
    # refills the cache after a Redis flush / fresh deploy without waiting for
    # the Wednesday cot_update sync.
    await cot_ctrl.setup_redis()
    logger.info("Ran cot_ttf cache warm-up")

async def cot_update(ctx):
    await cot_model.update_cot()
    # print("Running")
    logger.info(f"Running COT worker with id ")

async def currency_snapshot(ctx):

    await market_ovw.get_currency()
    logger.info(f"Running currency snapshot worker")

async def get_events(ctx):
    await econ_event_ctrl.store_economic_event()
    logger.info(f"Running economic event worker")

async def get_lse(ctx):
    await lse_ctrl.get_event_cal()
    logger.info(f"Running LSE worker with id ")

# async def get_ppi(ctx):
#     await ppi_ctrl.get_ppi()
#     logger.info(f"Runnin Ppi worker with id")

# async def get_unemp(ctx):
#     await unemp_ctrl.get_unemp()
#     logger.info(f"Runnin Unemployment rate worker with id")

# async def get_gdp(ctx):
#     await gdp_ctrl.get_gdp()
#     logger.info(f"Running GDP worker")

async def get_new_sentiment(ctx):
    await news_sentiment_ctrl.all_country_sentiment()
    logger.info(f"Running news sentiment")

async def refresh_factor_stats(ctx):
    await macro_ctrl.refresh_factor_stats()
    logger.info(f"Running factor stats refresh worker")

async def refresh_cross_section(ctx):
    await cross_section_ctrl.update_quandrant()
    logger.info(f"Running Cross Section analysis")

async def full_cot_positioning(ctx):
    await cot_ctrl.full_positioning()
    logger.info(f"Running full COT positioning (non-curated instruments)")

async def curated_cot_positioning(ctx):
    await cot_ctrl.instituitional_pos()
    logger.info(f"Running curated COT positioning refresh (cot_pos:_meta)")


async def on_startup(ctx):
    # Runs after arq has installed its own logging config, so this call
    # (force=True) restores the project's format + file handler for the worker
    # process. The API process does the equivalent from main.py.
    configure_logging()
    logger.info("arq worker started")


class WorkerSettings:
    # arq reads this class directly (via `arq worker.WorkerSettings`) to
    # know which jobs to schedule and how to reach its own job-queue Redis.

    on_startup = on_startup

    # NOTE on run_at_startup: every job that POPULATES a durable cache runs
    # once when the worker boots, so a fresh deploy / flushed Redis is refilled
    # within minutes instead of waiting for the job's scheduled day. arq
    # enqueues them as normal jobs (parallel, retried, unique-deduped), so this
    # doesn't block the worker. The two exceptions - cot_update and
    # full_cot_positioning - stay False because they're heavy (a full CFTC sync
    # / hundreds of rate-limited LLM calls); the cache still comes up via
    # warm_cot_cache + curated_cot_positioning.
    #
    # NOTE on minute=: arq's cron() treats an unset field as "every value", so
    # `cron(fn, hour=23)` with NO minute fires the job every minute from 23:00
    # to 23:59 - 60 runs, and unique= does NOT dedupe across minutes because
    # each minute is a distinct scheduled slot. Every job below therefore
    # pins an explicit minute.
    cron_jobs = [
        # On startup + every ~4 hours: refill cot_ttf:* from Postgres if empty.
        cron(warm_cot_cache, hour={0, 4, 8, 12, 16, 20}, minute=15, unique=True,
            run_at_startup=True),
        # Every Wednesday at 23:00 - weekly CFTC COT reports are typically
        # released Friday afternoons (for the prior Tuesday's data). Heavy full
        # sync, so it does NOT run at startup - warm_cot_cache covers the cache.
        cron(cot_update,  weekday="wed", hour=23, minute=0, unique=True,
            run_at_startup=False),
        # Every day at 05:00, and on startup (repopulates overview:currency:*).
        cron(currency_snapshot, hour=5 , minute=0,
            unique=True,
            run_at_startup=True),
        # Every Saturday at 23:00, and on startup (repopulates news:* events).
        cron(get_events,weekday='sat', hour=23, minute=0, unique=True,
            run_at_startup=True),
        # On the 1st, 5th, 10th, 15th, 20th, 25th, and 30th of every month at
        # 23:00, and on startup - get_event_cal now also rebuilds the LSE
        # {table}:{country} / {table}:avg cache from Postgres every run.
        cron(get_lse,day={1, 5, 10, 15, 20, 25, 30}, hour=23, minute=0, unique=True,
            run_at_startup=True),
        # Every 3 hours (00/03/06/09/12/15/18/21), on the hour.
        # No unique/run_at_startup override, so this uses arq's defaults
        # (unique=True, run_at_startup=True) unlike the other jobs above.
        cron(get_new_sentiment, hour={0, 3, 6, 9, 12, 15, 18, 21},  # Every 3rd hour of the day
            minute=0 ),
        # Every Sunday at 22:00, and on startup - trailing (mu, sigma) stats
        # have a 9-day TTL (STATS_TTL), so without a startup run a deploy after
        # a wipe leaves {table}:stats:* empty until the next Sunday.
        cron(refresh_factor_stats, weekday="sun", hour=22, minute=0, unique=True,
            run_at_startup=True),
        # 1st/10th/15th/20th/25th at 22:30, and on startup - cross_section:*
        # has a 40-day TTL; a startup run keeps it populated after a deploy.
        cron(refresh_cross_section, day={1, 10, 15, 20, 25}, hour=22, minute=30, unique=True,
            run_at_startup=True),
        # Every Sunday at 01:00 - a few days after cot_update has synced the new
        # weekly report, so the long-tail positioning (full_positioning: every
        # non-curated cot_ttf instrument, with an LLM breakdown each) is scored
        # against fresh data. Slow (hundreds of rate-limited LLM calls); runs
        # overnight. Writes cot_pos:_meta_all; never touches cot_pos:_meta.
        cron(full_cot_positioning, weekday="sun", hour=1, minute=0, unique=True,
            run_at_startup=False),
        # Every Saturday at 00:30, and on startup - rescores the curated
        # COT_CURATED_ASSETS shortlist and rewrites cot_pos:_meta + its blobs
        # (~11 instruments, each with an LLM summary). cot_pos:* has a 30-day
        # TTL; the startup run repopulates it after a deploy. Depends on
        # cot_ttf:* being present, so it is ordered after warm_cot_cache above.
        cron(curated_cot_positioning, weekday="sat", hour=0, minute=30, unique=True,
            run_at_startup=True),
    ]

    # Redis instance arq itself uses to store/dispatch jobs - separate from
    # RedisConnection (database/redis_.py) used by the app's own data cache,
    # but the same physical Redis - so if requirepass is set, arq needs the
    # same password or it can't dequeue jobs at all. Reuses REDIS_HOST /
    # REDIS_PASSWORD from database/redis_.py rather than re-deriving them.
    redis_settings = RedisSettings(host=REDIS_HOST, password=REDIS_PASSWORD)

