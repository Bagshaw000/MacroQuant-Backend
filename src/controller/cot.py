"""CFTC Commitment of Traders (COT) data pipeline: fetches weekly COT
reports, derives net-position metrics per trader category (dealers, asset
managers, leveraged money, other reportables), caches them in Redis, and
computes percentage change over several trailing periods (1/3/6/12 months)
so the frontend can show positioning trends per instrument.

Uses BOTH a sync (`self.redis`) and an async (`self.aioredis`) Redis client
from the same RedisConnection - most methods here use the sync client even
though they're declared `async def`, which means those Redis calls block
the event loop for their duration rather than yielding to it. Only
`setup_redis` and `insert_cot_redis` consistently use the async client.
"""

import asyncio
from datetime import datetime, timedelta
import json
import logging
logger = logging.getLogger(__name__)
import math
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cot import CotData
from database.redis_ import RedisConnection
from model.cot import CotModell
from controller.llm import LLMController
import pandas as pd
import numpy as np

# Redis key prefix for the cached positioning snapshot. One JSON blob per
# instrument at f"{COT_POS_KEY_PREFIX}:{asset}".
COT_POS_KEY_PREFIX = "cot_pos"

# The curated index (instituitional_pos / store_positioning): only the
# COT_CURATED_ASSETS instruments, each with an LLM `summary`, plus an update
# timestamp. This key is the frontend's "tracked instruments" list.
COT_POS_META_KEY = f"{COT_POS_KEY_PREFIX}:_meta"

# The full index (full_positioning / store_full_positioning): curated + the
# long tail of every other cot_ttf instrument. store_full_positioning READS
# COT_POS_META_KEY for the curated names but never writes it.
COT_POS_META_ALL_KEY = f"{COT_POS_KEY_PREFIX}:_meta_all"

# TTL on the snapshot. COT is released weekly, so this is a staleness guard
# for a stalled refresh - roughly a month, several release cycles of margin.
COT_POS_TTL = 30 * 24 * 3600

# Macro-relevant instruments, grouped by the `market` label used in the Redis
# key (cot_ttf:{market}:{asset}:{date}). instituitional_pos() scores exactly
# these; full_positioning() scores every cot_ttf instrument that is NOT here.
COT_CURATED_ASSETS: dict[str, list[str]] = {
    "Currency": ["AUSTRALIAN DOLLAR", "EURO FX", "USD INDEX", "BRITISH POUND", "JAPANESE YEN"],
    "Crypto": ["BITCOIN"],
    "Indices": ["S&P 500 STOCK INDEX", "S&P 500 VIX", "DOW JONES INDUSTRIAL AVERAGE"],
    "Financial": ["FED FUNDS", "UST 10Y NOTE"],
}

# Instruments per fetch batch in full_positioning - bounds concurrent SCANs and
# pipelines when scoring the whole cot_ttf universe (hundreds of contracts).
COT_POS_FULL_BATCH = 40


class COTController:

    def __init__(self):
        # Sync client - used for quick lookups (keys/scan/hgetall) in most
        # methods below, even inside `async def` functions (see module
        # docstring: this blocks the event loop while it runs).
        self.redis = RedisConnection().get_redis()
        # Async client - used where the code actually awaits Redis calls
        # (insert_cot_redis, setup_redis).
        self.aioredis = RedisConnection().get_async_redis()
        self.cot = CotModell()
        self.llm = LLMController()

    #Get Cot data
    async def get_cot_data(self):
        """Return COT percentage-change data for every configured
        instrument, grouped by asset class (Currency, Indicies, Financial,
        Crypto).

        If Redis has no cached COT data yet (`check_cot` empty), first
        fetches and caches the last year of reports via
        `self.cot.get_all_last_year_cot()` + `insert_cot_redis`. Either way,
        reads back from Redis, converts to per-asset dicts, and computes
        the pct-change breakdown for each asset class.
        """
        try:
            # Check if data is stored in redis
            data_obj = dict()
            check_cot =   self.redis.keys("cot_ttf*")

            # self.redis.delete(*check_cot)

            # return
            # # Or clear all fields but keep the hash
            # # self.redis.hdel('cot_ttf', *self.redis.hkeys('cot_ttf*'))
            # return
            with open('data/instr.json', 'r') as f:
                data = json.load(f)

            for category, items in data.items():
                data_obj[category] = items

            data_keys = list(data_obj.keys())

            # Check if we have existing records not in redis
            if not check_cot:

                data_list = await self.cot.get_all_last_year_cot()


                # If batch data is not returned then stop operations
                if not data_list:
                    logger.info("data list is empty")
                    return

                # Update redis records
                insert_data = await self.insert_cot_redis(data_list)


                if not insert_data:
                    logger.info("Failed to update redis cot data")
                    return

            # If data is our redis convert data to dataframe for further processing
            # NOTE: data_keys[0..3] assumes data/instr.json always has
            # exactly these 4 categories in this order (Currency, Indicies,
            # Financial, Crypto per the `data` dict below) - if instr.json
            # ever gains/loses/reorders a category this indexing silently
            # breaks or maps the wrong category to the wrong asset class.
            currency_data,indices_data,financial_data,crypto_data =await asyncio.gather( self.new_covert_redis_dataframe(data_obj[data_keys[0]],data_keys[0]),self.new_covert_redis_dataframe(data_obj[data_keys[1]],data_keys[1]),self.new_covert_redis_dataframe(data_obj[data_keys[2]],data_keys[2]),self.new_covert_redis_dataframe(data_obj[data_keys[3]],data_keys[3]))

            # Ensure all asset class converts properly
            if not currency_data or not indices_data or not financial_data or not crypto_data:
                logger.info("Error in coverting to redis")
                return

            # Calculate the percentage change for all asset classes
            cur_pct,ind_pct,fin_pct,crypt_pct = await asyncio.gather(self.new_calculate_all_change(currency_data),self.new_calculate_all_change(indices_data),self.new_calculate_all_change(financial_data),self.new_calculate_all_change(crypto_data))


            data = {
                "Currency":cur_pct,
                "Indicies":ind_pct,
                "Financial":fin_pct,
                "Crypto":crypt_pct
            }

            return data


        except Exception as e:
            logger.error(f"Error getting data : {e}", exc_info=True)
            raise


    # Calculate the percentage change for a given period
    async def new_calculate_all_change(self, data:dict):
        """For every instrument in `data`, compute pct change of each net-
        position field (large_spec_net, commercial_net, dealer_net,
        asset_mgr_Net, lev_money_net, other_rept_net) over 1/3/6/12-month
        trailing windows (expressed in weekly COT reports: 4/12/24/52
        weeks), using the most recent non-null value forward-filled across
        gaps. Mutates and returns `data` with a `pct_change` key added to
        each instrument that had at least one computable field.
        """
        try:
            # These are the fields that are relevant to the task
            fields = ['large_spec_net', 'commercial_net', 'dealer_net',
                  'asset_mgr_Net', 'lev_money_net', 'other_rept_net']

            # Periods to call calculate pct change
            periods = {'1_month': 4, '3_month': 12, '6_month': 24, '1_year': 52}


            # Loop through all the instrument data
            for instrument, instrument_data in data.items():

                if not instrument_data:
                    continue

                # Sort the entry by the dates
                dates = np.array(sorted(instrument_data.keys()))

                # Check if there is enough data
                if len(dates) < 4:
                    continue

                instrument_results = dict()

                # Loop through all fields and
                for field in fields:
                    # Extract and forward fill values
                    values=list()
                    last_val = None

                    # Loop through all dates for the instrument
                    for d in dates:
                        # Get the value for the field for the coressponding date
                        val = instrument_data[d].get(field)

                        # Check if the value got is valid before converting to float
                        if val is not None and val !='':
                            try:
                                last_val= float(val)
                            except:
                                pass

                        # Append the value to an array
                        values.append(last_val)

                    # convert to np aray and automatically cast all values to a float
                    values = np.array(values,dtype=np.float64)

                    # Check if any value are nan and return
                    valid_mask = ~np.isnan(values)

                    # if in all values are nan is breaks
                    if not valid_mask.any():
                        continue

                    # Get most recent valid value where valid mask is True(This index is likely the most recent value)
                    last_idx = np.max(np.where(valid_mask)[0])
                    current_val = values[last_idx]
                    current_date = dates[last_idx]


                    field_results= dict()

                    # Loop through all the periods
                    for period_name, weeks in periods.items():
                        # For the number of weeks in the period calculate the target index
                        target_idx= last_idx - weeks

                        # Check that the target does not exceed 0 and there is value in that index
                        if target_idx >= 0 and valid_mask[target_idx]:

                            prev_val = values[target_idx]

                            # Check the previous value is not 0
                            if prev_val !=0:
                                # Calculate pct change for that period and round to d.p
                                pct = round(((current_val - prev_val) / abs(prev_val))*100, 2)

                                # Store the pct change for tha period with its dat
                                field_results[period_name] = {current_date:pct}

                    # field result not empty
                    if field_results:
                        # Store all period change to the particular field
                        instrument_results[field] = field_results
                #   Store the  pct result for all field to the instrument
                if instrument_results:
                    data[instrument]['pct_change'] = instrument_results

            return data
        except Exception as e:
            logger.error(f"Error getting data : {e}", exc_info=True)

    # Convert redis to dataframe
    async def new_covert_redis_dataframe(self, asset_list:list, asset_cls:str):
        """For every asset in `asset_list`, read its most recent 53 weekly
        "cot_ttf:{asset_cls}:{asset}:*" hashes (~1 year of COT reports - matches
        the longest window new_calculate_all_change needs) into {date: hash}
        dicts. Returns {asset: {date: hash}} for every asset that had at least
        one stored entry.

        `asset_cls` is the {market} segment of the key, so _fetch_recent_weeks
        does the whole scan/sort/pipeline job. That helper uses the async Redis
        client, so this is genuinely non-blocking (unlike the rest of this path).
        """
        try:
            results = await asyncio.gather(
                *(self._fetch_recent_weeks(asset_cls, asset, 53) for asset in asset_list)
            )
            return {asset: rows for asset, rows in results if rows}

        except Exception as e:
            logger.error(f"Converting redis to dataframe", exc_info=True)
            raise


    # This insert the cot_ttf into the redis
    async def insert_cot_redis(self, data:list[CotData]):
        """Convert fetched CotData records into net-position metrics per
        trader category, then cache each row in Redis as a hash keyed
        "cot_ttf:{market}:{market_and_exchange_names}:{report_date}", with
        a TTL of ~60 weeks from that report's date (get_ttl_until_60_weeks).

        NOTE: `batch_size`/`batch` (below) are computed as if this loops
        over the dataframe in chunks, but the inner loop actually iterates
        `new_df.iterrows()` (the WHOLE dataframe) every time through the
        outer `for i in range(0, len(new_df), batch_size)` loop, not
        `batch.iterrows()` - so `batch` is unused and every row gets
        processed/written len(new_df)/batch_size times over, not once.
        """
        try:
            # pass
            batch_size = 1000


            df = pd.DataFrame.from_records([r.__dict__ for r in data])

            # Remove 'id' and 'market' if you don't need them
            df = df.drop(columns=['_sa_instance_state','id'], errors='ignore')

            # Reverse to chronological order (oldest first)

            df["report_date_as_yyyy_mm_dd"]= df["report_date_as_yyyy_mm_dd"]


            # Example calculations
            df['dealer_net'] = df['dealer_positions_long_all'] - df['dealer_positions_short_all']
            # Positive = Dealers are net long (could be bearish signal - hedging client selling)
            # Negative = Dealers are net short (could be bullish signal - hedging client buying)

            df['asset_mgr_net'] = df['asset_mgr_positions_long_all'] - df['asset_mgr_positions_short_all']
            # Positive = Institutions bullish (trend-following)
            # Negative = Institutions bearish (trend-following)

            df['lev_money_net'] = df['lev_money_positions_long_all'] - df['lev_money_positions_short_all']
            # Positive = Hedge funds/CTAs bullish (often crowded - contrarian signal at extremes)
            # Negative = Hedge funds/CTAs bearish (often crowded - contrarian signal at extremes)

            df['other_rept_net'] = df['other_rept_positions_long_all'] - df['other_rept_positions_short_all']


            df['commercial_net'] = df['dealer_net'] + df['other_rept_net']
            # Positive = Commercials net long (smart money bullish)
            # Negative = Commercials net short (smart money bearish)

            df['large_spec_net'] = df['asset_mgr_net'] + df['lev_money_net']
            # Positive = Speculators bullish (sentiment indicator)
            # Negative = Speculators bearish (sentiment indicator)

            new_df = df[["market_and_exchange_names","market","report_date_as_yyyy_mm_dd","dealer_net", 'dealer_positions_long_all','dealer_positions_short_all','asset_mgr_net','asset_mgr_positions_long_all', 'asset_mgr_positions_short_all','commercial_net','lev_money_positions_long_all','lev_money_positions_short_all','large_spec_net','other_rept_net',"open_interest_all"]]

            # Batch execute the insert functionality
            for i in range(0, len(new_df), batch_size):
                batch = new_df.iloc[i:i+batch_size]
                pipe = self.aioredis.pipeline()
                for index, row in new_df.iterrows():
                    # Get temporary data
                    tmp_data:dict = row.to_dict()
                    tmp_data["report_date_as_yyyy_mm_dd"]= datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d")

                    # Get the expiration date
                    expiration_date = await self.get_ttl_until_60_weeks(datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d"))
                    row_date = datetime.fromisoformat(datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d"))

                    key = f"cot_ttf:{row["market"]}:{row["market_and_exchange_names"]}:{row_date.date()}"
                    pipe.hset(key, mapping=tmp_data)
                    pipe.expire(key,time= expiration_date)


                exec = await pipe.execute()

            return exec


        except Exception as e:
            logger.error(f"Error inserting into redis data : {e}",exc_info=True)
            raise

    # Get the expiration for 60 weeks in seconds
    async def get_ttl_until_60_weeks(self,reference_date_str, format="%Y-%m-%d"):
        """
        Returns the number of seconds from the current moment until
        the date that is 60 weeks after the provided reference_date.
        """
        # Parse the reference date
        ref_date = datetime.strptime(reference_date_str, format)

        # Calculate the future expiration date (60 weeks later)
        expire_date = ref_date + timedelta(weeks=60)

        # Get the current time
        now = datetime.now()

        # Calculate the TTL in seconds
        ttl_seconds = int((expire_date - now).total_seconds())

        # Return 0 if the expiration date is in the past
        return max(0, ttl_seconds)





    # This warms the cot_ttf:* Redis cache from Postgres on startup.
    async def setup_redis(self):
        """Warm the `cot_ttf:*` Redis cache from Postgres when it is empty.

        Idempotent, and gated on the ACTUAL presence of keys (a SCAN) rather
        than a sticky "done" flag - so it also self-heals after the keys
        expire (60-week TTL), get evicted, or Redis is flushed. It never
        deletes anything: insert_cot_redis overwrites by key, and old rows age
        out on their own TTL.

        Never raises - a cold cache should not take down API startup. The
        weekly `cot_update` cron and the lazy per-instrument fetches recover
        whatever this misses.
        """
        try:
            # SCAN (non-blocking), stop at the first hit - we only need to
            # know whether ANY cot_ttf key exists.
            async for _ in self.aioredis.scan_iter(match="cot_ttf:*", count=1):
                logger.info("cot_ttf cache already populated - skipping warm-up")
                return

            data_list = await self.cot.get_all_last_year_cot()
            if not data_list:
                logger.warning("setup_redis: Postgres returned no COT rows to cache")
                return

            await self.insert_cot_redis(data_list)
            logger.info("setup_redis: cot_ttf cache warmed from Postgres")

        except Exception as e:
            # Log and swallow - do not propagate into main.py's lifespan.
            logger.error(f"Error setting up redis: {e}", exc_info=True)

    # Trader categories scored for positioning, each as (long_field, short_field).
    # `asset_mgr` is the institutional / "real money" bucket - the headline here;
    # `lev_money` (hedge funds / CTAs - "fast money") and `dealer` (sell-side
    # counterparty) are carried alongside so the frontend can show divergences
    # (e.g. institutions crowded long while fast money is crowded short).
    _POS_LEGS = {
        "asset_mgr": ("asset_mgr_positions_long_all", "asset_mgr_positions_short_all"),
        "lev_money": ("lev_money_positions_long_all", "lev_money_positions_short_all"),
        "dealer":    ("dealer_positions_long_all",    "dealer_positions_short_all"),
    }

    # Every TFF trader group, as (long_field, short_field, net_field). Net is
    # long - short where the legs are in the Redis hash; `other_rept` only has
    # the pre-computed net column (see insert_cot_redis's column select), so its
    # legs are None and the net_field is used instead.
    _GROUP_LEGS = {
        "dealer":     ("dealer_positions_long_all",    "dealer_positions_short_all",    "dealer_net"),
        "asset_mgr":  ("asset_mgr_positions_long_all", "asset_mgr_positions_short_all", "asset_mgr_net"),
        "lev_money":  ("lev_money_positions_long_all", "lev_money_positions_short_all", "lev_money_net"),
        "other_rept": (None,                            None,                            "other_rept_net"),
    }

    # Trailing pct-change windows, counted in weekly COT reports (~4.33 wk/mo).
    _CHANGE_WINDOWS = {"1_month": 4, "3_month": 13, "6_month": 26, "1_year": 52}

    @staticmethod
    def _crowding_label(percentile: float) -> str | None:
        """Bucket a 0-100 positioning percentile into a crowding label.

        The cut points are deliberately symmetric around the 40-60 "balanced"
        band: 5 / 20 / 40 / 60 / 80 / 95. The outer 5-point tails are the
        genuine extremes ("crowded"); the 80/95 and 5/20 bands are the
        approach to them ("stretched" / "leaning"). Tighten the tails (e.g.
        3/97) for a rarer signal.
        """
        if pd.isna(percentile):
            return None                       # no score -> no label
        if percentile >= 95: return "crowded long"
        if percentile >= 80: return "stretched long"
        if percentile >= 60: return "leaning long"
        if percentile >  40: return "balanced"
        if percentile >  20: return "leaning short"
        if percentile >   5: return "stretched short"
        return                    "crowded short"

    def _positioning_metrics(self, data: dict, window: int = 52, min_w: int = 26) -> dict:
        """Vectorised positioning percentile / z-score / momentum for every
        instrument in `data` ({asset: {report_date: redis_hash}}, oldest-first).

        For each trader category in `_POS_LEGS`, on the most recent week:
          - net_pct_oi : (long - short) / open_interest, as a percent. Normalising
                         by OI makes the number comparable across instruments and
                         across time (OI drifts up as a market grows).
          - percentile : rank of that net_pct_oi within the instrument's own
                         trailing `window` weeks, 0-100. This is the "crowded"
                         gauge - >=95 = most bullish positioning in the window,
                         <=5 = most bearish. Ranking by value (not by slot) means
                         a missing weekly report doesn't corrupt it.
          - score      : 2*percentile - 100, i.e. -100 (max short) .. +100 (max long).
          - z          : (net_pct_oi - rolling mean) / rolling std (population).
                         Secondary read - "how many sigmas from normal". Can
                         disagree with `percentile` when the positioning history
                         is skewed; that gap is itself informative.
          - mom_4w     : 4-week change in net_pct_oi (percentage points) - is the
                         crowding still building or already unwinding.
          - label      : `_crowding_label(percentile)`.

        Returns {asset: {category: {metric: value}}}. Instruments with fewer than
        `min_w` usable weeks are dropped (percentile would be noise).

        `window` defaults to 52 because instituitional_pos() only pulls the last
        52 weekly rows from Redis; widen both once a longer history is available
        (3 years / 156 weeks is the convention for true positioning extremes).
        """
        try:
            # ----------------------------------------------------------------
            # STEP 1 - Flatten the nested dict into ONE tidy DataFrame holding
            # every instrument's weekly rows stacked together.
            #
            # Decision: one combined frame rather than a per-instrument loop.
            # It lets every calculation below run as a single vectorised pandas
            # op over all ~11 instruments at once; per-instrument isolation is
            # then handled by groupby, not by Python iteration.
            # ----------------------------------------------------------------
            rows = [
                {"instrument": asset, "date": date, **hash_data}
                for asset, by_date in data.items()
                for date, hash_data in by_date.items()
            ]
            if not rows:
                return {}                       # nothing came back from Redis
            df = pd.DataFrame(rows)

            # Redis hashes are all strings, and CFTC uses "." for a missing
            # value - errors="coerce" turns any unparseable cell into NaN
            # instead of raising, so one bad row can't sink the whole batch.
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

            # The only numeric columns we need: each category's long/short legs
            # plus open interest. df.get-style guard (`col in df.columns`)
            # covers an instrument whose hash is missing a category entirely.
            legs = {field for pair in self._POS_LEGS.values() for field in pair}
            for col in legs | {"open_interest_all"}:
                df[col] = pd.to_numeric(df[col], errors="coerce") if col in df.columns else np.nan

            # Drop rows with an unparseable date, then sort chronologically
            # within each instrument - the rolling windows and the "take the
            # last row" step below both depend on ascending date order. The
            # caller already returns oldest-first, but sorting makes this
            # method correct regardless of input order.
            df = df.dropna(subset=["date"]).sort_values(["instrument", "date"])

            # net-%-of-OI needs a positive denominator. .where(> 0) nulls out
            # any row with missing / zero / negative OI; that row's metrics
            # then come out NaN and are skipped when the results are assembled.
            oi = df["open_interest_all"].where(df["open_interest_all"] > 0)

            # ----------------------------------------------------------------
            # STEP 2 - For each trader category, derive the positioning metrics.
            #
            # `metrics` is pre-seeded with an empty dict per instrument so the
            # assembly loop can just fill in whichever categories produced a
            # usable number.
            # ----------------------------------------------------------------
            metrics: dict = {inst: {} for inst in df["instrument"].unique()}
            for cat, (long_f, short_f) in self._POS_LEGS.items():
                # 2a. Normalised position: this category's net contracts as a
                #     fraction of the instrument's total open interest. Dividing
                #     by OI (per row, so each instrument by its own OI) makes
                #     the number comparable across instruments and across time.
                net_oi = (df[long_f] - df[short_f]) / oi

                # 2b. Group by instrument so every rolling calc below stays
                #     inside one instrument's own history - a window never
                #     bleeds VIX data into FED FUNDS. transform() returns a
                #     Series index-aligned to df, so pct / mean / std / mom all
                #     line up row-for-row and can be combined directly.
                by_inst = net_oi.groupby(df["instrument"])

                # 2c. Percentile: where each week's net_oi ranks within its own
                #     trailing `window` weeks (0-100). min_periods=min_w lets a
                #     short history still produce a value once it has min_w
                #     points, instead of staying all-NaN until the full window
                #     fills. pandas' rank(pct=True) uses the rank/n convention
                #     (a unique window max scores exactly 100).
                pct = by_inst.transform(
                    lambda s: s.rolling(window, min_periods=min_w).rank(pct=True) * 100
                )

                # 2d. Z-score inputs over the same window. ddof=0 (population
                #     std): the window IS the full reference set being compared
                #     against, not a sample drawn from something larger.
                mean = by_inst.transform(lambda s: s.rolling(window, min_periods=min_w).mean())
                std = by_inst.transform(lambda s: s.rolling(window, min_periods=min_w).std(ddof=0))
                # replace(0, NaN): a perfectly flat window has 0 std -> the
                #     z-score is undefined, so emit NaN rather than divide by 0.
                z = (net_oi - mean) / std.replace(0, np.nan)

                # 2e. Momentum: 4-week (~1 month) change in net_oi, still in
                #     OI-fraction terms. shift(4) is per-instrument (it's on the
                #     grouped object), so it can't pull a value from a different
                #     instrument at the series boundary.
                mom = net_oi - by_inst.shift(4)

                # 2f. Bundle the derived columns, rounding for presentation, and
                #     keep only the most recent row per instrument - that row is
                #     "today's" positioning read. Building it as a frame first
                #     keeps every column aligned to the same rows.
                latest = pd.DataFrame({
                    "instrument": df["instrument"],
                    "net_pct_oi": (net_oi * 100).round(2),   # fraction -> percent
                    "percentile": pct.round(1),
                    "score": (2 * pct - 100).round(),        # 0..100 -> -100..+100
                    "z": z.round(2),
                    "mom_4w": (mom * 100).round(2),          # fraction -> percentage points
                }).groupby("instrument").tail(1)

                # 2g. Assemble the output. Only ~11 rows here, so a plain
                #     iterrows loop is fine. numpy scalars are cast to native
                #     float/int and NaN -> None so the dict serialises to clean
                #     JSON in store_positioning().
                for _, r in latest.iterrows():
                    if pd.isna(r["percentile"]):
                        continue  # instrument has fewer than min_w weeks - skip
                    metrics[r["instrument"]][cat] = {
                        "net_pct_oi": None if pd.isna(r["net_pct_oi"]) else float(r["net_pct_oi"]),
                        "percentile": float(r["percentile"]),
                        "score": None if pd.isna(r["score"]) else int(r["score"]),
                        "z": None if pd.isna(r["z"]) else float(r["z"]),
                        "mom_4w": None if pd.isna(r["mom_4w"]) else float(r["mom_4w"]),
                        "label": self._crowding_label(r["percentile"]),
                    }

            # Drop instruments that produced no category at all (e.g. every
            # category short on history), so callers only see real results.
            return {inst: cats for inst, cats in metrics.items() if cats}
        except Exception as e:
            logger.error(f"Error computing positioning metrics: {e}", exc_info=True)
            raise

    async def _fetch_last_52(self, market: str, asset: str) -> tuple[str, dict]:
        """One instrument's most recent 52 weekly COT hashes from Redis, as
        (asset, {report_date: hash}) oldest-first - the order
        _positioning_metrics' rolling windows expect. Empty dict if the
        instrument isn't cached.
        """
        return await self._fetch_recent_weeks(market, asset, 52)

    async def _fetch_recent_weeks(self, market: str, asset: str,
                                  weeks: int) -> tuple[str, dict]:
        """One instrument's most recent `weeks` weekly COT hashes from Redis,
        as (asset, {report_date: hash}) oldest-first. Empty dict if the
        instrument isn't cached.
        """
        pattern = f"cot_ttf:{market}:{asset}:*"

        # SCAN (non-blocking) rather than KEYS. count=100 is a work-per-call
        # hint, not a result limit.
        keys = [k async for k in self.aioredis.scan_iter(match=pattern, count=100)]
        if not keys:
            return asset, {}

        # Keys end in an ISO date, which sorts chronologically as text, so a
        # reverse sort puts the newest first; take the last `weeks` reports.
        recent_keys = sorted(keys, reverse=True)[:weeks]

        # One pipelined round trip for all the HGETALLs.
        pipe = self.aioredis.pipeline()
        for k in recent_keys:
            pipe.hgetall(k)
        rows = await pipe.execute()

        # Re-key by report date, reverse back to oldest-first. `if row` drops a
        # key that raced with a TTL expiry.
        return asset, {
            k.split(":")[-1]: row
            for k, row in reversed(list(zip(recent_keys, rows)))
            if row
        }

    def _net_pct_oi_history(self, by_date: dict) -> dict:
        """Turn one instrument's {report_date: redis_hash} (any order) into a
        per-category weekly history of net % of open interest:

            {"asset_mgr": [["2025-01-07", 12.3], ["2025-01-14", 11.8], ...],
             "lev_money": [...], "dealer": [...]}

        net_pct_oi = (long - short) / open_interest_all * 100, using the same
        _POS_LEGS categories and OI-normalisation as _positioning_metrics.
        Weeks are oldest-first; a week with missing / non-positive OI or an
        unparseable leg comes through as None so the series stays aligned.
        A category whose long/short columns are entirely absent is skipped.
        """
        # One row per weekly report, the hash fields spread out as columns.
        df = pd.DataFrame([{"date": d, **h} for d, h in by_date.items()])
        if df.empty:
            return {}

        # Redis hashes are all strings; coerce the date and sort ascending so
        # the emitted series is oldest-first regardless of dict order.
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")

        # net-%-of-OI needs a positive denominator - .where(> 0) nulls out any
        # week with missing / zero / negative OI so it comes through as NaN.
        oi = pd.to_numeric(df.get("open_interest_all"), errors="coerce")
        oi = oi.where(oi > 0)

        out: dict = {}
        for cat, (long_f, short_f) in self._POS_LEGS.items():
            # An instrument whose hash never carried this category's legs -
            # skip it rather than emit an all-None series.
            if long_f not in df.columns or short_f not in df.columns:
                continue
            # (long - short) / OI, per row, as a percent. Same definition as
            # _positioning_metrics; this just keeps every week instead of the last.
            net = (pd.to_numeric(df[long_f], errors="coerce")
                   - pd.to_numeric(df[short_f], errors="coerce")) / oi * 100
            # [date, value] pairs; NaN (bad OI / unparseable leg) -> None so the
            # JSON stays clean and the series stays date-aligned.
            out[cat] = [
                [d.strftime("%Y-%m-%d"), None if pd.isna(v) else round(float(v), 2)]
                for d, v in zip(df["date"], net)
            ]
        return out

    async def net_pct_oi_timeseries(self, scope: str = "tracked",
                                    weeks: int = 52) -> dict:
        """Net % of open interest as a weekly time series, per trader category,
        for every instrument listed in the positioning meta index.

            scope="tracked" -> cot_pos:_meta      (curated shortlist)
            scope="all"     -> cot_pos:_meta_all  (curated + long tail)

        Returns {asset: {category: [[report_date, net_pct_oi], ...]}}, each
        series oldest-first. Reads straight from the cot_ttf:{market}:{asset}:*
        weekly hashes (not the cached positioning snapshot), so it reflects
        whatever history Redis currently holds - typically ~1 year.

        Instruments named in the index but with no cot_ttf hashes cached (or
        not present in Postgres, so their `market` label is unknown) are
        omitted.
        """
        try:
            # "all" -> the full curated+tail index, anything else -> curated only.
            meta_key = COT_POS_META_ALL_KEY if scope == "all" else COT_POS_META_KEY

            # decode_responses=True on the pool -> str, or None if the index key
            # has expired / never been written. `wanted` is the instrument names.
            meta_raw = await self.aioredis.get(meta_key)
            meta = json.loads(meta_raw) if meta_raw else {}
            wanted = set(meta.get("instruments", []))
            if not wanted:
                logger.info(f"No instruments listed in {meta_key}")
                return {}

            # The meta index stores instrument names only; the Redis keys are
            # cot_ttf:{market}:{name}:{date}, so map name -> market label from
            # Postgres to build each scan pattern.
            name_to_market = {
                name: market
                for market, name in await self.cot.get_distinct_instruments()
                if name in wanted
            }
            pairs = [(name_to_market[n], n) for n in wanted if n in name_to_market]
            if not pairs:
                logger.info("No index instruments resolve to a known market")
                return {}

            # Bounded fan-out: COT_POS_FULL_BATCH instruments per gather so the
            # "all" scope (hundreds of contracts) doesn't open every SCAN at once.
            series: dict = {}
            for i in range(0, len(pairs), COT_POS_FULL_BATCH):
                batch = pairs[i:i + COT_POS_FULL_BATCH]
                results = await asyncio.gather(
                    *(self._fetch_recent_weeks(m, n, weeks) for m, n in batch)
                )
                for asset, by_date in results:
                    if by_date:
                        series[asset] = self._net_pct_oi_history(by_date)
            return series
        except Exception as e:
            logger.error(f"Error building net %OI time series: {e}", exc_info=True)
            raise

    async def _resolve_market(self, asset: str) -> str | None:
        """Look up an instrument's `market` key-segment (e.g. "Currency") from
        Postgres by name. None if the instrument isn't in cot_ttf at all."""
        return next(
            (m for m, n in await self.cot.get_distinct_instruments() if n == asset),
            None,
        )

    def _group_changes_history(self, by_date: dict) -> tuple[dict, str | None]:
        """Shared by asset_group_changes and get_instrument: turn one
        instrument's {report_date: hash} into every TFF trader group's
        net-position series plus its pct change over 1/3/6/12-month windows.

        Returns ({group: {"net": [...], "pct_change": {...}}}, last_date) -
        ({}, None) if `by_date` yields no usable rows.

        net        = long - short (or the pre-computed *_net column for
                     other_rept), forward-filled across any missing weekly report.
        pct_change = (latest - value `w` weeks back) / abs(that value) * 100.
                     A window longer than the history, or a zero base, gives None.
        """
        # One row per weekly report; coerce + sort ascending by date.
        df = pd.DataFrame([{"date": d, **h} for d, h in by_date.items()])
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return {}, None
        dates = df["date"].dt.strftime("%Y-%m-%d").tolist()

        groups: dict = {}
        for group, (long_f, short_f, net_f) in self._GROUP_LEGS.items():
            # Prefer long - short; fall back to the stored net column.
            if long_f in df.columns and short_f in df.columns:
                net = (pd.to_numeric(df[long_f], errors="coerce")
                       - pd.to_numeric(df[short_f], errors="coerce"))
            elif net_f in df.columns:
                net = pd.to_numeric(df[net_f], errors="coerce")
            else:
                continue                       # group absent from the hash

            # Forward-fill so one missing weekly report doesn't punch a hole
            # in the series or throw off the pct-change look-backs.
            net = net.ffill()

            series = [
                [d, None if pd.isna(v) else round(float(v), 2)]
                for d, v in zip(dates, net)
            ]

            # pct change of the latest value vs the value `w` reports earlier.
            latest = net.iloc[-1]
            pct_change: dict = {}
            for name, w in self._CHANGE_WINDOWS.items():
                idx = len(net) - 1 - w
                if idx < 0 or pd.isna(latest):
                    pct_change[name] = None    # not enough history
                    continue
                base = net.iloc[idx]
                pct_change[name] = (
                    None if pd.isna(base) or base == 0
                    else round((latest - base) / abs(base) * 100, 2)
                )

            groups[group] = {"net": series, "pct_change": pct_change}

        return groups, dates[-1]

    async def asset_group_changes(self, asset: str, market: str | None = None,
                                  weeks: int = 52) -> dict:
        """Last `weeks` weekly COT reports for ONE instrument, with every TFF
        trader group's net-position series and its pct change over 1 / 3 / 6 /
        12-month trailing windows.

        `market` is the {market} key segment (e.g. "Currency"); when omitted it
        is looked up from Postgres by the instrument name.

        Returns:
            {
              "asset": "EURO FX", "market": "Currency",
              "weeks": 52, "as_of": "2025-08-26",
              "groups": {
                "dealer": {
                  "net": [["2024-09-03", -30120.0], ..., ["2025-08-26", -32050.0]],
                  "pct_change": {"1_month": 6.7, "3_month": -12.4,
                                 "6_month": 40.1, "1_year": 5.3}
                },
                "asset_mgr": {...}, "lev_money": {...}, "other_rept": {...}
              }
            }
            {} if the instrument has no cached hashes / isn't in cot_ttf.
        """
        try:
            if market is None:
                market = await self._resolve_market(asset)
                if market is None:
                    logger.info(f"{asset} not found in cot_ttf")
                    return {}

            # {report_date: hash} for the newest `weeks` reports, oldest-first.
            _, by_date = await self._fetch_recent_weeks(market, asset, weeks)
            if not by_date:
                logger.info(f"No cached COT hashes for {market}:{asset}")
                return {}

            groups, as_of = self._group_changes_history(by_date)
            if as_of is None:
                return {}

            return {
                "asset": asset,
                "market": market,
                "weeks": weeks,
                "as_of": as_of,
                "groups": groups,
            }
        except Exception as e:
            logger.error(f"Error computing asset group changes for {asset}: {e}", exc_info=True)
            raise

    async def get_instrument(self, asset: str, market: str | None = None,
                             weeks: int = 52) -> dict:
        """Everything this controller knows about ONE instrument, from a
        single Redis fetch of its last `weeks` weekly reports:

          - "changes"     : net-position series + trailing pct change per TFF
                            trader group (dealer / asset_mgr / lev_money /
                            other_rept) - same shape as asset_group_changes'
                            "groups".
          - "positioning" : percentile / score / z / mom_4w / label per
                            _POS_LEGS category (asset_mgr, lev_money, dealer),
                            computed fresh over this fetch via
                            _positioning_metrics - NOT read from the cot_pos
                            cache, so this works for any cot_ttf instrument,
                            curated or not, on demand.
          - "net_pct_oi"  : weekly net-%-of-open-interest history per
                            _POS_LEGS category (_net_pct_oi_history).
          - "summary"     : the cached LLM summary from cot_pos:{asset}, if
                            the positioning cron has already scored this
                            instrument. Never generated on demand - an LLM
                            call is too slow for a request path. None if
                            there's no cached blob yet.
          - "raw"         : the unprocessed weekly hashes themselves, oldest
                            first - [{"date": "2024-09-03", <every field CFTC
                            publishes for that week: long/short legs per
                            group, open_interest_all, traders_* counts, the
                            precomputed *_net columns, ...>}, ...]. Straight
                            off cot_ttf:{market}:{asset}:{date} via
                            insert_cot_redis, so every value is still the raw
                            string Redis stored (decode_responses=True) -
                            callers coerce types themselves, same as this
                            method does internally for the derived fields.

        Returns {} if the instrument isn't in cot_ttf or has no cached hashes.
        """
        try:
            if market is None:
                market = await self._resolve_market(asset)
                if market is None:
                    logger.info(f"{asset} not found in cot_ttf")
                    return {}

            _, by_date = await self._fetch_recent_weeks(market, asset, weeks)
            if not by_date:
                logger.info(f"No cached COT hashes for {market}:{asset}")
                return {}

            changes, as_of = self._group_changes_history(by_date)
            if as_of is None:
                return {}

            positioning = self._positioning_metrics({asset: by_date}).get(asset, {})
            net_pct_oi = self._net_pct_oi_history(by_date)

            # Cheap GET, no LLM call - reuse whatever summary the positioning
            # cron already generated for this instrument, if any.
            summary_raw = await self.aioredis.get(f"{COT_POS_KEY_PREFIX}:{asset}")
            summary = json.loads(summary_raw).get("summary") if summary_raw else None

            # The unprocessed weekly hashes, oldest-first - by_date is already
            # {date: hash}, just flatten it into a list ordered the same way
            # as every other series in this response.
            raw = [{"date": d, **by_date[d]} for d in sorted(by_date)]

            return {
                "asset": asset,
                "market": market,
                "weeks": weeks,
                "as_of": as_of,
                "positioning": positioning,
                "net_pct_oi": net_pct_oi,
                "changes": changes,
                "summary": summary,
                "raw": raw,
            }
        except Exception as e:
            logger.error(f"Error building instrument snapshot for {asset}: {e}", exc_info=True)
            raise

    async def instituitional_pos(self):
        """Institutional (asset-manager) positioning per instrument, plus
        leveraged-money and dealer positioning for context, for the curated
        COT_CURATED_ASSETS shortlist.

        Pulls the last 52 weekly COT reports for each, runs
        `_positioning_metrics`, and caches via `store_positioning` (per-asset
        blobs with an LLM summary each + the cot_pos:_meta index). Returns
        {asset: {category: {metric: value}}}.
        """
        try:
            # Fan out: one _fetch_last_52 per (market, asset), awaited together.
            tasks = [
                self._fetch_last_52(market, asset)
                for market, assets in COT_CURATED_ASSETS.items()
                for asset in assets
            ]
            results = await asyncio.gather(*tasks)

            # Keep only instruments that actually had cached weekly hashes.
            data = {asset: rows for asset, rows in results if rows}
            # Score them (percentile / z / momentum / label per category)...
            metrics = self._positioning_metrics(data)
            # ...and write cot_pos:{asset} blobs + the cot_pos:_meta index.
            await self.store_positioning(metrics)
            return metrics

        except Exception as e:
            logger.error(f"Error calculating instituitional Positioning:{e}", exc_info=True)
            raise

    async def ensure_positioning(self, max_age_hours: int = 24 * 4) -> None:
        """Populate the curated cot_pos:* snapshot only if it is missing or
        older than `max_age_hours`. Meant for app startup: when the cache is
        warm this is a single GET, so it is safe to call on every boot without
        paying for the LLM fan-out each time.

        The default 4-day window comfortably spans one weekly COT release
        cycle, so a running app that also has the worker cron below will
        essentially always hit the cheap path here.
        """
        try:
            # store_positioning stamps _meta with an ISO "updated" timestamp.
            raw = await self.aioredis.get(COT_POS_META_KEY)
            if raw:
                updated = json.loads(raw).get("updated")
                if updated:
                    # Fresh enough -> the cheap path: no fetch, no scoring, no LLM.
                    age = datetime.now() - datetime.fromisoformat(updated)
                    if age < timedelta(hours=max_age_hours):
                        logger.info(f"cot_pos snapshot is {age} old - skipping refresh")
                        return
            # No index, or it's stale -> do the full curated rebuild.
            logger.info("cot_pos snapshot missing or stale - rebuilding")
            await self.instituitional_pos()
        except Exception as e:
            logger.error(f"ensure_positioning failed: {e}", exc_info=True)
            raise

    async def _write_positioning(self, metrics: dict, meta_key: str, meta: dict,
                                 ttl: int, with_summary: bool) -> None:
        """Shared writer for both positioning snapshots: one
        f"{COT_POS_KEY_PREFIX}:{asset}" JSON blob per instrument, plus the
        `meta` index blob at `meta_key`, all in a single pipeline / round trip.

        with_summary=True fans out one LLM `breakdown_inst_positioning` per
        instrument (return_exceptions=True - a failed one is logged and that
        instrument is stored without a "summary" rather than sinking the batch).

        The caller owns `meta` and `meta_key`: store_positioning writes the
        curated COT_POS_META_KEY; store_full_positioning writes
        COT_POS_META_ALL_KEY and never touches the curated key.
        """
        assets = list(metrics.keys())

        # One LLM summary per instrument, launched together. The LLMController
        # caps its own concurrency and retries 429s, so the fan-out is safe.
        summaries: list = []
        if with_summary:
            summaries = await asyncio.gather(
                *(self.llm.breakdown_inst_positioning(metrics[a]) for a in assets),
                return_exceptions=True,
            )

        # N per-instrument blobs + the meta blob, same TTL, one round trip.
        # Instrument names carry spaces / "&" - fine inside a Redis key.
        pipe = self.aioredis.pipeline()
        for i, asset in enumerate(assets):
            cats = metrics[asset]
            if with_summary:
                summary = summaries[i]
                if isinstance(summary, Exception):
                    logger.error(f"breakdown_inst_positioning failed for {asset}: {summary}")
                else:
                    cats["summary"] = summary
            pipe.set(f"{COT_POS_KEY_PREFIX}:{asset}", json.dumps(cats), ex=ttl)
        pipe.set(meta_key, json.dumps(meta), ex=ttl)
        await pipe.execute()

    async def store_positioning(self, metrics: dict, ttl: int = COT_POS_TTL) -> None:
        """Cache instituitional_pos()'s output: one JSON blob per instrument at
        f"{COT_POS_KEY_PREFIX}:{asset}" ({category: {net_pct_oi, percentile,
        score, z, mom_4w, label}}), each with an LLM summary, plus the
        COT_POS_META_KEY index (the curated shortlist + update timestamp).

        _meta lets get_positioning() enumerate the per-instrument keys with a
        single GET instead of a SCAN, and carries the freshness timestamp the
        frontend shows.
        """
        try:
            # Never overwrite a good snapshot with an empty one (e.g. a run
            # where every instrument was short on history).
            if not metrics:
                logger.info("No positioning metrics to store")
                return

            meta = {
                "instruments": list(metrics.keys()),
                "updated": datetime.now().isoformat(),
            }
            await self._write_positioning(metrics, COT_POS_META_KEY, meta,
                                          ttl, with_summary=True)
        except Exception as e:
            logger.error(f"Error storing positioning metrics to redis: {e}", exc_info=True)
            raise

    async def full_positioning(self, batch_size: int = COT_POS_FULL_BATCH,
                               with_summary: bool = True) -> dict:
        """Positioning metrics for every cot_ttf instrument that is NOT in
        COT_CURATED_ASSETS (the long tail).

        Discovers the universe from Postgres (get_distinct_instruments),
        subtracts the curated names, fetches each instrument's last 52 weekly
        reports in bounded batches, scores them via `_positioning_metrics`, and
        caches through `store_full_positioning`. Returns
        {asset: {category: {metric: value}}}.

        with_summary=True (default) also generates an LLM breakdown per
        instrument (same as the curated flow). This is the slow part - the
        LLMController caps concurrency at 2 and pauses ~10s per call, so a few
        hundred instruments take a while; pass with_summary=False for a
        metrics-only refresh.
        """
        try:
            curated = {a for assets in COT_CURATED_ASSETS.values() for a in assets}
            instruments = [
                (market, name)
                for market, name in await self.cot.get_distinct_instruments()
                if name not in curated
            ]
            if not instruments:
                logger.info("No non-curated instruments to score")
                return {}

            # Bounded fan-out: batch_size instruments per gather so hundreds of
            # contracts don't open hundreds of SCANs/pipelines at once.
            data: dict = {}
            for i in range(0, len(instruments), batch_size):
                batch = instruments[i:i + batch_size]
                results = await asyncio.gather(
                    *(self._fetch_last_52(market, name) for market, name in batch)
                )
                data.update({name: rows for name, rows in results if rows})

            metrics = self._positioning_metrics(data)
            await self.store_full_positioning(metrics, with_summary=with_summary)
            return metrics
        except Exception as e:
            logger.error(f"Error calculating full positioning: {e}", exc_info=True)
            raise

    async def store_full_positioning(self, metrics: dict, ttl: int = COT_POS_TTL,
                                     with_summary: bool = True) -> None:
        """Cache the long tail's positioning: one f"{COT_POS_KEY_PREFIX}:{asset}"
        blob per instrument, plus a COT_POS_META_ALL_KEY index listing
        curated + tail instruments (with a separate `tail` list).

        with_summary=True attaches an LLM `breakdown_inst_positioning` to each
        blob (same as store_positioning); with_summary=False skips the LLM
        entirely (metrics only).

        Reads COT_POS_META_KEY for the curated names but NEVER writes it - that
        key stays store_positioning's, listing only the curated shortlist.
        """
        try:
            if not metrics:
                logger.info("No full positioning metrics to store")
                return

            # Curated names, read-only, from the curated index.
            curated_raw = await self.aioredis.get(COT_POS_META_KEY)
            curated = json.loads(curated_raw).get("instruments", []) if curated_raw else []

            meta_all = {
                "instruments": sorted(set(curated) | set(metrics)),
                "tail": sorted(metrics),
                "updated": datetime.now().isoformat(),
            }
            await self._write_positioning(metrics, COT_POS_META_ALL_KEY, meta_all,
                                          ttl, with_summary=with_summary)
        except Exception as e:
            logger.error(f"Error storing full positioning to redis: {e}", exc_info=True)
            raise

    async def get_positioning(self, scope: str = "tracked") -> dict:
        """Read a cached positioning snapshot back out of Redis.

        scope="tracked" (default): the curated shortlist, via COT_POS_META_KEY.
        scope="all": curated + long tail, via COT_POS_META_ALL_KEY. Curated
        instruments carry a "summary"; tail ones don't.

        Returns {"meta": {...}, "instruments": {asset: {category: {metrics}}}}.
        An empty "instruments" dict means that index has expired or its
        producer has never run.
        """
        try:
            meta_key = COT_POS_META_ALL_KEY if scope == "all" else COT_POS_META_KEY

            # decode_responses=True on the pool -> str, or None if the key is
            # gone. A missing index is normal (never populated / expired).
            meta_raw = await self.aioredis.get(meta_key)
            meta = json.loads(meta_raw) if meta_raw else {}

            # Normal path: instrument list straight from the index.
            assets = meta.get("instruments")
            if not assets:
                # Fallback: index gone but per-instrument blobs survive - SCAN
                # for them. This can't tell curated from tail, so it returns
                # every cached instrument regardless of `scope`.
                prefix = f"{COT_POS_KEY_PREFIX}:"
                assets = [
                    k[len(prefix):]
                    async for k in self.aioredis.scan_iter(match=f"{prefix}*", count=100)
                    if not k.endswith((":_meta", ":_meta_all"))
                ]

            if not assets:
                return {"meta": meta, "instruments": {}}

            # One pipelined batch of GETs for every instrument blob.
            pipe = self.aioredis.pipeline()
            for asset in assets:
                pipe.get(f"{COT_POS_KEY_PREFIX}:{asset}")
            rows = await pipe.execute()

            # Skip any instrument whose key expired between the index read and
            # this batch (row is None).
            instruments = {
                asset: json.loads(row)
                for asset, row in zip(assets, rows)
                if row is not None
            }
            return {"meta": meta, "instruments": instruments}
        except Exception as e:
            logger.error(f"Error getting positioning metrics from redis: {e}", exc_info=True)
            raise
# if __name__ == "__main__":
#     test = COTController()
#     print(asyncio.run(test.asset_group_changes("EURO FX")))
