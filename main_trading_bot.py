import os
import time
import json
import pytz
import pyotp
import asyncio
import tempfile
import datetime
import threading
import requests
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# SmartAPI SDK imports
try:
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except ImportError:
    SmartConnect = None
    SmartWebSocketV2 = None

# ==================== CONFIGURATION & RISK LIMITS ====================
load_dotenv()

# Credentials from environment
API_KEY = os.getenv('ANGEL_API_KEY', '')
CLIENT_ID = os.getenv('CLIENT_ID', '')
PASSWORD = os.getenv('PASSWORD', '')
TOTP_SECRET_KEY = os.getenv('TOTP_SECRET_KEY', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# Trading mode: Set to True for live order placement, False for paper simulation
LIVE_EXECUTION = False

# Timezone
IST = pytz.timezone('Asia/Kolkata')

# Exchange & Instrument Parameters
INDEX_TOKEN = "99926000"       # NSE NIFTY 50 Index token
NIFTY_LOT_SIZE = 65            # Configurable lot size (update based on current exchange contract specs)
LOTS = 5                       # Default trade size in lots
TOTAL_QTY = NIFTY_LOT_SIZE * LOTS

# Risk Management Limits
MAX_DAILY_LOSS = -6000.0       # Max daily loss threshold in INR
MAX_DAILY_TRADES = 6           # Max trades per day
SLIPPAGE = 0.50                # Modeled slippage per leg in points
FRICTION_PER_LOT = 9.0         # Approximate round-trip STT, exchange turnover, and broker fees per lot
WATCHDOG_TIMEOUT = 15.0        # Seconds without tick before triggering reconnect (15s avoids false alarms)

# ==================== GLOBALS ====================
live_prices = {}
ws_instance = None
last_tick_time = time.time()
telegram_urgent = asyncio.Queue()
telegram_batch = asyncio.Queue()
MAIN_LOOP = None
is_reconnecting = False


# ==================== ASYNC TELEGRAM DISPATCHER ====================
async def telegram_worker():
    """
    Background worker that handles outbound Telegram alerts.
    Dispatches urgent messages immediately and batches lower-priority updates.
    """
    batch_buffer = []
    last_batch_time = time.time()

    async def send_to_tg(text: str):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print(f"[Telegram Console Output] {text}")
            return
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML"
            }
            await asyncio.to_thread(requests.post, url, json=payload, timeout=5)
        except Exception as e:
            print(f"Telegram Alert Failed: {e}")

    while True:
        # 1. Dispatch Urgent Messages Immediately
        try:
            msg = telegram_urgent.get_nowait()
            timestamp = datetime.datetime.now(IST).strftime("%H:%M:%S")
            await send_to_tg(f"[{timestamp}] {msg}")
            await asyncio.sleep(0.5)
            continue
        except asyncio.QueueEmpty:
            pass

        # 2. Buffer Low-Priority Messages
        try:
            msg = telegram_batch.get_nowait()
            batch_buffer.append(msg)
        except asyncio.QueueEmpty:
            pass

        # 3. Flush Batched Messages (every 3 seconds or when 4 messages accumulate)
        if batch_buffer and (time.time() - last_batch_time > 3.0 or len(batch_buffer) >= 4):
            timestamp = datetime.datetime.now(IST).strftime("%H:%M:%S")
            combined_msg = "\n".join(batch_buffer)
            await send_to_tg(f"[{timestamp}] 📋 <b>Digest:</b>\n{combined_msg}")
            batch_buffer.clear()
            last_batch_time = time.time()
            await asyncio.sleep(0.5)

        await asyncio.sleep(0.1)


def send_alert(message: str, urgent: bool = True):
    """Enqueue alert to the Telegram worker thread-safely."""
    global MAIN_LOOP
    if MAIN_LOOP and MAIN_LOOP.is_running():
        queue = telegram_urgent if urgent else telegram_batch
        MAIN_LOOP.call_soon_threadsafe(queue.put_nowait, message)
    else:
        print(f"[{'URGENT' if urgent else 'INFO'}] {message}")


# ==================== WEBSOCKET HANDLERS ====================
def on_data(wsapp, message):
    """Handles real-time streaming market tick payloads from SmartWebSocketV2."""
    global last_tick_time
    try:
        token = str(message.get("token"))
        ltp = message.get("last_traded_price", 0)
        vol = message.get("volume_traded_today", 0)
        if token and ltp:
            # Angel One sends prices in paise (e.g. 2500000 -> 25000.0 or 10500 -> 105.0)
            price = float(ltp)
            if isinstance(ltp, int) or price > 50000:
                price = price / 100.0
            live_prices[token] = {
                "ltp": price,
                "vol": int(vol or 0),
                "time": time.time()
            }
            last_tick_time = time.time()
    except Exception:
        pass


def on_open(wsapp):
    send_alert("🟢 <b>WebSocket:</b> Connection Opened.", urgent=False)


def on_error(wsapp, error):
    send_alert(f"🔴 <b>WebSocket Error:</b> {error}", urgent=True)


def on_close(wsapp):
    send_alert("🔴 <b>WebSocket:</b> Connection Closed.", urgent=False)


def subscribe_tokens(ce_token=None, pe_token=None):
    """Subscribes to Index feed and optional active CE/PE options."""
    global ws_instance
    if not ws_instance:
        return
    token_list = [{"exchangeType": 1, "tokens": [INDEX_TOKEN]}]
    opt_tokens = [str(t) for t in [ce_token, pe_token] if t]
    if opt_tokens:
        token_list.append({"exchangeType": 2, "tokens": opt_tokens})
    try:
        ws_instance.subscribe("token_subscription", 1, token_list)
        send_alert("📡 Subscribed to market feeds.", urgent=False)
    except Exception as e:
        send_alert(f"🔴 Subscribe Error: {e}", urgent=True)


def unsubscribe_tokens(ce_token=None, pe_token=None):
    """Safely unsubscribes from active option tokens and flushes cached prices."""
    global ws_instance
    tokens = [str(t) for t in [ce_token, pe_token] if t]
    if not ws_instance or not tokens:
        return
    token_list = [{"exchangeType": 2, "tokens": tokens}]
    try:
        ws_instance.unsubscribe("token_subscription", 1, token_list)
    except Exception:
        pass
    for t in tokens:
        live_prices.pop(t, None)


# ==================== STRIKE SELECTOR ====================
class StrikeSelector:
    """Handles daily scrip master caching, ATM strike calculation, and dynamic strike selection."""

    def __init__(self, api_client):
        self.api = api_client
        self.scrip_df = pd.DataFrame()
        self.load_scrip_master()

    def load_scrip_master(self):
        """Loads scrip master from local cache or downloads from Angel One API."""
        today_str = datetime.datetime.now(IST).strftime("%Y%m%d")
        cache_file = f"scrip_master_{today_str}.json"
        if os.path.exists(cache_file):
            try:
                self.scrip_df = pd.read_json(cache_file)
                self.scrip_df['expiry_date'] = pd.to_datetime(self.scrip_df['expiry_date'])
                self.scrip_df['strike_num'] = pd.to_numeric(self.scrip_df['strike_num'], errors='coerce')
                self.scrip_df['token'] = self.scrip_df['token'].astype(str)
                return
            except Exception:
                pass

        try:
            send_alert("📥 Downloading Scrip Master...", urgent=False)
            response = requests.get(
                "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
                timeout=15
            )
            df = pd.DataFrame(response.json())
            df = df[(df['name'] == 'NIFTY') & (df['exch_seg'] == 'NFO') & (df['instrumenttype'] == 'OPTIDX')].copy()
            # Convert strike (typically stored in paise by Angel One) to standard value
            df['strike_num'] = (pd.to_numeric(df['strike'], errors='coerce') / 100.0).round(2)
            df['expiry_date'] = pd.to_datetime(df['expiry'])
            df['token'] = df['token'].astype(str)
            self.scrip_df = df
            self.scrip_df.to_json(cache_file)
        except Exception as e:
            send_alert(f"🔴 Scrip Master Error: {e}", urgent=True)

    def get_tokens(self, current_index_ltp: float):
        """
        Determines target CE and PE contracts based on current index LTP.
        - Expiry Day (>= 12:30 PM): Uses ATM strikes.
        - Standard Regime: Evaluates strikes within +/- 400 pts and selects those closest to ~₹100 LTP.
        """
        if self.scrip_df.empty or not current_index_ltp:
            return None, None, None, None, None

        atm_strike = round(current_index_ltp / 50.0) * 50
        now = datetime.datetime.now(IST)
        future_expiries = self.scrip_df[self.scrip_df['expiry_date'] >= pd.Timestamp(now.date())]
        if future_expiries.empty:
            return None, None, None, None, None

        nearest_expiry = future_expiries['expiry_date'].min()
        active_contracts = future_expiries[future_expiries['expiry_date'] == nearest_expiry]

        # 1. EXPIRY REGIME (Thursday/Expiry Day after 12:30 PM: pick ATM)
        if nearest_expiry.date() == now.date() and now.time() >= datetime.time(12, 30):
            ce_match = active_contracts[
                (active_contracts['strike_num'] == atm_strike) &
                (active_contracts['symbol'].str.strip().str.upper().str.endswith('CE'))
            ]
            pe_match = active_contracts[
                (active_contracts['strike_num'] == atm_strike) &
                (active_contracts['symbol'].str.strip().str.upper().str.endswith('PE'))
            ]
            if not ce_match.empty and not pe_match.empty:
                return (
                    atm_strike,
                    ce_match.iloc[0]['symbol'],
                    str(ce_match.iloc[0]['token']),
                    pe_match.iloc[0]['symbol'],
                    str(pe_match.iloc[0]['token'])
                )

        # 2. STANDARD REGIME (Look for contracts trading near ~₹100)
        range_contracts = active_contracts[
            (active_contracts['strike_num'] >= atm_strike - 400) &
            (active_contracts['strike_num'] <= atm_strike + 400)
        ]
        ce_sub = range_contracts[range_contracts['symbol'].str.strip().str.upper().str.endswith('CE')]
        pe_sub = range_contracts[range_contracts['symbol'].str.strip().str.upper().str.endswith('PE')]

        ce_tokens = ce_sub['token'].astype(str).tolist()
        pe_tokens = pe_sub['token'].astype(str).tolist()

        market_data = []
        if self.api and (ce_tokens or pe_tokens):
            try:
                res = self.api.getMarketData(mode="LTP", exchangeTokens={"NFO": ce_tokens + pe_tokens})
                if isinstance(res, dict):
                    market_data = res.get('data', {}).get('fetched', [])
            except Exception:
                market_data = []

        best_ce, best_pe = None, None
        min_ce_diff, min_pe_diff = float('inf'), float('inf')

        for item in market_data:
            token = str(item.get('exchangeToken'))
            ltp = float(item.get('ltp', 0))
            if ltp <= 0:
                continue
            diff = abs(ltp - 100.0)
            if token in ce_tokens and diff < min_ce_diff:
                min_ce_diff, best_ce = diff, token
            elif token in pe_tokens and diff < min_pe_diff:
                min_pe_diff, best_pe = diff, token

        if best_ce and best_pe:
            ce_row = range_contracts[range_contracts['token'].astype(str) == best_ce].iloc[0]
            pe_row = range_contracts[range_contracts['token'].astype(str) == best_pe].iloc[0]
            return atm_strike, ce_row['symbol'], str(ce_row['token']), pe_row['symbol'], str(pe_row['token'])

        # Fallback to ATM strikes if LTP lookup fails or returns empty outside market hours
        ce_atm = active_contracts[
            (active_contracts['strike_num'] == atm_strike) &
            (active_contracts['symbol'].str.strip().str.upper().str.endswith('CE'))
        ]
        pe_atm = active_contracts[
            (active_contracts['strike_num'] == atm_strike) &
            (active_contracts['symbol'].str.strip().str.upper().str.endswith('PE'))
        ]
        if not ce_atm.empty and not pe_atm.empty:
            return (
                atm_strike,
                ce_atm.iloc[0]['symbol'],
                str(ce_atm.iloc[0]['token']),
                pe_atm.iloc[0]['symbol'],
                str(pe_atm.iloc[0]['token'])
            )

        return None, None, None, None, None


# ==================== O(1) STREAMING INDICATORS ====================
class HybridIndicators:
    """
    Maintains continuous O(1) updates for VWAP, EMA(9), EMA(15), ATR(14), and Volume Profile (POC).
    Fixes the candle boundary bug by cleanly separating candle finalization from new candle initialization.
    """

    def __init__(self):
        self.ema9 = None
        self.ema15 = None
        self.vwap = 0.0
        self.cum_vol = 0
        self.cum_pv = 0.0
        self.prev_close = None
        self.atr14 = 0.0
        self.vol_profile = {}
        self.last_minute = None
        self.candle_high = 0.0
        self.candle_low = 0.0
        self.candle_close = 0.0
        self._last_tick_time = None
        self._last_candle_vol = 0

    def seed_from_rest(self, df: pd.DataFrame):
        """Initializes technical indicators and volume profile from historical 1-minute REST candle data."""
        if df.empty or len(df) < 15:
            return
        self.ema9 = float(df['close'].ewm(span=9, adjust=False).mean().iloc[-1])
        self.ema15 = float(df['close'].ewm(span=15, adjust=False).mean().iloc[-1])

        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        self.atr14 = float(true_range.rolling(14).mean().iloc[-1])
        self.prev_close = float(df['close'].iloc[-1])

        recent_30 = df.tail(30)
        self.vol_profile = {}
        for _, row in recent_30.iterrows():
            price_level = round(float(row['close']))
            self.vol_profile[price_level] = self.vol_profile.get(price_level, 0) + int(row.get('volume', 0))

    def get_poc(self):
        """Returns the Point of Control (price level with max volume in profile)."""
        if not self.vol_profile:
            return None
        return max(self.vol_profile, key=self.vol_profile.get)

    def update_tick(self, price: float, volume: int, tick_timestamp: float):
        """
        Processes streaming tick data in O(1) time complexity.
        Correctly finalizes the completed minute candle before initializing the new minute.
        """
        # Deduplicate identical tick timestamps to prevent redundant processing
        if self._last_tick_time == tick_timestamp:
            return
        self._last_tick_time = tick_timestamp

        # Cumulative VWAP calculation
        if volume > self.cum_vol:
            tick_vol = volume - self.cum_vol
            self.cum_vol = volume
            self.cum_pv += (price * tick_vol)
            self.vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else price
        elif self.cum_vol == 0:
            self.vwap = price

        tick_dt = datetime.datetime.fromtimestamp(tick_timestamp, tz=IST)
        current_min = tick_dt.minute

        # First tick initialization
        if self.last_minute is None:
            self.last_minute = current_min
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
            return

        # Check if 1-minute boundary has rolled over
        if current_min != self.last_minute:
            # 1. Finalize the PREVIOUS minute candle
            completed_close = self.candle_close
            completed_high = self.candle_high
            completed_low = self.candle_low

            # Update EMA(9) and EMA(15) with previous candle close
            if self.ema9 is not None:
                self.ema9 = (completed_close - self.ema9) * (2.0 / 10.0) + self.ema9
            else:
                self.ema9 = completed_close

            if self.ema15 is not None:
                self.ema15 = (completed_close - self.ema15) * (2.0 / 16.0) + self.ema15
            else:
                self.ema15 = completed_close

            # Calculate True Range for ATR
            if self.prev_close is not None:
                tr = max(
                    completed_high - completed_low,
                    abs(completed_high - self.prev_close),
                    abs(completed_low - self.prev_close)
                )
            else:
                tr = completed_high - completed_low

            if self.atr14 > 0:
                self.atr14 = ((self.atr14 * 13.0) + tr) / 14.0
            else:
                self.atr14 = tr

            # Update Volume Profile with completed bar
            bar_vol = max(0, self.cum_vol - self._last_candle_vol)
            self._last_candle_vol = self.cum_vol
            level = round(completed_close)
            self.vol_profile[level] = self.vol_profile.get(level, 0) + bar_vol

            self.prev_close = completed_close
            self.last_minute = current_min

            # 2. Initialize the NEW minute candle with current tick
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
        else:
            # Intra-minute tick updates
            if price > self.candle_high:
                self.candle_high = price
            if price < self.candle_low:
                self.candle_low = price
            self.candle_close = price


# ==================== EXECUTION ENGINE ====================
class ScalpingEngine:
    """Manages trade lifecycle, position tracking, atomic state persistence, and risk exits."""

    def __init__(self, api_client):
        self.api = api_client
        self.idx_ind = HybridIndicators()
        self.ce_ind = HybridIndicators()
        self.pe_ind = HybridIndicators()
        self.active_tokens = {
            "ce_token": None, "ce_sym": None,
            "pe_token": None, "pe_sym": None,
            "atm": 0
        }
        self.trade_state = {
            "date": "", "active": False, "type": None, "token": None, "sym": None,
            "entry_price": 0.0, "entry_idx_price": 0.0, "qty": TOTAL_QTY,
            "hard_sl": 0.0, "idx_sl": 0.0, "trailing_sl": 0.0,
            "target_1_hit": False, "entry_opt_atr": 0.0,
            "daily_pnl": 0.0, "trades_today": 0
        }
        self.trade_log = []
        self.load_state()

    def load_state(self):
        """Loads persistent trading state from disk, recovering active positions across restarts."""
        today_str = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        if os.path.exists("trade_state.json"):
            try:
                with open("trade_state.json", "r") as f:
                    saved = json.load(f)
                if saved.get("date") == today_str:
                    self.trade_state.update(saved)
                    if self.trade_state.get("active"):
                        send_alert(f"🔄 <b>Recovered Active Trade:</b> {self.trade_state.get('sym')}", urgent=True)
                else:
                    send_alert("🧹 Stale state file found. Resetting daily metrics.", urgent=False)
            except Exception:
                pass

    def save_state(self):
        """Atomically saves trade state to disk using a temporary file to avoid corruption."""
        self.trade_state["date"] = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        try:
            with tempfile.NamedTemporaryFile("w", dir=".", delete=False) as tf:
                json.dump(self.trade_state, tf, indent=2)
                temp_name = tf.name
            os.replace(temp_name, "trade_state.json")
        except Exception as e:
            print(f"Error saving trade state: {e}")

    def fetch_rest_seed(self, token: str, exchange: str = "NFO") -> pd.DataFrame:
        """Fetches 60 minutes of 1-minute historical candles from Angel One REST API."""
        if not self.api or not token:
            return pd.DataFrame()
        now = datetime.datetime.now(IST)
        from_time = (now - datetime.timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M")
        to_time = now.strftime("%Y-%m-%d %H:%M")
        try:
            res = self.api.getCandleData({
                "exchange": exchange,
                "symboltoken": str(token),
                "interval": "ONE_MINUTE",
                "fromdate": from_time,
                "todate": to_time
            })
            if res and res.get('status') and res.get('data'):
                df = pd.DataFrame(res['data'], columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
                return df.apply(pd.to_numeric, errors='ignore')
        except Exception as e:
            print(f"fetch_rest_seed error ({exchange} token {token}): {e}")
        return pd.DataFrame()

    async def execute_trade(self, opt_type: str, token: str, sym: str, raw_price: float,
                            idx_price: float, atr_opt: float, atr_idx: float):
        """Simulates or executes trade entry with slippage and initiates multi-tier stop loss."""
        if not atr_opt or atr_opt <= 0:
            atr_opt = 2.0
        if not atr_idx or atr_idx <= 0:
            atr_idx = 5.0

        entry_price = raw_price + SLIPPAGE  # Model Ask-side Slippage

        self.trade_state.update({
            "active": True,
            "type": opt_type,
            "token": str(token),
            "sym": sym,
            "entry_price": entry_price,
            "entry_idx_price": idx_price,
            "qty": TOTAL_QTY,
            "entry_opt_atr": atr_opt,
            "hard_sl": entry_price - (1.5 * atr_opt),
            "trailing_sl": entry_price - (1.5 * atr_opt),
            "idx_sl": idx_price - (1.0 * atr_idx) if opt_type == 'CE' else idx_price + (1.0 * atr_idx),
            "target_1_hit": False
        })
        self.trade_state["trades_today"] += 1
        self.save_state()

        # Optional Live Order Execution
        if LIVE_EXECUTION and self.api:
            try:
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": sym,
                    "symboltoken": str(token),
                    "transactiontype": "BUY",
                    "exchange": "NFO",
                    "ordertype": "MARKET",
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "quantity": str(TOTAL_QTY)
                }
                res = self.api.placeOrder(order_params)
                print(f"[LIVE ORDER] Placed Entry Order: {res}")
            except Exception as e:
                send_alert(f"🔴 Live Entry Order Placement Failed: {e}", urgent=True)

        mode_label = "LIVE" if LIVE_EXECUTION else "PAPER"
        send_alert(
            f"🚀 <b>{mode_label} ENTRY ({opt_type}):</b> {sym}\n"
            f"Entry: ₹{entry_price:.2f} (Slip Inc.)\n"
            f"Capital SL: ₹{self.trade_state['hard_sl']:.2f}\n"
            f"Index SL: {self.trade_state['idx_sl']:.2f}",
            urgent=True
        )

    async def exit_trade(self, raw_price: float, reason: str):
        """Exits position, accounts for slippage and friction, updates PnL, and saves state."""
        exit_price = raw_price - SLIPPAGE  # Model Bid-side Slippage
        qty = self.trade_state['qty']
        gross_pnl = (exit_price - self.trade_state['entry_price']) * qty
        friction_cost = FRICTION_PER_LOT * (qty / NIFTY_LOT_SIZE)
        net_pnl = gross_pnl - friction_cost

        self.trade_state['daily_pnl'] += net_pnl
        sym_name = self.trade_state.get('sym', 'UNKNOWN')

        self.trade_log.append({
            "sym": sym_name,
            "entry": self.trade_state['entry_price'],
            "exit": exit_price,
            "pnl": net_pnl,
            "reason": reason,
            "qty": qty
        })

        # Optional Live Order Execution
        if LIVE_EXECUTION and self.api:
            try:
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": sym_name,
                    "symboltoken": str(self.trade_state['token']),
                    "transactiontype": "SELL",
                    "exchange": "NFO",
                    "ordertype": "MARKET",
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "quantity": str(qty)
                }
                res = self.api.placeOrder(order_params)
                print(f"[LIVE ORDER] Placed Exit Order: {res}")
            except Exception as e:
                send_alert(f"🔴 Live Exit Order Placement Failed: {e}", urgent=True)

        self.trade_state["active"] = False
        self.trade_state["qty"] = TOTAL_QTY  # Reset quantity for next trade
        self.save_state()

        send_alert(
            f"🛑 <b>EXIT ({reason}):</b> {sym_name}\n"
            f"Exit: ₹{exit_price:.2f} | Net PnL: ₹{net_pnl:+.2f}",
            urgent=True
        )

    async def check_exits(self, raw_price: float, idx_price: float):
        """Evaluates exit conditions across structural stops, trailing stops, and targets."""
        if not self.trade_state["active"]:
            return
        ts = self.trade_state
        atr = ts["entry_opt_atr"]
        eval_price = raw_price - SLIPPAGE

        # 1. Structural Stop (Index Level Breach)
        if (ts['type'] == 'CE' and idx_price < ts['idx_sl']) or (ts['type'] == 'PE' and idx_price > ts['idx_sl']):
            await self.exit_trade(raw_price, "Structural Index Stop")
            return

        # 2. Capital Hard Stop / Trailing Stop
        if eval_price <= ts['trailing_sl']:
            await self.exit_trade(raw_price, "Trailing/Hard SL Hit")
            return

        # 3. Parabolic Cap (+3.5 ATR target exit)
        if eval_price >= ts['entry_price'] + (3.5 * atr):
            await self.exit_trade(raw_price, "Parabolic Target (3.5 ATR)")
            return

        # 4. Partial Exit & Cost-Trailing (+2.0 ATR hit: book 3 lots, trail stop to +1.2 ATR)
        if not ts['target_1_hit'] and eval_price >= ts['entry_price'] + (2.0 * atr):
            ts['target_1_hit'] = True
            ts['trailing_sl'] = ts['entry_price'] + (1.2 * atr)
            partial_qty = 3 * NIFTY_LOT_SIZE
            ts['qty'] -= partial_qty
            gross_pnl = (eval_price - ts['entry_price']) * partial_qty
            friction_cost = FRICTION_PER_LOT * (partial_qty / NIFTY_LOT_SIZE)
            net_pnl = gross_pnl - friction_cost
            self.trade_state['daily_pnl'] += net_pnl

            self.trade_log.append({
                "sym": ts['sym'],
                "entry": ts['entry_price'],
                "exit": eval_price,
                "pnl": net_pnl,
                "reason": "Partial Profit Booked",
                "qty": partial_qty
            })

            if LIVE_EXECUTION and self.api:
                try:
                    order_params = {
                        "variety": "NORMAL",
                        "tradingsymbol": ts['sym'],
                        "symboltoken": str(ts['token']),
                        "transactiontype": "SELL",
                        "exchange": "NFO",
                        "ordertype": "MARKET",
                        "producttype": "INTRADAY",
                        "duration": "DAY",
                        "quantity": str(partial_qty)
                    }
                    self.api.placeOrder(order_params)
                except Exception as e:
                    send_alert(f"🔴 Live Partial Exit Failed: {e}", urgent=True)

            self.save_state()
            send_alert(
                f"💰 <b>PARTIAL BOOKED (2.0 ATR):</b> {ts['sym']}\n"
                f"Net PnL: ₹{net_pnl:+.2f}\n"
                f"Trail SL: ₹{ts['trailing_sl']:.2f}",
                urgent=True
            )

        # 5. Trail Stop to Cost at +1.0 ATR
        elif not ts['target_1_hit'] and eval_price >= ts['entry_price'] + (1.0 * atr):
            if ts['trailing_sl'] < ts['entry_price']:
                ts['trailing_sl'] = ts['entry_price']
                self.save_state()
                send_alert("🛡️ <b>SL Trailed to Cost (1.0 ATR)</b>", urgent=False)


# ==================== MAIN LIFECYCLE ====================
async def main_loop():
    """Main event loop coordinating authentication, data feeds, dynamic strikes, and trading signals."""
    global ws_instance, last_tick_time, MAIN_LOOP, is_reconnecting
    MAIN_LOOP = asyncio.get_running_loop()
    asyncio.create_task(telegram_worker())

    api = None
    engine = None
    selector = None
    session_data = None
    is_sleeping = False

    while True:
        now = datetime.datetime.now(IST)
        current_time = now.time()

        # ------------------------------------------------------------------
        # 1. Weekend / Night Sleep (Before 08:30 or after 15:30 IST)
        # ------------------------------------------------------------------
        if now.weekday() >= 5 or current_time >= datetime.time(15, 30) or current_time < datetime.time(8, 30):
            if not is_sleeping:
                if ws_instance:
                    try:
                        ws_instance.close()
                    except Exception:
                        pass
                    ws_instance = None
                send_alert("💤 <b>System Sleeping.</b>", urgent=False)
                is_sleeping = True
            await asyncio.sleep(60)
            continue
        else:
            is_sleeping = False

        # ------------------------------------------------------------------
        # 2. Session Boot & Recovery (08:30 - 15:25 IST)
        # Connects if started during pre-market or recovered mid-session
        # ------------------------------------------------------------------
        if not ws_instance and datetime.time(8, 30) <= current_time < datetime.time(15, 26):
            phase = "Pre-Market Boot" if current_time < datetime.time(9, 15) else "Live Session Recovery Boot"
            send_alert(f"⚙️ <b>{phase}:</b> Authenticating...", urgent=True)

            try:
                if SmartConnect is None or SmartWebSocketV2 is None:
                    raise ImportError("SmartApi package is not installed. Please install 'smartapi-python'.")

                api = SmartConnect(api_key=API_KEY)
                totp = pyotp.TOTP(TOTP_SECRET_KEY).now()
                session = api.generateSession(CLIENT_ID, PASSWORD, totp)

                if session.get('status'):
                    session_data = session['data']
                    engine = ScalpingEngine(api)
                    selector = StrikeSelector(api)
                    feed_token = session_data.get('feedToken') or api.getfeedToken()

                    ws_instance = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_ID, feed_token
                    )
                    ws_instance.on_data = on_data
                    ws_instance.on_open = on_open
                    ws_instance.on_error = on_error
                    ws_instance.on_close = on_close

                    threading.Thread(target=ws_instance.connect, daemon=True).start()

                    if engine.trade_state["active"]:
                        subscribe_tokens(engine.trade_state["token"], None)
                    else:
                        subscribe_tokens(None, None)
                    send_alert("✅ <b>Authentication Successful:</b> Connected to SmartAPI.", urgent=False)
                else:
                    send_alert(f"🔴 <b>Login Failed:</b> {session.get('message', 'Invalid credentials')}", urgent=True)
            except Exception as e:
                send_alert(f"🔴 <b>Session Initialization Error:</b> {e}", urgent=True)

            # Sleep briefly to give WebSocket time to open
            await asyncio.sleep(10 if current_time >= datetime.time(9, 15) else 60)
            continue

        # ------------------------------------------------------------------
        # 3. Dynamic Strike Tracking (09:15 - 15:15 IST)
        # Shifts tracked strikes if Index moves >= 200 pts from anchor
        # ------------------------------------------------------------------
        if engine and selector and datetime.time(9, 15) <= current_time < datetime.time(15, 15):
            idx_data = live_prices.get(INDEX_TOKEN)
            if idx_data and not engine.trade_state['active']:
                idx_ltp = idx_data['ltp']
                anchor = engine.active_tokens['atm']

                if not anchor or abs(idx_ltp - anchor) >= 200:
                    atm, ce_sym, ce_token, pe_sym, pe_token = selector.get_tokens(idx_ltp)
                    if ce_token and pe_token:
                        unsubscribe_tokens(
                            engine.active_tokens.get('ce_token'),
                            engine.active_tokens.get('pe_token')
                        )
                        engine.active_tokens.update({
                            "atm": atm, "ce_sym": ce_sym, "ce_token": ce_token,
                            "pe_sym": pe_sym, "pe_token": pe_token
                        })
                        subscribe_tokens(ce_token, pe_token)
                        send_alert(f"🎯 <b>Strikes Shifted:</b>\nCE: {ce_sym}\nPE: {pe_sym}", urgent=False)

                        # Seed indicator history via non-blocking REST calls
                        idx_seed = await asyncio.to_thread(engine.fetch_rest_seed, INDEX_TOKEN, "NSE")
                        ce_seed = await asyncio.to_thread(engine.fetch_rest_seed, ce_token, "NFO")
                        pe_seed = await asyncio.to_thread(engine.fetch_rest_seed, pe_token, "NFO")
                        engine.idx_ind.seed_from_rest(idx_seed)
                        engine.ce_ind.seed_from_rest(ce_seed)
                        engine.pe_ind.seed_from_rest(pe_seed)

        # ------------------------------------------------------------------
        # 4. Watchdog & Auto-Reconnection
        # Detects silent socket drops if no ticks received for WATCHDOG_TIMEOUT seconds
        # ------------------------------------------------------------------
        if (current_time >= datetime.time(9, 15) and
                (time.time() - last_tick_time > WATCHDOG_TIMEOUT) and
                not is_reconnecting and ws_instance):
            is_reconnecting = True
            send_alert("⚠️ <b>Watchdog:</b> Stale socket. Reconnecting...", urgent=True)
            try:
                ws_instance.close()
            except Exception:
                pass
            await asyncio.sleep(2)

            if api and session_data:
                try:
                    feed_token = session_data.get('feedToken') or api.getfeedToken()
                    ws_instance = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_ID, feed_token
                    )
                    ws_instance.on_data = on_data
                    ws_instance.on_open = on_open
                    ws_instance.on_error = on_error
                    ws_instance.on_close = on_close
                    threading.Thread(target=ws_instance.connect, daemon=True).start()
                    last_tick_time = time.time()

                    if engine and engine.trade_state["active"]:
                        subscribe_tokens(engine.trade_state["token"], None)
                    elif engine and engine.active_tokens.get('ce_token'):
                        subscribe_tokens(
                            engine.active_tokens['ce_token'],
                            engine.active_tokens['pe_token']
                        )

                    if engine:
                        idx_seed = await asyncio.to_thread(engine.fetch_rest_seed, INDEX_TOKEN, "NSE")
                        engine.idx_ind.seed_from_rest(idx_seed)
                except Exception as e:
                    send_alert(f"🔴 Watchdog Reconnect Failed: {e}", urgent=True)

            is_reconnecting = False

        # ------------------------------------------------------------------
        # 5. Terminal Square-Off (15:26 IST)
        # Closes any open trade and publishes daily performance report
        # ------------------------------------------------------------------
        if current_time >= datetime.time(15, 26) and current_time < datetime.time(15, 27):
            if engine and engine.trade_state['active']:
                token = engine.trade_state['token']
                ltp = live_prices.get(token, {}).get('ltp', 0.0)
                await engine.exit_trade(ltp, "15:26 Hard Square-Off")

            if engine and engine.trade_log:
                wins = sum(1 for t in engine.trade_log if t['pnl'] > 0)
                total_legs = len(engine.trade_log)
                win_rate = (wins / total_legs * 100.0) if total_legs > 0 else 0.0
                send_alert(
                    f"🛑 <b>EOD REPORT</b>\n"
                    f"Total Legs: {total_legs}\n"
                    f"Win Rate: {win_rate:.1f}%\n"
                    f"Net PnL: ₹{engine.trade_state['daily_pnl']:.2f}",
                    urgent=True
                )
                engine.trade_log.clear()

            await asyncio.sleep(60)
            continue

        # ------------------------------------------------------------------
        # 6. Live Execution Loop (09:15 - 15:15 IST)
        # Evaluates indicators, circuit breakers, and entry/exit conditions
        # ------------------------------------------------------------------
        if engine and datetime.time(9, 15) <= current_time < datetime.time(15, 15):
            idx = live_prices.get(INDEX_TOKEN)
            ce = live_prices.get(engine.active_tokens.get('ce_token'))
            pe = live_prices.get(engine.active_tokens.get('pe_token'))

            if idx:
                engine.idx_ind.update_tick(idx['ltp'], idx['vol'], idx['time'])
            if ce:
                engine.ce_ind.update_tick(ce['ltp'], ce['vol'], ce['time'])
            if pe:
                engine.pe_ind.update_tick(pe['ltp'], pe['vol'], pe['time'])

            # Circuit Breakers Check
            breaker_hit = (
                (engine.trade_state['daily_pnl'] <= MAX_DAILY_LOSS) or
                (engine.trade_state['trades_today'] >= MAX_DAILY_TRADES)
            )

            # Manage Active Position
            if engine.trade_state['active']:
                opt = live_prices.get(engine.trade_state['token'])
                if opt and idx:
                    await engine.check_exits(opt['ltp'], idx['ltp'])

            # Check New Entries if circuit breaker is not breached
            elif not breaker_hit and idx and engine.idx_ind.prev_close:
                trade_executed = False

                # Check CE (Calls) Entry
                if ce and ce['ltp'] > engine.ce_ind.vwap and engine.ce_ind.prev_close:
                    poc_ce = engine.ce_ind.get_poc()
                    ema_bullish = (engine.ce_ind.ema9 is not None) and (engine.ce_ind.ema9 > engine.ce_ind.ema15)
                    rg_divergence = (idx['ltp'] < engine.idx_ind.prev_close) and (ce['ltp'] > engine.ce_ind.prev_close)

                    if ema_bullish and rg_divergence and poc_ce and (ce['ltp'] > poc_ce + 1.5):
                        await engine.execute_trade(
                            "CE",
                            engine.active_tokens['ce_token'],
                            engine.active_tokens['ce_sym'],
                            ce['ltp'],
                            idx['ltp'],
                            engine.ce_ind.atr14,
                            engine.idx_ind.atr14
                        )
                        trade_executed = True

                # Check PE (Puts) Entry (only if no CE trade was entered)
                if not trade_executed and not engine.trade_state['active']:
                    if pe and pe['ltp'] > engine.pe_ind.vwap and engine.pe_ind.prev_close:
                        poc_pe = engine.pe_ind.get_poc()
                        ema_bullish = (engine.pe_ind.ema9 is not None) and (engine.pe_ind.ema9 > engine.pe_ind.ema15)
                        gg_divergence = (idx['ltp'] > engine.idx_ind.prev_close) and (pe['ltp'] > engine.pe_ind.prev_close)

                        if ema_bullish and gg_divergence and poc_pe and (pe['ltp'] > poc_pe + 1.5):
                            await engine.execute_trade(
                                "PE",
                                engine.active_tokens['pe_token'],
                                engine.active_tokens['pe_sym'],
                                pe['ltp'],
                                idx['ltp'],
                                engine.pe_ind.atr14,
                                engine.idx_ind.atr14
                            )

        await asyncio.sleep(0.01)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\n[INFO] Bot stopped gracefully by user.")
