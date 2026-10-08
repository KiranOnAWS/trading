# NIFTY Options Algorithmic Scalping Bot

An asynchronous Python algorithmic trading bot built for **NSE NIFTY 50 Index Options** using the **Angel One SmartAPI** and **Telegram Bot API**.

---

## Features

- **Automated Authentication:** Pre-market login with TOTP generation (`pyotp`) and SmartConnect session generation.
- **Dynamic Strike Tracking:**
  - Standard session: Automatically scans strikes within $\pm 400$ points of ATM and selects contracts nearest to **~₹100 LTP**.
  - Expiry regime (after 12:30 PM on expiry day): Switches to ATM contracts.
  - Automatically shifts active strikes if the index moves $\ge 200$ points.
- **$O(1)$ Streaming Indicators:**
  - Real-time VWAP calculation.
  - Streaming 1-minute candle synthesis.
  - Real-time online EMA(9) and EMA(15).
  - Streaming ATR(14) calculation.
  - Volume Profile Point of Control (POC) tracking.
- **Multi-Tier Risk & Execution Engine:**
  - Simulated slippage (₹0.50) and friction modeling (STT, exchange turnover fees).
  - Index structural stop loss (`idx_sl`).
  - Option capital hard stop / trailing stop (`-1.5 ATR`).
  - Parabolic target exit (`+3.5 ATR`).
  - Partial profit booking (`+2.0 ATR`: books 3 lots and tightens stop to `+1.2 ATR`).
  - Breakeven cost-trailing (`+1.0 ATR`).
  - EOD square-off at 15:26 IST.
- **Circuit Breakers:**
  - Halts new trades if daily loss $\le -₹6,000$ or if max daily trades $\ge 6$.
- **Resilience:**
  - Atomic JSON state persistence (`trade_state.json`) with automatic trade recovery upon crash/reboot.
  - Smart socket watchdog to detect and recover dropped WebSocket connections.
- **Asynchronous Telegram Alerts:**
  - Urgent queue for trade entries, exits, and socket alerts.
  - Batched 3-second digest for lower-priority system updates.

---

## Repository Structure

```
├── main_trading_bot.py      # Core trading engine & lifecycle loop
├── MainTradingBot.pdf       # Original specification / source document
├── requirements.txt         # Python package dependencies
├── .env.example             # Configuration template
├── .gitignore               # Ignored cache, state, and credential files
└── README.md                # Project documentation
```

---

## Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/KiranOnAWS/trading.git
cd trading
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Fill in your credentials:
```ini
ANGEL_API_KEY=your_angel_api_key
CLIENT_ID=your_client_id
PASSWORD=your_password
TOTP_SECRET_KEY=your_totp_secret_key
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

### 4. Run the Bot
```bash
python main_trading_bot.py
```

> **Note:** By default, `LIVE_EXECUTION = False` (Paper Trading Mode). Set `LIVE_EXECUTION = True` in `main_trading_bot.py` only after thorough backtesting and verification.
