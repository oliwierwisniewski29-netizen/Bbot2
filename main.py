# === BBOT 3.3 - poprawiona wersja z indywidualnym MIN_NOTIONAL === 
import os, json, threading, time, asyncio, requests, sqlite3, math
from collections import defaultdict, deque
from queue import Queue
from decimal import Decimal
from websocket import WebSocketApp
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from binance.client import Client
from functools import wraps

# === KONFIGURACJA ===
CFG = {
    "BINANCE_API_KEY": "fygZMg3TIFqzjIOw7e0ZRNHRdP47wguZHugzATlohB7ZkdaX0gH4PRJvuh83PhqE",
    "BINANCE_API_SECRET": "LXZ0nTBkObPHQXm7UoZc9wPbfQekDyk6QdQVWKL8YTZYVdwANZDm9yOjyPY1E1oe",
    "TELEGRAM_BOT_TOKEN": "7971462955:AAHIqNKqJR38gr5ieC7_n5wafDD5bD-jRHE",
    "ALLOWED_CHAT_IDS": ["7684314138"],

    # parametry działania
    "WINDOW_SECONDS": 5,
    "PCT_THRESHOLD": 20.0,
    "BUY_ALLOCATION_PERCENT": 1.0,
    "CONVERT_FROM_USDC_PERCENT": 0.50,
    "TP_PERCENT": 7.0,
    "MAX_CONCURRENT_TRADES": 5,
    "PAPER_TRADING": False,
    "USERDATA_STREAM": True,
    "TRADE_COOLDOWN_SECONDS": 10,
    "API_RETRY_ATTEMPTS": 3,
    "API_RETRY_BACKOFF": 1.0,

    # 🟢 INDYWIDUALNE MIN_NOTIONAL DLA KAŻDEJ WALUTY
    "MIN_NOTIONALS": {
        "USDC": 5.0,
        "USDT": 5.0,
        "BNB": 0.01,
        "BTC": 0.0001,
        "TRY": 10.0,
        "ETH": 0.001,
        "EUR": 5.0,
        "XRP": 10.0,
        "DOGE": 30.0,
        "TRX": 100.0,
        "BRL": 10.0,
        "JPY": 100.0,
        "PLN": 25.0
    },
    "MIN_NOTIONAL_DEFAULT": 5.0
}


# === POMOCNICZE ===
def now_ts(): return int(time.time())

def safe_float(x):
    try: return float(x)
    except: return 0.0

def send_telegram(text):
    """Wyślij wiadomość do Telegrama"""
    for chat in CFG["ALLOWED_CHAT_IDS"]:
        try:
            requests.post(
                f"https://api.telegram.org/bot{CFG['TELEGRAM_BOT_TOKEN']}/sendMessage",
                data={"chat_id": chat, "text": text, "parse_mode": "HTML"},
                timeout=5
            )
        except Exception as e:
            print("Telegram error:", e)

def floor_to_step(qty, step):
    """Zaokrągla ilość w dół do wielokrotności step"""
    try:
        if step <= 0:
            return qty
        mult = math.floor(qty / step)
        return round(mult * step, 8)
    except Exception:
        return qty

# retry decorator
def retry_api(attempts=3, backoff=1.0, allowed_exceptions=(Exception,)):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for i in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    last_exc = e
                    wait = backoff * (2 ** (i - 1))
                    print(f"[retry] API error on {func.__name__}: {e} — retry {i}/{attempts} after {wait:.1f}s")
                    time.sleep(wait)
            raise last_exc
        return wrapper
    return deco


# === BAZA ===
class DB:
    def __init__(self, path="data.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init()

    def _init(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS positions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, qty REAL, avg_price REAL, status TEXT,
                opened_at INTEGER, closed_at INTEGER, pnl REAL
            )""")
            self.conn.commit()

    def insert_pos(self, sym, qty, avg):
        with self.lock:
            c = self.conn.cursor()
            c.execute("INSERT INTO positions(symbol,qty,avg_price,status,opened_at) VALUES(?,?,?,?,?)",
                      (sym, qty, avg, "OPEN", now_ts()))
            self.conn.commit()
            return c.lastrowid

    def close_pos(self, sym, pnl):
        with self.lock:
            c = self.conn.cursor()
            c.execute("UPDATE positions SET status=?, closed_at=?, pnl=? WHERE symbol=? AND status='OPEN'",
                      ("CLOSED", now_ts(), pnl, sym))
            self.conn.commit()

    def has_open_position(self, sym):
        with self.lock:
            c = self.conn.cursor()
            c.execute("SELECT 1 FROM positions WHERE symbol=? AND status='OPEN' LIMIT 1", (sym,))
            return c.fetchone() is not None


# === EGZEKUTOR TRANSAKCJI ===
class Executor:
    def __init__(self, db: DB):
        self.db = db
        self.q = Queue()
        self.paper = CFG["PAPER_TRADING"]
        if not self.paper:
            self.client = Client(CFG["BINANCE_API_KEY"], CFG["BINANCE_API_SECRET"])
        else:
            self.client = None
        self.symbol_filters = {}
        self._load_symbol_filters()
        self.last_trade_ts = {}
        self.active_symbols = set()

    @retry_api(attempts=CFG["API_RETRY_ATTEMPTS"], backoff=CFG["API_RETRY_BACKOFF"])
    def _api_get_exchange_info(self):
        return self.client.get_exchange_info()

    def _load_symbol_filters(self):
        """Pobiera filtry z Binance"""
        if self.client is None:
            return
        try:
            info = self._api_get_exchange_info()
            for s in info.get("symbols", []):
                symbol = s["symbol"]
                min_notional = CFG["MIN_NOTIONAL_DEFAULT"]
                step_size = 0.000001
                for f in s.get("filters", []):
                    if f.get("filterType") == "MIN_NOTIONAL":
                        min_notional = safe_float(f.get("minNotional", min_notional))
                    if f.get("filterType") == "LOT_SIZE":
                        step_size = safe_float(f.get("stepSize", step_size))
                self.symbol_filters[symbol] = {"min_notional": min_notional, "step_size": step_size}
            print(f"✅ Załadowano {len(self.symbol_filters)} filtrów symboli z Binance")
        except Exception as e:
            print("Błąd pobierania filtrów:", e)

    def _get_balance(self, asset):
        if self.paper:
            if asset == "USDC":
                return 100.0
            return 0.0
        try:
            bal = self.client.get_asset_balance(asset)
            return safe_float(bal.get("free", 0))
        except Exception as e:
            print("Balance err:", e)
            return 0.0

    def enqueue(self, sig): self.q.put(sig)

    def _buy(self, symbol, price):
        """Kup po cenie rynkowej z indywidualnym min_notional."""
        if self.db.has_open_position(symbol) or symbol in self.active_symbols:
            return

        last = self.last_trade_ts.get(symbol, 0)
        if time.time() - last < CFG["TRADE_COOLDOWN_SECONDS"]:
            return

        quote = None
        for q in ["USDC", "USDT", "BTC", "BNB", "TRY", "ETH"]:
            if symbol.endswith(q):
                quote = q
                break
        if not quote:
            print("❓ Nie rozpoznano quote:", symbol)
            return

        info = self.symbol_filters.get(symbol, {})
        step = info.get("step_size", 0.000001)

        # 🟢 wybór indywidualnego min_notional
        min_notional = CFG["MIN_NOTIONALS"].get(
            quote,
            info.get("min_notional", CFG["MIN_NOTIONAL_DEFAULT"])
        )
        send_telegram(f"💰 Użyto min_notional={min_notional} dla {quote}")

        balance = self._get_balance(quote)
        invest = balance * CFG["BUY_ALLOCATION_PERCENT"]

        if invest < min_notional:
            print(f"Pominięto {symbol}: {invest:.6f} < {min_notional}")
            return

        self.active_symbols.add(symbol)

        if self.paper:
            qty = floor_to_step(invest / price, step)
            self.db.insert_pos(symbol, qty, price)
            send_telegram(f"[PAPER] KUPNO {symbol}: {qty:.8f} @ {price:.8f} (invest={invest:.4f} {quote})")
            self.last_trade_ts[symbol] = time.time()
            self.active_symbols.discard(symbol)
            return

        # Live trading
        try:
            order = self.client.order_market_buy(symbol=symbol, quoteOrderQty=str(round(invest, 6)))
            executed_qty = safe_float(order.get('executedQty'))
            avg_price = price
            if order.get('fills'):
                avg_price = safe_float(order['fills'][0].get('price', price))
            self.db.insert_pos(symbol, executed_qty, avg_price)
            send_telegram(f"🟢 KUPNO {symbol}: {executed_qty:.8f} @ {avg_price:.8f} (invest={invest:.4f})")
        except Exception as e:
            send_telegram(f"❌ Błąd kupna {symbol}: {e}")
        finally:
            self.active_symbols.discard(symbol)


    def worker(self):
        while True:
            sig = self.q.get()
            try:
                self._buy(sig["symbol"], sig["price"])
            except Exception as e:
                print("Executor worker error:", e)


# === STRATEGIA ===
class Strategy:
    def __init__(self, executor: Executor):
        self.executor = executor
        self.price_hist = defaultdict(lambda: deque(maxlen=100))

    def on_tick(self, entry, ts):
        s = entry.get("s")
        p = safe_float(entry.get("c"))
        if p <= 0 or s.startswith("USDC"):
            return
        dq = self.price_hist[s]
        dq.append((ts, p))
        old = next((pp for tt, pp in dq if tt <= ts - CFG["WINDOW_SECONDS"]), None)
        if old:
            pct = (p - old) / old * 100
            if pct <= -abs(CFG["PCT_THRESHOLD"]):
                print(f"💥 Spadek {s} {pct:.2f}% → kupuję")
                self.executor.enqueue({"symbol": s, "price": p})


# === TELEGRAM ===
class TelegramBot:
    def __init__(self, db: DB, executor: Executor):
        self.db = db
        self.executor = executor
        self.app = ApplicationBuilder().token(CFG["TELEGRAM_BOT_TOKEN"]).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("status", self.status))

    async def start(self, update, ctx):
        await update.message.reply_text("🤖 Bot działa!")

    async def status(self, update, ctx):
        openpos = self.db.conn.execute("SELECT symbol,qty,avg_price FROM positions WHERE status='OPEN'").fetchall()
        msg = f"📊 Otwarte pozycje: {len(openpos)}\n"
        for s in openpos:
            msg += f"{s[0]} {s[1]} @ {s[2]}\n"
        await update.message.reply_text(msg)

    def run(self):
        self.app.run_polling()


# === WEBSOCKET ===
class WS:
    def __init__(self, strat: Strategy):
        self.strat = strat

    def on_msg(self, ws, msg):
        try:
            data = json.loads(msg)
            ts = time.time()
            for e in data:
                self.strat.on_tick(e, ts)
        except Exception as e:
            print("ws parse err", e)

    def run(self):
        while True:
            try:
                ws = WebSocketApp("wss://stream.binance.com:9443/ws/!miniTicker@arr", on_message=self.on_msg)
                ws.run_forever()
            except Exception as e:
                print("ws error", e)
            time.sleep(3)


# === MAIN ===
if __name__ == "__main__":
    print("🚀 Start BBOT 2.1 (indywidualne min_notional)")
    db = DB()
    exe = Executor(db)
    strat = Strategy(exe)
    ws = WS(strat)
    tg = TelegramBot(db, exe)

    threading.Thread(target=ws.run, daemon=True).start()
    threading.Thread(target=exe.worker, daemon=True).start()
    tg.run()
