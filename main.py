# === BBOT 3.0 - Auto USDC→target + smart buy protection ===
import os, json, threading, time, asyncio, requests, sqlite3, math
from collections import defaultdict, deque
from queue import Queue
from decimal import Decimal
from websocket import WebSocketApp
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import Update
from binance.client import Client
from functools import wraps

# === KONFIGURACJA ===
CFG = {
    "BINANCE_API_KEY": "fygZMg3TIFqzjIOw7e0ZRNHRdP47wguZHugzATlohB7ZkdaX0gH4PRJvuh83PhqE",
    "BINANCE_API_SECRET": "LXZ0nTBkObPHQXm7UoZc9wPbfQekDyk6QdQVWKL8YTZYVdwANZDm9yOjyPY1E1oe",
    "TELEGRAM_BOT_TOKEN": "7971462955:AAHIqNKqJR38gr5ieC7_n5wafDD5bD-jRHE",
    "ALLOWED_CHAT_IDS": ["7684314138"],

    "WINDOW_SECONDS": 5,
    "PCT_THRESHOLD": 20.0,
    "BUY_ALLOCATION_PERCENT": 1.0,        # ile % salda quote użyć na zakup
    "CONVERT_FROM_USDC_PERCENT": 0.50,    # ile % salda USDC przekonwertować
    "TP_PERCENT": 7.0,
    "MAX_CONCURRENT_TRADES": 5,
    "PAPER_TRADING": False,
    "USERDATA_STREAM": True,
    "TRADE_COOLDOWN_SECONDS": 10,
    "API_RETRY_ATTEMPTS": 3,
    "API_RETRY_BACKOFF": 1.0,

    # minimalne notional dla każdej waluty
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
    "MIN_NOTIONAL_DEFAULT": 5.0,
    "MAX_VOLATILITY_PERCENT": 5.0,  # maksymalne dopuszczalne wahania ceny wstecz (%)
    "VOLATILITY_LOOKBACK": 60       # liczba sekund, z których analizowana jest zmienność
}

# === POMOCNICZE ===
def now_ts(): return int(time.time())
def safe_float(x):
    try: return float(x)
    except: return 0.0

def send_telegram(text):
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
    try:
        if step <= 0: return qty
        mult = math.floor(qty / step)
        return round(mult * step, 8)
    except: return qty

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
                    print(f"[retry] {func.__name__} error: {e} → retry {i}/{attempts} in {wait:.1f}s")
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
            c = self.conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS positions(
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

    def has_open_position(self, sym):
        with self.lock:
            c = self.conn.cursor()
            c.execute("SELECT 1 FROM positions WHERE symbol=? AND status='OPEN' LIMIT 1", (sym,))
            return c.fetchone() is not None

    def close_position(self, sym):
        with self.lock:
            c = self.conn.cursor()
            c.execute("UPDATE positions SET status='CLOSED', closed_at=? WHERE symbol=? AND status='OPEN'",
                      (now_ts(), sym))
            self.conn.commit()

# === EGZEKUTOR ===
class Executor:
    def __init__(self, db: DB):
        self.db = db
        self.q = Queue()
        self.paper = CFG["PAPER_TRADING"]
        self.client = None if self.paper else Client(CFG["BINANCE_API_KEY"], CFG["BINANCE_API_SECRET"])
        self.symbol_filters = {}
        self._load_symbol_filters()
        self.last_trade_ts = {}
        self.active_symbols = set()

    # === API ===
    @retry_api()
    def _api_get_exchange_info(self):
        return self.client.get_exchange_info()

    # === FILTRY SYMBOLI ===
    def _load_symbol_filters(self):
        try:
            if not self.client:
                return
            info = self._api_get_exchange_info()
            for s in info["symbols"]:
                symbol = s["symbol"]
                min_notional = CFG["MIN_NOTIONAL_DEFAULT"]
                step_size = 0.000001
                for f in s.get("filters", []):
                    if f["filterType"] == "MIN_NOTIONAL":
                        min_notional = safe_float(f.get("minNotional", min_notional))
                    if f["filterType"] == "LOT_SIZE":
                        step_size = safe_float(f.get("stepSize", step_size))
                self.symbol_filters[symbol] = {
                    "min_notional": min_notional,
                    "step_size": step_size
                }
            print(f"✅ Załadowano {len(self.symbol_filters)} filtrów")
        except Exception as e:
            print("Błąd filtrów:", e)

    # === BALANSE ===
    def _get_balance(self, asset):
        if self.paper:
            return 100.0 if asset == "USDC" else 0.0
        try:
            bal = self.client.get_asset_balance(asset)
            return safe_float(bal.get("free", 0))
        except Exception as e:
            print("Balance error:", e)
            return 0.0

    # === ENQUEUE (NAPRAWA BŁĘDU) ===
    def enqueue(self, sig):
        """Dodaje sygnał do kolejki (np. z Strategy.on_tick)"""
        self.q.put(sig)

    # === KONWERSJA USDC → TARGET ===
    def convert_usdc_to_target(self, target, convert_percent):
        """Skonwertuj część USDC → target. convert_percent (0..1) procent z całego USDC balansu.
        Zwraca: ilość target (base), która powstała oraz ile USDC użyto, albo (0,0) przy błędzie."""
        usdc_bal = self._get_balance("USDC")
        if usdc_bal <= 0:
            return 0.0, 0.0

        amount_usdc = usdc_bal * float(convert_percent)
        if amount_usdc <= 0:
            return 0.0, 0.0

        pair = f"{target}USDC"  # np. TRYUSDC -> kupujemy TRY za USDC
        attempts = CFG["API_RETRY_ATTEMPTS"]
        backoff = CFG["API_RETRY_BACKOFF"]
        last_exc = None

        for i in range(1, attempts + 1):
            try:
                order = self.client.order_market_buy(
                    symbol=pair,
                    quoteOrderQty=str(round(amount_usdc, 6))
                )

                executed_qty = safe_float(
                    order.get('executedQty') or
                    sum(safe_float(f.get('qty', 0)) for f in order.get('fills', []))
                )

                send_telegram(
                    f"💱 Skonwertowano {amount_usdc:.6f} USDC → {executed_qty:.8f} {target} (para {pair})"
                )
                return executed_qty, amount_usdc

            except Exception as e:
                last_exc = e
                wait = backoff * (2 ** (i - 1))
                print(f"[convert retry] error converting {pair}: {e} — retry {i}/{attempts} after {wait:.1f}s")
                time.sleep(wait)

        print("Conversion error (final):", last_exc)
        return 0.0, 0.0

    # === NOWA FUNKCJA KONWERSJI ===
    def convert_from_usdc(self, target: str, convert_percent: float):
        """
        Skonwertuj część salda USDC -> target.
        Zwraca (ilość_target, ilość_usdc_użyta) lub (0.0, 0.0) przy błędzie.
        """
        try:
            usdc_bal = self._get_balance("USDC")
            if usdc_bal <= 0:
                send_telegram("❌ Brak środków USDC do konwersji.")
                return 0.0, 0.0

            amount_usdc = usdc_bal * float(convert_percent)
            if amount_usdc <= 0:
                send_telegram("⚠️ Nieprawidłowy procent konwersji (0%).")
                return 0.0, 0.0

            min_notional = CFG["MIN_NOTIONALS"].get("USDC", 5.0)
            if amount_usdc < min_notional:
                send_telegram(f"⚠️ Kwota {amount_usdc:.2f} USDC < minimalna {min_notional} USDC.")
                return 0.0, 0.0

            pair = f"{target}USDC"
            send_telegram(f"🔄 Konwertuję {amount_usdc:.2f} USDC → {target} (para {pair})...")

            attempts = CFG.get("API_RETRY_ATTEMPTS", 3)
            backoff = CFG.get("API_RETRY_BACKOFF", 2)
            last_exc = None

            for i in range(1, attempts + 1):
                try:
                    if not self.paper:
                        order = self.client.order_market_buy(symbol=pair, quoteOrderQty=str(round(amount_usdc, 6)))
                        executed_qty = safe_float(order.get('executedQty')) or sum(
                            safe_float(f.get('qty', 0)) for f in order.get('fills', [])
                        )
                        send_telegram(f"✅ Skonwertowano {amount_usdc:.2f} USDC → {executed_qty:.8f} {target}")
                        return executed_qty, amount_usdc
                    else:
                        send_telegram(f"[PAPER] Symulacja konwersji {amount_usdc:.2f} USDC → {target}")
                        return amount_usdc / 100, amount_usdc
                except Exception as e:
                    last_exc = e
                    wait = backoff * (2 ** (i - 1))
                    print(f"[convert retry] {pair} error: {e} — retry {i}/{attempts} after {wait:.1f}s")
                    time.sleep(wait)

            send_telegram(f"❌ Błąd konwersji {pair}: {last_exc}")
            return 0.0, 0.0

        except Exception as e:
            send_telegram(f"❌ Wyjątek konwersji USDC→{target}: {e}")
            return 0.0, 0.0

    # === SPRZEDAŻ I KUPNO ===
    def sell_all_position(self, symbol):
        try:
            if not self.db.has_open_position(symbol):
                send_telegram(f"⚠️ Brak otwartej pozycji {symbol}.")
                return

            quote = next((q for q in CFG["MIN_NOTIONALS"].keys() if symbol.endswith(q)), None)
            if not quote:
                send_telegram(f"❌ Nie rozpoznano quote dla {symbol}")
                return

            base = symbol[:-len(quote)]
            qty = self._get_balance(base)
            if qty <= 0:
                send_telegram(f"❌ Brak {base} do sprzedaży.")
                self.db.close_position(symbol)
                return

            send_telegram(f"🔴 Sprzedaję {qty:.8f} {base} ({symbol})...")
            if self.paper:
                send_telegram(f"[PAPER] Sprzedano {qty:.8f} {base}")
                self.db.close_position(symbol)
                return

            info = self.symbol_filters.get(symbol, {})
            step = info.get("step_size", 0.000001)
            qty_to_sell = floor_to_step(qty, step)
            if qty_to_sell <= 0:
                send_telegram("⚠️ Ilość po zaokrągleniu = 0, pomijam.")
                return

            order = self.client.order_market_sell(symbol=symbol, quantity=str(qty_to_sell))
            avg_price = safe_float(order["fills"][0]["price"]) if order.get("fills") else 0.0
            send_telegram(f"✅ Sprzedano {qty_to_sell:.8f} {base} @ {avg_price}")
            self.db.close_position(symbol)
        except Exception as e:
            send_telegram(f"❌ Błąd sprzedaży {symbol}: {e}")

    def _buy(self, symbol, price):
        if self.db.has_open_position(symbol) or symbol in self.active_symbols:
            return
        if time.time() - self.last_trade_ts.get(symbol, 0) < CFG["TRADE_COOLDOWN_SECONDS"]:
            return

        quote = next((q for q in CFG["MIN_NOTIONALS"].keys() if symbol.endswith(q)), None)
        if not quote:
            print(f"❓ Nie rozpoznano quote: {symbol}")
            return

        info = self.symbol_filters.get(symbol, {})
        step = info.get("step_size", 0.000001)
        min_notional = CFG["MIN_NOTIONALS"].get(quote, CFG["MIN_NOTIONAL_DEFAULT"])
        balance = self._get_balance(quote)

        if balance < min_notional and quote != "USDC":
            send_telegram(f"⚠️ Mało {quote}, konwertuję z USDC...")
            self.convert_from_usdc(quote, CFG["CONVERT_FROM_USDC_PERCENT"])
            balance = self._get_balance(quote)

        invest = balance * CFG["BUY_ALLOCATION_PERCENT"]
        if invest < min_notional:
            return

        self.active_symbols.add(symbol)
        try:
            if self.paper:
                qty = floor_to_step(invest / price, step)
                self.db.insert_pos(symbol, qty, price)
                send_telegram(f"[PAPER] KUPNO {symbol}: {qty:.8f} @ {price:.4f}")
            else:
                order = self.client.order_market_buy(symbol=symbol, quoteOrderQty=str(round(invest, 6)))
                qty = safe_float(order.get("executedQty"))
                avg = safe_float(order["fills"][0]["price"]) if order.get("fills") else price
                self.db.insert_pos(symbol, qty, avg)
                send_telegram(f"🟢 KUPNO {symbol}: {qty:.8f} @ {avg:.4f}")
        except Exception as e:
            send_telegram(f"❌ Błąd kupna {symbol}: {e}")
        finally:
            self.last_trade_ts[symbol] = time.time()
            self.active_symbols.discard(symbol)

    def worker(self):
        while True:
            sig = self.q.get()
            try:
                self._buy(sig["symbol"], sig["price"])
            except Exception as e:
                print("Worker error:", e)
                self.active_symbols.discard(sig.get("symbol"))

# === STRATEGIA ===
class Strategy:
    def __init__(self, executor):
        self.executor = executor
        self.price_hist = defaultdict(lambda: deque(maxlen=200))  # większy bufor

    def on_tick(self, entry, ts):
        s = entry.get("s")
        p = safe_float(entry.get("c"))
        if not s or p <= 0 or s.startswith("USDC"):
            return

        dq = self.price_hist[s]
        dq.append((ts, p))

        # usuń stare dane spoza okna analizy
        while dq and dq[0][0] < ts - CFG["VOLATILITY_LOOKBACK"]:
            dq.popleft()

        # sprawdź zmienność rynku w ostatnim okresie
        prices = [pp for _, pp in dq]
        if len(prices) < 5:
            return  # za mało danych do oceny stabilności

        max_p, min_p = max(prices), min(prices)
        volatility = ((max_p - min_p) / min_p) * 100 if min_p > 0 else 0

        if volatility < CFG["MIN_VOLATILITY_PERCENT"]:
            # rynek zbyt spokojny — pomijamy sygnał
            print(f"😴 Pomijam {s}: zmienność {volatility:.1f}% < {CFG['MIN_VOLATILITY_PERCENT']}%")
            return

        # sprawdź spadek ceny
        old = next((pp for tt, pp in dq if tt <= ts - CFG["WINDOW_SECONDS"]), None)
        if old:
            pct = (p - old) / old * 100
            if pct <= -abs(CFG["PCT_THRESHOLD"]):
                print(f"💥 Spadek {s}: {pct:.2f}% (zmienność {volatility:.1f}%) → kupuję")
                self.executor.enqueue({"symbol": s, "price": p})

# === TELEGRAM ===
class TelegramBot:
    def __init__(self, db, executor):
        self.db = db
        self.executor = executor
        self.app = ApplicationBuilder().token(CFG["TELEGRAM_BOT_TOKEN"]).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("status", self.status))
        self.app.add_handler(CommandHandler("sellall", self.sellall))

    async def start(self, u: Update, c: ContextTypes.DEFAULT_TYPE):
        await u.message.reply_text("🤖 Bot działa!")

    async def status(self, u: Update, c: ContextTypes.DEFAULT_TYPE):
        rows = self.db.conn.execute("SELECT symbol,qty,avg_price FROM positions WHERE status='OPEN'").fetchall()
        msg = f"📊 Otwarte pozycje: {len(rows)}\n" + "\n".join(f"{r[0]} {r[1]} @ {r[2]}" for r in rows)
        await u.message.reply_text(msg or "Brak otwartych pozycji.")

    async def sellall(self, u: Update, c: ContextTypes.DEFAULT_TYPE):
        args = u.message.text.split()
        if len(args) < 2:
            await u.message.reply_text("⚠️ Użycie: /sellall SYMBOL (np. /sellall MMTBNB)")
            return
        symbol = args[1].upper()
        threading.Thread(target=self.executor.sell_all_position, args=(symbol,), daemon=True).start()
        await u.message.reply_text(f"🚀 Rozpoczynam sprzedaż {symbol}...")

    def run(self):
        self.app.run_polling()

# === WEBSOCKET ===
class WS:
    def __init__(self, strat):
        self.strat = strat

    def on_msg(self, ws, msg):
        try:
            data = json.loads(msg)
            ts = time.time()
            for e in data:
                self.strat.on_tick(e, ts)
        except Exception as e:
            print("ws err:", e)

    def run(self):
        while True:
            try:
                ws = WebSocketApp("wss://stream.binance.com:9443/ws/!miniTicker@arr", on_message=self.on_msg)
                ws.run_forever()
            except Exception as e:
                print("ws error:", e)
                time.sleep(3)

# === MAIN ===
if __name__ == "__main__":
    print("🚀 Start BBOT 3.0")
    db = DB()
    exe = Executor(db)
    strat = Strategy(exe)
    ws = WS(strat)
    tg = TelegramBot(db, exe)
    threading.Thread(target=ws.run, daemon=True).start()
    threading.Thread(target=exe.worker, daemon=True).start()
    tg.run()
