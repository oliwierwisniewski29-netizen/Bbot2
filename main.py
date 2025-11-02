# === BBOT 3.2 - poprawiona wersja (pełny plik) ===
import os, json, threading, time, asyncio, requests, sqlite3, math
from collections import defaultdict, deque
from queue import Queue
from decimal import Decimal
from websocket import WebSocketApp
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from binance.client import Client

# === KONFIGURACJA ===
CFG = {
    "BINANCE_API_KEY": "fygZMg3TIFqzjIOw7e0ZRNHRdP47wguZHugzATlohB7ZkdaX0gH4PRJvuh83PhqE",
    "BINANCE_API_SECRET": "LXZ0nTBkObPHQXm7UoZc9wPbfQekDyk6QdQVWKL8YTZYVdwANZDm9yOjyPY1E1oe",
    "TELEGRAM_BOT_TOKEN": "7971462955:AAHIqNKqJR38gr5ieC7_n5wafDD5bD-jRHE",
    "ALLOWED_CHAT_IDS": ["7684314138"],
    "WINDOW_SECONDS": 5,               # okno czasowe do analizy (s)
    "PCT_THRESHOLD": 20.0,             # spadek procentowy do triggera
    "BUY_ALLOCATION_PERCENT": 1.0,    # jeśli mamy quote, kupujemy tę część (5%): zwykła ścieżka
    "CONVERT_FROM_USDC_PERCENT": 0.50, # jeśli trzeba konwertować USDC -> quote, ile % USDC konwertować (np. 0.1 = 10%)
    "TP_PERCENT": 10.0,                 # take profit %
    "MAX_CONCURRENT_TRADES": 5,
    "PAPER_TRADING": False,
    "USERDATA_STREAM": True,
    "MIN_NOTIONAL_DEFAULT": 5.0,       # wartość minimalna jeśli brak info z API
    "TRADE_COOLDOWN_SECONDS": 90       # cooldown na symbol po wykonanym trade
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
    """Zwraca qty zredukowane (floor) do wielokrotności step."""
    try:
        if step <= 0:
            return qty
        # dla step np 0.00010000 - zachowujemy precyzję
        return math.floor(qty / step) * step
    except Exception:
        return qty

# === BAZA DANYCH ===
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

# === EGZEKUTOR TRANSAKCJI ===
class Executor:
    def __init__(self, db: DB):
        self.db = db
        self.q = Queue()
        self.paper = CFG["PAPER_TRADING"]
        # init client tylko gdy live trading
        if not self.paper:
            self.client = Client(CFG["BINANCE_API_KEY"], CFG["BINANCE_API_SECRET"])
        else:
            self.client = None
        # filtry symboli (minNotional, stepSize)
        self.symbol_filters = {}
        self._load_symbol_filters()
        # cooldowny po trade na symbol -> timestamp
        self.last_trade_ts = {}

    def _load_symbol_filters(self):
        """Pobiera filtry z Binance (minNotional i stepSize). Jeśli nie da się pobrać,
           będzie używać wartości domyślnych."""
        if self.client is None:
            return
        try:
            info = self.client.get_exchange_info()
            for s in info.get("symbols", []):
                symbol = s["symbol"]
                min_notional = CFG["MIN_NOTIONAL_DEFAULT"]
                step_size = 0.000001
                for f in s.get("filters", []):
                    if f.get("filterType") == "MIN_NOTIONAL":
                        # różne wersje API używają minNotional lub minNotional['minNotional']
                        min_notional = safe_float(f.get("minNotional") or f.get("minNotional", 0) or f.get("minNotional", 0.0))
                    if f.get("filterType") == "LOT_SIZE":
                        step_size = safe_float(f.get("stepSize", 0.000001))
                self.symbol_filters[symbol] = {"min_notional": min_notional, "step_size": step_size}
            print(f"✅ Załadowano {len(self.symbol_filters)} filtrów symboli z Binance")
        except Exception as e:
            print("Błąd pobierania filtrów:", e)

    def enqueue(self, sig): self.q.put(sig)

    def _get_balance(self, asset):
        if self.paper:
            # w trybie paper można rozbudować symulowane saldo (tu przykładowo)
            # można zamiast tego czytać z pliku JSON
            # dla testów domyślnie 100 USDC:
            if asset == "USDC":
                return 100.0
            return 0.0
        try:
            bal = self.client.get_asset_balance(asset)
            return safe_float(bal.get("free", 0))
        except Exception as e:
            print("Get balance err:", e)
            return 0.0

    def _convert_usdc(self, target, convert_percent):
        """Skonwertuj część USDC -> target. convert_percent (0..1) procent z całego USDC balansu.
           Zwraca: ilość target (base) która powstała albo 0 przy błędzie."""
        usdc_bal = self._get_balance("USDC")
        if usdc_bal <= 0:
            return 0.0, 0.0
        amount_usdc = usdc_bal * float(convert_percent)
        if amount_usdc <= 0:
            return 0.0, 0.0
        pair = f"{target}USDC"  # np. TRYUSDC -> kupujemy TRY za USDC
        try:
            # wykonujemy market buy używając quoteOrderQty = amount_usdc (kupujemy target)
            order = self.client.order_market_buy(symbol=pair, quoteOrderQty=str(round(amount_usdc, 6)))
            # policz ile base dostaliśmy
            executed_qty = 0.0
            if 'executedQty' in order:
                executed_qty = safe_float(order['executedQty'])
            else:
                # alternatywnie sumuj fills
                executed_qty = sum(safe_float(f.get('qty', 0)) for f in order.get('fills', []))
            send_telegram(f"💱 Skonwertowano {amount_usdc:.6f} USDC → {executed_qty:.8f} {target} (para {pair})")
            return executed_qty, amount_usdc
        except Exception as e:
            print("Conversion error:", e)
            return 0.0, 0.0

    def _buy(self, symbol, price):
        """Kup po cenie rynkowej. Logika:
           - rozpoznaj quote
           - jeśli brak quote balance:
               * jeśli quote != USDC -> skonwertuj CONVERT_FROM_USDC_PERCENT USDC na quote i użyj CAŁEJ przekonwertowanej kwoty do kupna
               * jeśli quote == USDC -> jeśli brak USDC zakończ
           - jeśli mamy quote balance -> invest = balance * BUY_ALLOCATION_PERCENT (standard)
           - sprawdź minNotional i stepSize -> jeśli inwest < minNotional: pomiń
           - złóż zamówienie (quoteOrderQty = invest), dodaj zapis do DB, wyślij telegram
        """
        # ochronny cooldown: zapobiega natychmiastowym powtórkom (spam)
        last = self.last_trade_ts.get(symbol, 0)
        if time.time() - last < CFG["TRADE_COOLDOWN_SECONDS"]:
            print(f"Cooldown for {symbol}, skip (last trade {time.time()-last:.1f}s ago)")
            return

        if symbol.startswith("USDC"):
            # nie kupujemy par zaczynających się od USDC/...
            return

        # znajdź quote (najczęściej 3-4 znaki)
        quote = None
        for q in ["USDC", "USDT", "BUSD", "BTC", "BNB", "ETH"]:
            if symbol.endswith(q):
                quote = q
                break
        if not quote:
            print("Nie rozpoznano quote dla", symbol)
            return

        # filtry symbolu
        info = self.symbol_filters.get(symbol, {})
        step = info.get("step_size", 0.000001)
        min_notional = info.get("min_notional", CFG["MIN_NOTIONAL_DEFAULT"])

        # sprawdź obecny stan balansu w quote
        balance = self._get_balance(quote)
        invest = 0.0
        used_converted = False

        if balance <= 0:
            # brak salda w quote -> spróbuj konwersji z USDC (jeśli quote != USDC)
            if quote != "USDC" and self.client is not None:
                print(f"Brak {quote}, próbuję skonwertować USDC ({CFG['CONVERT_FROM_USDC_PERCENT']*100:.1f}%)")
                base_acquired, usdc_used = self._convert_usdc(quote, CFG["CONVERT_FROM_USDC_PERCENT"])
                # po konwersji odczytaj stan quote balance ponownie
                balance = self._get_balance(quote)
                # zgodnie z życzeniem: jeśli konwertujemy, używamy CAŁYCH przekonwertowanych środków do kupna
                if balance > 0:
                    invest = balance
                    used_converted = True
                else:
                    print("Konwersja nie dostarczyła wystarczających środków, pomijam.")
                    return
            else:
                # quote == USDC i brak USDC
                print("Brak USDC/quote — nie mogę kupić", symbol)
                return
        else:
            # mamy balance w quote -> normalna ścieżka: kup tylko część (BUY_ALLOCATION_PERCENT)
            invest = balance * float(CFG["BUY_ALLOCATION_PERCENT"])

        # jeśli po obliczeniach inwest < min_notional -> pomiń
        if invest < min_notional:
            print(f"Pominięto {symbol}: inwestycja {invest:.6f} < minNotional {min_notional}")
            return

        # Użyj quoteOrderQty by uniknąć problemów z stepSize przy kupnie.
        try:
            # jeżeli paper trading - symulacja
            if self.paper:
                # prosty paper: oblicz qty = invest/price i zapisz
                qty = invest / price
                qty = floor_to_step(qty, step)
                pos_id = self.db.insert_pos(symbol, qty, price)
                self.db.conn.commit()
                send_telegram(f"[PAPER] KUPNO {symbol}: {qty:.8f} @ {price:.8f} (invest={invest:.6f})")
                self.last_trade_ts[symbol] = time.time()
                return

            # live: zlecenie market buy z wykorzystaniem quoteOrderQty
            order = self.client.order_market_buy(symbol=symbol, quoteOrderQty=str(round(invest, 6)))
            # wyciągnij wykonaną ilość (executedQty) i średnią cenę
            executed_qty = safe_float(order.get('executedQty') or sum(safe_float(f.get('qty', 0)) for f in order.get('fills', [])))
            avg_price = 0.0
            if order.get('fills'):
                # średnia ważona można pobrać inaczej, ale najprościej: użyj price z fills[0] przy market
                avg_price = safe_float(order['fills'][0].get('price', price))
            else:
                avg_price = price
            # zapis pozycji
            self.db.insert_pos(symbol, executed_qty, avg_price)
            send_telegram(f"🟢 KUPNO {symbol}: {executed_qty:.8f} @ {avg_price:.8f} (invest={invest:.6f})")
            self.last_trade_ts[symbol] = time.time()
        except Exception as e:
            # złap błąd i pokaż czy to NOTIONAL (binance)
            print("Buy error:", e)
            # nie wysyłamy powiadomienia dla każdego błędu żeby nie spamować; ale można
            return

    def _sell(self, symbol, qty):
        """Sprzedaż po cenie rynkowej (używane np. przez komendę /sellall)"""
        try:
            if self.paper:
                # w paper: zamknięcie pozycji (symulacja)
                self.db.close_pos(symbol, 0)
                send_telegram(f"[PAPER] SPRZEDAŻ {symbol}: {qty}")
                return
            # dostosuj qty do stepSize
            info = self.symbol_filters.get(symbol, {})
            step = info.get("step_size", 0.000001)
            qty_to_sell = floor_to_step(qty, step)
            order = self.client.order_market_sell(symbol=symbol, quantity=str(qty_to_sell))
            avg = 0.0
            if order.get('fills'):
                avg = safe_float(order['fills'][0].get('price'))
            self.db.close_pos(symbol, 0)
            send_telegram(f"🔴 SPRZEDAŻ {symbol}: {qty_to_sell} @ {avg}")
        except Exception as e:
            print("Sell error:", e)

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
        if p <= 0:
            return
        # pomiń jeśli symbol zaczyna się od USDC (USDC/xxx) — nie chcesz analizować takich par
        if s.startswith("USDC"):
            return
        dq = self.price_hist[s]
        dq.append((ts, p))
        # znajdź price sprzed WINDOW_SECONDS
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
        self.app.add_handler(CommandHandler("sellall", self.sell_all))
    async def start(self, update, ctx):
        await update.message.reply_text("🤖 Bot działa!")
    async def status(self, update, ctx):
        openpos = self.db.conn.execute("SELECT symbol,qty,avg_price FROM positions WHERE status='OPEN'").fetchall()
        msg = f"📊 Otwarte pozycje: {len(openpos)}\n"
        for s in openpos: msg += f"{s[0]} {s[1]} @ {s[2]}\n"
        await update.message.reply_text(msg)
    async def sell_all(self, update, ctx):
        try:
            args = ctx.args
            if not args:
                await update.message.reply_text("Użyj: /sellall BNB (lub inny symbol fragment jak BNB, BTC)")
                return
            asset = args[0].upper()
            pos = self.db.conn.execute("SELECT symbol,qty FROM positions WHERE symbol LIKE ? AND status='OPEN'", (f"%{asset}%",)).fetchone()
            if pos:
                self.executor._sell(pos[0], pos[1])
                await update.message.reply_text(f"🔴 Sprzedałem wszystkie {asset}.")
            else:
                await update.message.reply_text(f"❌ Brak otwartej pozycji {asset}.")
        except Exception as e:
            await update.message.reply_text(f"Błąd: {e}")
    def run(self):
        self.app.run_polling()

# === WEBSOCKET BINANCE ===
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
    print("🚀 Start BBOT 2.0 ...")
    db = DB()
    exe = Executor(db)
    strat = Strategy(exe)
    ws = WS(strat)
    tg = TelegramBot(db, exe)
    # thread: ws, executor worker
    threading.Thread(target=ws.run, daemon=True).start()
    threading.Thread(target=exe.worker, daemon=True).start()
    tg.run()

