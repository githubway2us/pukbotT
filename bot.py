import os
import threading
import time
import json
import math
from datetime import datetime, timedelta

import pandas as pd
import pandas_ta as ta
from binance.client import Client
from binance.enums import *
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv
from rich.align import Align
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

# ==============================================================================
# PRODUCTION PRODUCTION INFRASTRUCTURE PARAMETERS
# ==============================================================================
load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

TOP_N = 50
LEVERAGE = 10
FIXED_MARGIN_PER_TRADE = 5.0  # ทุนเข้าออเดอร์คงที่ 5 USDT
MAX_ACTIVE_TRADES = 6      
SCAN_INTERVAL = 15 
TRADE_COOLDOWN_MINUTES = 35   # ระยะเวลาห้ามเข้าซ้ำหลังจากปิดออเดอร์ (นาที)
VOL_MULTIPLIER = 1.2      

# Portfolio Exposure Risk Gate Boundaries
MAX_DAILY_DRAWDOWN_PCT = 0.05    
MAX_PORTFOLIO_LONG_EXPOSURE = 0.40  
MAX_PORTFOLIO_SHORT_EXPOSURE = 0.40 
MAX_SECTOR_EXPOSURE = 0.20       
ADX_TREND_THRESHOLD = 25       

STATE_FILE = "quantum_system_state.json"

class QuantumInstitutionalSystem:
    def __init__(self, mode="LIVE"):
        self.mode = mode 
        self.client = Client(API_KEY, API_SECRET) if mode == "LIVE" else None
        self.lock = threading.Lock()
        
        # State Arrays
        self.symbols = []
        self.active_positions = []
        self.ping = 0
        self.current_scanning = "INIT"
        self.system_status = "OPERATIONAL 🟢"
        
        # Core State Machine Registers
        self.cooldown_trades = {} 
        self.market_scores = {} 
        self.market_vol_status = {} 
        self.market_vol_ratio = {}   
        self.price_cache = {}
        self.deep_analysis_report = "Analysing Macro Market..."
        
        # 🔥 HIGH SECURITY REGISTRY: บัญชีดำล็อกชื่อเหรียญที่เคยเปิดออเดอร์ในเซสชันนี้แล้ว ห้ามเข้าซ้ำเด็ดขาด
        self.session_executed_symbols = set()
        
        # Observability Diagnostic Vectors
        self.telemetry = {
            "total_execution_attempts": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "api_throttling_count": 0,
            "last_error_msg": "None",
            "last_heartbeat": datetime.now().strftime("%H:%M:%S")
        }
        
        self.account_info = {
            "initial_wallet": 0.0,
            "total_wallet": 0.0, 
            "equity": 0.0, 
            "pnl": 0.0, 
            "active_orders": 0, 
            "last_update": "None"
        }
        self.symbol_info = {}
        self.twm = None 
        
        if self.mode == "LIVE":
            self.recover_state()

    def record_metrics(self, success=True, error_msg=None):
        with self.lock:
            self.telemetry["total_execution_attempts"] += 1
            if success:
                self.telemetry["successful_orders"] += 1
            else:
                self.telemetry["failed_orders"] += 1
                if error_msg:
                    self.telemetry["last_error_msg"] = str(error_msg)
                    if "429" in str(error_msg) or "1003" in str(error_msg):
                        self.telemetry["api_throttling_count"] += 1

    def save_state(self):
        try:
            with self.lock:
                cooldown_serializable = {k: v.isoformat() for k, v in self.cooldown_trades.items()}
                state_data = {
                    "cooldown_trades": cooldown_serializable,
                    "session_executed_symbols": list(self.session_executed_symbols), # เซฟบัญชีดำลงไฟล์ป้องกันบอทหลุด
                    "initial_wallet": self.account_info["initial_wallet"],
                    "telemetry": self.telemetry,
                    "timestamp": datetime.now().isoformat()
                }
            with open(STATE_FILE, "w") as f:
                json.dump(state_data, f, indent=4)
        except Exception as e:
            self.record_metrics(success=False, error_msg=f"Save State Fail: {e}")

    def recover_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r") as f:
                state_data = json.load(f)
            with self.lock:
                self.account_info["initial_wallet"] = state_data.get("initial_wallet", 0.0)
                self.telemetry = state_data.get("telemetry", self.telemetry)
                self.session_executed_symbols = set(state_data.get("session_executed_symbols", []))
                for sym, dt_str in state_data.get("cooldown_trades", {}).items():
                    self.cooldown_trades[sym] = datetime.fromisoformat(dt_str)
            self.system_status = "RECOVERED 🔄"
        except Exception as e:
            self.system_status = "RECOVERY FAILED ⚠️"

    def get_symbol_price(self, symbol):
        with self.lock:
            price = self.price_cache.get(symbol, 0.0)
        if price == 0.0 and self.mode == "LIVE":
            try:
                res = self.client.futures_symbol_ticker(symbol=symbol)
                price = float(res['price'])
                with self.lock:
                    self.price_cache[symbol] = price
            except Exception: 
                return 0.0
        return price

    # --------------------------------------------------------------------------
    # NETWORK DATA STREAM (HIGH VISCOSITY SOCKET RECEIVER)
    # --------------------------------------------------------------------------
    def start_ticker_websocket(self):
        if self.mode != "LIVE": return
        try:
            self.twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
            self.twm.start()

            def handle_socket_message(msg):
                with self.lock:
                    if isinstance(msg, list):
                        for ticker in msg:
                            symbol = ticker.get('s')
                            if symbol:
                                self.price_cache[symbol] = float(ticker.get('c', 0))
                    elif isinstance(msg, dict) and msg.get('e') == '24hrMiniTicker':
                        self.price_cache[msg['s']] = float(msg['c'])

            self.twm.start_miniticker_socket(callback=handle_socket_message)
        except Exception as e:
            self.record_metrics(success=False, error_msg=f"WS Exception: {e}")

    def update_account_and_risk(self):
        if self.mode != "LIVE": return
        while True:
            try:
                acc = self.client.futures_account()
                current_active = [p for p in acc["positions"] if float(p["positionAmt"]) != 0]
                active_symbols = {p["symbol"] for p in current_active}
                
                total_wallet = float(acc["totalWalletBalance"])
                equity = float(acc["totalMarginBalance"])
                
                with self.lock:
                    if self.account_info["initial_wallet"] == 0.0:
                        self.account_info["initial_wallet"] = total_wallet
                        
                    self.active_positions = current_active
                    self.account_info.update({
                        "total_wallet": total_wallet, 
                        "equity": equity, 
                        "pnl": float(acc["totalUnrealizedProfit"]),
                        "active_orders": len(current_active),
                        "last_update": datetime.now().strftime("%H:%M:%S")
                    })
                    self.telemetry["last_heartbeat"] = datetime.now().strftime("%H:%M:%S")
                    initial = self.account_info["initial_wallet"]

                    # 🔥 DYNAMIC CLEANER GATES: ถ้าเหรียญไหนปิดสถานะไปแล้ว และพ้น Cooldown ให้ปลดล็อกออกจากบัญชีดำเซสชัน
                    now = datetime.now()
                    cleared_symbols = set()
                    for sym in list(self.session_executed_symbols):
                        if sym not in active_symbols:
                            if sym in self.cooldown_trades:
                                expire = self.cooldown_trades[sym] + timedelta(minutes=TRADE_COOLDOWN_MINUTES)
                                if now >= expire:
                                    cleared_symbols.add(sym)
                            else:
                                # หากไม่มีในประวัติ Cooldown แต่ออเดอร์จบแล้ว ให้ปลดล็อกได้
                                cleared_symbols.add(sym)
                    
                    for sym in cleared_symbols:
                        self.session_executed_symbols.discard(sym)
                
                # --- [CIRCUIT BREAKER RISK PROTOCOL] ---
                drawdown = (initial - equity) / initial if initial > 0 else 0
                if drawdown >= MAX_DAILY_DRAWDOWN_PCT:
                    with self.lock:
                        self.system_status = "CRITICAL BREAKER TRIGGERED 🛑"
                    
                    for pos in current_active:
                        sym = pos['symbol']
                        amt = float(pos['positionAmt'])
                        side = SIDE_SELL if amt > 0 else SIDE_BUY
                        try:
                            self.client.futures_create_order(
                                symbol=sym, side=side, type=ORDER_TYPE_MARKET, 
                                quantity=abs(amt), reduceOnly=True
                            )
                            self.client.futures_cancel_all_open_orders(symbol=sym)
                        except Exception:
                            pass
                    time.sleep(60)
                    continue

                # --- [ANTI-ORPHAN ORDER CLEANER] ---
                open_orders = self.client.futures_get_open_orders()
                for order in open_orders:
                    sym = order['symbol']
                    if sym not in active_symbols:
                        try:
                            self.client.futures_cancel_all_open_orders(symbol=sym)
                        except Exception:
                            pass

                # --- [COOLDOWN AUTOMATION ENGINE] ---
                all_recent_orders = self.client.futures_get_all_orders(limit=20)
                now = datetime.now()
                for order in all_recent_orders:
                    if order['status'] == 'FILLED' and (order.get('reduceOnly') is True or order.get('closePosition') is True):
                        closed_time = datetime.fromtimestamp(order['updateTime'] / 1000.0)
                        if now - closed_time < timedelta(minutes=TRADE_COOLDOWN_MINUTES):
                            with self.lock:
                                if order['symbol'] not in self.cooldown_trades:
                                    self.cooldown_trades[order['symbol']] = closed_time
                
                self.save_state()

            except Exception as e:
                self.record_metrics(success=False, error_msg=f"Risk Loop Err: {e}")
            time.sleep(SCAN_INTERVAL)

    # --------------------------------------------------------------------------
    # DETERMINISTIC STATE MACHINE EXECUTION ENGINE
    # --------------------------------------------------------------------------
    def verify_remote_order_state(self, symbol, order_id, target_state, verification_timeout_sec=5):
        start_gate = time.time()
        while time.time() - start_gate < verification_timeout_sec:
            try:
                order = self.client.futures_get_order(symbol=symbol, orderId=order_id)
                self.metrics_api_increment()
                if order['status'] == target_state:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def metrics_api_increment(self):
        with self.lock:
            self.telemetry["total_execution_attempts"] += 1

    def check_portfolio_exposure_limits(self, symbol, side, proposed_notional):
        with self.lock:
            equity = self.account_info["equity"]
            active_positions = list(self.active_positions)
            
        if equity == 0: return False
        
        current_long_notional = 0.0
        current_short_notional = 0.0
        sector_notional = 0.0
        
        for pos in active_positions:
            amt = float(pos.get('positionAmt', 0))
            entry_p = float(pos.get('entryPrice', 0))
            notional = abs(amt) * entry_p
            
            if amt > 0: current_long_notional += notional
            elif amt < 0: current_short_notional += notional
            
            if pos['symbol'] == symbol:
                sector_notional += notional

        if (sector_notional + proposed_notional) / equity > MAX_SECTOR_EXPOSURE:
            return False
            
        if side == SIDE_BUY and (current_long_notional + proposed_notional) / equity > MAX_PORTFOLIO_LONG_EXPOSURE:
            return False
        if side == SIDE_SELL and (current_short_notional + proposed_notional) / equity > MAX_PORTFOLIO_SHORT_EXPOSURE:
            return False
            
        return True

    def execute_institutional_trade(self, symbol, side, entry, atr):
        """
        [DETERMINISTIC STATE MACHINE ENGINE - ISOLATED MEMORY MODEL]
        """
        try:
            with self.lock:
                # 🛑 TRIPLE LOCK SECURITY GATE (ระบบเช็คความซ้ำระดับสูงสุด)
                if "🛑" in self.system_status or self.account_info["active_orders"] >= MAX_ACTIVE_TRADES: return
                
                # ด่านที่ 1: เช็คจาก Memory Blacklist ในอดีตของรอบเซสชันนี้
                if symbol in self.session_executed_symbols: return
                
                # ด่านที่ 2: เช็คยอดคงค้างในกระเป๋า Real-time ณ ปัจจุบัน
                if any(p['symbol'] == symbol for p in self.active_positions): return
                
                # ด่านที่ 3: เช็คว่าติดเวลา Cooldown หรือไม่
                if symbol in self.cooldown_trades: return

            s_info = self.symbol_info.get(symbol)
            if not s_info: return

            p_prec = s_info["price_precision"]
            l_prec = s_info["lot_precision"]

            # คำนวณขนาดสัญญาจากวงเงิน $5 คงที่
            proposed_notional = FIXED_MARGIN_PER_TRADE * LEVERAGE
            qty = round(proposed_notional / entry, l_prec)
            
            if qty < s_info.get("min_qty", 0.0): 
                qty = s_info.get("min_qty", 0.0)

            proposed_notional = qty * entry
            if not self.check_portfolio_exposure_limits(symbol, side, proposed_notional): return

            half_qty = round(qty * 0.5, l_prec)
            if half_qty < s_info.get("min_qty", 0.0): half_qty = qty

            sl_distance = atr * 1.5
            if side == SIDE_BUY:
                sl = round(entry - sl_distance, p_prec)
                tp1 = round(entry + (atr * 1.5), p_prec)
                tp2 = round(entry + (atr * 3.0), p_prec)
                exit_side = SIDE_SELL
            else:
                sl = round(entry + sl_distance, p_prec)
                tp1 = round(entry - (atr * 1.5), p_prec)
                tp2 = round(entry - (atr * 3.0), p_prec)
                exit_side = SIDE_BUY

            if self.mode == "LIVE":
                # 🔥 MEMORY LOCK ACTIVATION: สั่งล็อกรายชื่อเหรียญทันทีก่อนยิงออเดอร์ เพื่อป้องกันจังหวะ Race Condition
                with self.lock:
                    self.session_executed_symbols.add(symbol)

                # ==============================================================
                # STAGE 1: ENTRY PLACEMENT
                # ==============================================================
                self.client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
                entry_order = self.client.futures_create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
                self.metrics_api_increment()
                
                # ==============================================================
                # STAGE 2: CONFIRM ENTRY FILLED GATE
                # ==============================================================
                if not self.verify_remote_order_state(symbol, entry_order['orderId'], "FILLED"):
                    self.record_metrics(success=False, error_msg=f"[{symbol}] Entry Order Verification Timeout Blocked.")
                    return

                # ==============================================================
                # STAGE 3: STOP LOSS (SL) PLACEMENT
                # ==============================================================
                sl_order = self.client.futures_create_order(symbol=symbol, side=exit_side, type=FUTURE_ORDER_TYPE_STOP_MARKET, stopPrice=sl, closePosition=True)
                self.metrics_api_increment()

                # ==============================================================
                # STAGE 4: CONFIRM SL REGISTERED
                # ==============================================================
                if not self.verify_remote_order_state(symbol, sl_order['orderId'], "NEW"):
                    self.client.futures_create_order(symbol=symbol, side=exit_side, type=ORDER_TYPE_MARKET, quantity=qty, reduceOnly=True)
                    self.record_metrics(success=False, error_msg=f"[{symbol}] CRITICAL: SL Execution Inactive. Closed Position Immediately.")
                    return

                # ==============================================================
                # STAGE 5: TAKE PROFIT (TP) PLACEMENT
                # ==============================================================
                tp1_order = self.client.futures_create_order(symbol=symbol, side=exit_side, type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, stopPrice=tp1, quantity=half_qty, reduceOnly=True)
                tp2_order = self.client.futures_create_order(symbol=symbol, side=exit_side, type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, stopPrice=tp2, closePosition=True)
                self.metrics_api_increment()

                # ==============================================================
                # STAGE 6: CONFIRM TP ACTIVE
                # ==============================================================
                self.verify_remote_order_state(symbol, tp1_order['orderId'], "NEW")
                self.verify_remote_order_state(symbol, tp2_order['orderId'], "NEW")

                with self.lock:
                    self.cooldown_trades[symbol] = datetime.now()
                self.record_metrics(success=True)
                
            else:
                return {"symbol": symbol, "side": side, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "qty": qty}

        except Exception as e:
            # เกิดข้อผิดพลาดรุนแรง ให้ปลดล็อก Memory เผื่อลองจับสัญญาณในอนาคตรอบหน้า
            with self.lock:
                self.session_executed_symbols.discard(symbol)
            self.record_metrics(success=False, error_msg=e)

    # --------------------------------------------------------------------------
    # ENGINE SCANNER / ANALYTICS MATRIX
    # --------------------------------------------------------------------------
    def analyze_market_dataframe(self, df):
        df['atr'] = ta.atr(df['h'], df['l'], df['c'], length=14)
        df['vol_sma'] = ta.sma(df['v'], length=20)
        df['rsi'] = ta.rsi(df['c'], length=14)
        adx_df = ta.adx(df['h'], df['l'], df['c'], length=14)
        df['adx'] = adx_df['ADX_14'] if adx_df is not None else 0
        return df

    def run_backtest_validation(self, symbol, historical_df):
        print(f"⚙️ Running Deterministic Validation for {symbol}...")
        df = self.analyze_market_dataframe(historical_df.copy())
        backtest_logs = []
        
        for i in range(30, len(df)):
            window = df.iloc[:i+1]
            last_row = window.iloc[-1]
            ratio = (last_row['v'] / last_row['vol_sma']) if last_row['vol_sma'] > 0 else 0
            
            if ratio > VOL_MULTIPLIER:
                if last_row['adx'] > ADX_TREND_THRESHOLD and last_row['rsi'] < 35:
                    backtest_logs.append(f" Row {i} | [{symbol}] Trend Backtest Trigger BUY at {last_row['c']:.4f}")
                elif last_row['adx'] <= ADX_TREND_THRESHOLD and last_row['rsi'] < 28:
                    backtest_logs.append(f" Row {i} | [{symbol}] Mean-Rev Backtest Trigger BUY at {last_row['c']:.4f}")
                    
        print(f"✅ Backtest Validation Complete. Identified {len(backtest_logs)} Signals.")
        return backtest_logs

    def scanner_loop(self):
        if self.mode != "LIVE": return
        try:
            tickers = self.client.futures_ticker()
            self.symbols = [x["symbol"] for x in sorted(tickers, key=lambda x: float(x["quoteVolume"]), reverse=True)[:TOP_N] if x["symbol"].endswith("USDT")]
            
            ex_info = self.client.futures_exchange_info()
            for s in ex_info['symbols']:
                if s['symbol'] in self.symbols:
                    lot_f = next(f for f in s['filters'] if f['filterType']=='LOT_SIZE')
                    price_f = next(f for f in s['filters'] if f['filterType']=='PRICE_FILTER')
                    self.symbol_info[s['symbol']] = {
                        "lot_precision": len(lot_f['stepSize'].rstrip('0').split('.')[1]) if '.' in lot_f['stepSize'] else 0,
                        "price_precision": len(price_f['tickSize'].rstrip('0').split('.')[1]) if '.' in price_f['tickSize'] else 0,
                        "min_qty": float(lot_f['minQty'])
                    }
        except Exception as e:
            self.record_metrics(success=False, error_msg=e)

        while True:
            start_time = time.time()
            for symbol in self.symbols:
                self.current_scanning = symbol
                try:
                    with self.lock:
                        # ดักตรวจแผงควบคุมระดับตรรกะซ้ำซ้อนใน Scanner
                        if symbol in self.session_executed_symbols: continue
                        if symbol in self.cooldown_trades:
                            expire = self.cooldown_trades[symbol] + timedelta(minutes=TRADE_COOLDOWN_MINUTES)
                            if datetime.now() < expire: continue
                            else: del self.cooldown_trades[symbol]

                    bars = self.client.futures_klines(symbol=symbol, interval="15m", limit=100)
                    if len(bars) < 30: continue
                    
                    df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                    df = self.analyze_market_dataframe(df)
                    
                    last_row = df.iloc[-1]
                    atr = last_row['atr']
                    ratio = (last_row['v'] / last_row['vol_sma']) if last_row['vol_sma'] > 0 else 0
                    rsi = last_row['rsi']
                    adx = last_row['adx']
                    
                    if pd.isna(atr) or pd.isna(rsi) or pd.isna(adx): continue

                    with self.lock:
                        self.market_vol_ratio[symbol] = ratio
                        self.market_vol_status[symbol] = ratio > VOL_MULTIPLIER
                        self.market_scores[symbol] = 3 if (rsi < 35 or rsi > 65) else 1
                        active_count = self.account_info["active_orders"]

                    if ratio > VOL_MULTIPLIER and active_count < MAX_ACTIVE_TRADES:
                        if adx > ADX_TREND_THRESHOLD:
                            if rsi < 35: 
                                self.execute_institutional_trade(symbol, SIDE_BUY, last_row['c'], atr)
                        else:
                            if rsi < 28:
                                self.execute_institutional_trade(symbol, SIDE_BUY, last_row['c'], atr)
                            elif rsi > 72:
                                self.execute_institutional_trade(symbol, SIDE_SELL, last_row['c'], atr)

                    time.sleep(0.04)
                except Exception: 
                    continue
            
            with self.lock:
                self.ping = int((time.time() - start_time) * 1000)
            time.sleep(SCAN_INTERVAL)

    # --------------------------------------------------------------------------
    # MISSION CONTROL PANEL GRAPHICAL INTERFACE (MODERNIZED UI)
    # --------------------------------------------------------------------------
    def run(self):
        if self.mode != "LIVE":
            print("⚠️ Engine operating in Backtest Validation. Panel Disabled.")
            return
            
        threading.Thread(target=self.start_ticker_websocket, daemon=True).start()
        threading.Thread(target=self.update_account_and_risk, daemon=True).start()
        threading.Thread(target=self.scanner_loop, daemon=True).start()
        
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3), 
            Layout(name="main", ratio=1), 
            Layout(name="footer", size=4)
        )
        layout["main"].split_row(
            Layout(name="left", ratio=13), 
            Layout(name="right", ratio=11)
        )
        layout["left"].split_column(Layout(name="market", ratio=1))
        layout["right"].split_column(
            Layout(name="wallet", size=5),
            Layout(name="active", size=8),
            Layout(name="telemetry", ratio=1)
        )

        with Live(layout, refresh_per_second=2, screen=True):
            while True:
                with self.lock:
                    ping_val = self.ping
                    scan_val = self.current_scanning
                    status_val = self.system_status
                    telemetry_snap = dict(self.telemetry)
                    active_snap = list(self.active_positions)
                    acc_snap = dict(self.account_info)
                    scores_snap = dict(self.market_scores)
                    vol_status_snap = dict(self.market_vol_status)

                # --- 1. MODERNISED HEADER ---
                is_operational = "🛑" not in status_val
                status_color = "spring_green1" if is_operational else "bright_red"
                status_badge = "ACTIVE" if is_operational else "BREAKER"
                
                header_text = Text.assemble(
                    (" QUANTUM PRO ", "bold white on deep_sky_blue1"),
                    (f" v7.2.0 ", "bold sky_blue1 dim"),
                    (" ── ", "dim white"),
                    (f" [{status_badge}] ", f"bold {status_color}"),
                    (" ── ", "dim white"),
                    ("SYNC: ", "bold gray70"), (f"{scan_val} ", "cyan"),
                    ("PING: ", "bold gray70"), (f"{ping_val}ms", "green" if ping_val < 150 else "yellow")
                )
                layout["header"].update(Panel(Align.center(header_text, vertical="middle"), border_style="gray23"))

                # --- 2. PREMIUM CAPITAL BALANCE POOL ---
                pnl_val = acc_snap.get('pnl', 0.0)
                pnl_color = "spring_green1" if pnl_val >= 0 else "bright_red"
                pnl_prefix = "▲ +" if pnl_val >= 0 else "▼ "
                
                w_table = Table.grid(expand=True)
                w_table.add_column(style="bold gray84", ratio=1)
                w_table.add_column(justify="right", ratio=1)
                w_table.add_row("Net Equity Value:", f"[bold cyan]${acc_snap.get('equity', 0.0):,.2f}[/]")
                w_table.add_row("Total Wallet Pool:", f"[dim white]${acc_snap.get('total_wallet', 0.0):,.2f}[/]")
                w_table.add_row("Unrealized Session PnL:", f"[bold {pnl_color}]{pnl_prefix}${pnl_val:,.2f}[/]")
                layout["wallet"].update(Panel(w_table, title="[bold white]💳 CAPITAL OVERVIEW[/]", border_style="deep_sky_blue1", padding=(1, 2)))

                # --- 3. CLEAN SCANNER MATRIX ---
                m_table = Table(expand=True, box=box.HORIZONTALS, border_style="gray23", header_style="bold gray70")
                m_table.add_column("#", style="dim white", width=4, justify="center")
                m_table.add_column("SYMBOL", style="bold white")
                m_table.add_column("LAST PRICE", justify="right", style="cyan") 
                m_table.add_column("SCORE", justify="center")
                m_table.add_column("VOLATILITY STATUS", justify="right")

                for i, s in enumerate(self.symbols[:14]):
                    price = self.get_symbol_price(s)
                    score = scores_snap.get(s, 0)
                    score_stars = "★" * score + "☆" * (3 - score)
                    score_display = f"[bold gold1]{score_stars}[/]"
                    
                    is_shock = vol_status_snap.get(s)
                    vol_display = "[bold dark_orange]⚡ SHOCK OVERFLOW[/]" if is_shock else "[dim gray62]○ Stable[/]"
                    
                    m_table.add_row(str(i+1).zfill(2), s, f"{price:,.4f}", score_display, vol_display)
                layout["market"].update(Panel(m_table, title="[bold white]🛰️ ASSET MATRIX ANALYSIS (TOP VOLUME)[/]", border_style="gray23"))

                # --- 4. STREAMLINED DETECTED EXPOSURE ---
                p_table = Table(expand=True, box=box.MINIMAL, border_style="gray23", header_style="bold gray70")
                p_table.add_column("ACTIVE POSITION", style="bold white")
                p_table.add_column("DIRECTION", justify="center")
                p_table.add_column("REALTIME RISK PNL", justify="right")
                
                has_pos = False
                for p in active_snap:
                    amt = float(p.get('positionAmt', 0))
                    if amt == 0: continue
                    has_pos = True
                    side_display = "[bold aquamarine1]▲ LONG[/]" if amt > 0 else "[bold indian_red1]▼ SHORT[/]"
                    pnl = float(p.get('unrealizedProfit', 0))
                    pos_pnl_color = "spring_green1" if pnl >= 0 else "bright_red"
                    p_table.add_row(p['symbol'], side_display, f"[bold {pos_pnl_color}]{pnl:+.2f} USDT[/]")
                
                if not has_pos:
                    p_table.add_row("[dim gray42]No Active Exposure[/]", "", "")
                    
                layout["active"].update(Panel(p_table, title="[bold white]⚔️ ACTIVE EXPOSURE RISK GATE[/]", border_style="gray23"))

                # --- 5. REALTIME TELEMETRY DATA ---
                t_table = Table.grid(expand=True, padding=(0, 1))
                t_table.add_column(style="bold gray62", ratio=3)
                t_table.add_column(justify="right", style="bold gold1", ratio=2)
                t_table.add_row("Total Execution Sequences:", str(telemetry_snap["total_execution_attempts"]))
                t_table.add_row("Order Routing Metrics (Pass/Fail):", f"[green]{telemetry_snap['successful_orders']}[/] / [red]{telemetry_snap['failed_orders']}[/]")
                t_table.add_row("API Throttle Rate Control:", f"[orange1]{telemetry_snap['api_throttling_count']}[/]")
                t_table.add_row("Core Node Heartbeat Sync:", f"[cornflower_blue]{telemetry_snap['last_heartbeat']}[/]")
                layout["telemetry"].update(Panel(t_table, title="[bold white]📊 METRIC OBSERVED REGISTRY[/]", border_style="gray23", padding=(1, 2)))

                # --- 6. CLEAN FAULT LOG FOOTER ---
                err_msg = telemetry_snap['last_error_msg']
                err_color = "dim gray42" if err_msg == "None" else "bright_red"
                layout["footer"].update(Panel(f"[{err_color}]{err_msg}[/]", title="[bold white]🚨 SYSTEM SECURITY EXCEPTION LOG[/]", border_style="gray23"))
                
                time.sleep(1)

if __name__ == "__main__":
    QuantumInstitutionalSystem(mode="LIVE").run()