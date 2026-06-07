import os
import threading
import time
import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_ta as ta
import requests
import plotly.graph_objects as go
from binance.client import Client
from binance.enums import *
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv
from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

# ==============================================================================
# CONFIGURATION & INSTITUTIONAL SYSTEM SETTINGS
# ==============================================================================
load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

# Trading Parameters
TOP_N = 50
LEVERAGE = 10
RISK_PER_TRADE = 0.03 
MAX_ACTIVE_TRADES = 6      
SCAN_INTERVAL = 15 
BLACKLIST_AUTO_RELEASE_MINUTES = 30 
TRADE_COOLDOWN_MINUTES = 15 # เพิ่มค่าคงที่สำหรับคูลดาวน์เหรียญที่เพิ่งปิด 15 นาที
VOL_MULTIPLIER = 1.1      

# New Parameters for TP/SL
TP_RATIO = 2.0  # Reward per Risk (e.g., Risk 1 : Reward 2)
ATR_MULTIPLIER_SL = 1.5
ATR_MULTIPLIER_TP = ATR_MULTIPLIER_SL * TP_RATIO

# Setup Institutional In-Memory Logging (DISK WRITE PREVENTED FOR HFT SPEED OPTIMIZATION)
logging.basicConfig(
    level=logging.CRITICAL, # ยกเลิกการเขียนไฟล์สตรีม ล็อกเฉพาะระดับวิกฤตของระบบภายใน
    handlers=[logging.NullHandler()] # ผันสายข้อมูลออกจาก Disk Storage ทั้งหมดเพื่อประสิทธิภาพสูงสุด
)

class QuantumUltimateSystem:
    """
    QUANTITATIVE EXECUTION DESK (QED) - INSTITUTIONAL ARBITRAGE & MOMENTUM ENGINE
    Proprietary high-speed liquidity scanner with real-time risk mitigation.
    """
    
    def __init__(self):
        # Initialize Binance Client
        self.client = Client(API_KEY, API_SECRET)
        
        # Market Data Buffers
        self.symbols = []
        self.active_positions = []
        self.logs = []
        self.ping = 0
        self.current_scanning = "INIT"
        
        # State Management
        self.closed_trades = {}      
        self.cooldown_trades = {} # เพิ่มพื้นที่เก็บข้อมูลคูลดาวน์สำหรับเหรียญที่เพิ่งปิดสัญญา
        self.market_scores = {} 
        self.market_vol_status = {} 
        self.market_vol_ratio = {}   
        self.price_cache = {}
        self.deep_analysis_report = "Analysing Macro Market Inflow..."
        
        # Account Metadata
        self.account_info = {
            "total_wallet": 0.0, 
            "equity": 0.0, 
            "pnl": 0.0, 
            "active_orders": 0, 
            "last_update": "None"
        }
        self.symbol_info = {}
        self.twm = None 

    # --------------------------------------------------------------------------
    # CORE UTILITIES (INSTITUTIONAL STANDARD)
    # --------------------------------------------------------------------------
    def log(self, msg, level="INFO"):
        """System terminal feed routing only. Disk logging completely muted to eliminate I/O lag."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        tag = {"INFO": "[SYS]", "TRADE": "[EXE]", "ERROR": "[ERR]", "WARN": "[RM]"}.get(level, "[MSC]")
        color = {"INFO": "bright_blue", "TRADE": "green", "ERROR": "bright_red", "WARN": "dark_orange"}.get(level, "white")
        
        formatted_msg = f"[{color}]{tag} [{timestamp}] {msg}[/]"
        self.logs.append(formatted_msg)
        
        if len(self.logs) > 12: 
            self.logs.pop(0)

    def clean_series(self, data):
        """Helper to ensure pandas series compatibility."""
        if isinstance(data, pd.DataFrame): 
            return data.iloc[:, 0]
        return data

    def get_symbol_price(self, symbol):
        """Retrieves price from cache or directly from API if cache is empty."""
        price = self.price_cache.get(symbol, 0.0)
        if price == 0.0:
            try:
                res = self.client.futures_symbol_ticker(symbol=symbol)
                price = float(res['price'])
                self.price_cache[symbol] = price
            except Exception: 
                return 0.0
        return price

    # --------------------------------------------------------------------------
    # DATA STREAMS (WEBSOCKET & API)
    # --------------------------------------------------------------------------
    def start_ticker_websocket(self):
        """Starts a high-speed mini-ticker websocket for real-time price updates."""
        try:
            self.twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
            self.twm.start()

            def handle_socket_message(msg):
                if isinstance(msg, list):
                    for ticker in msg:
                        symbol = ticker.get('s')
                        if symbol:
                            self.price_cache[symbol] = float(ticker.get('c', 0))
                elif isinstance(msg, dict) and msg.get('e') == '24hrMiniTicker':
                    self.price_cache[msg['s']] = float(msg['c'])

            self.twm.start_miniticker_socket(callback=handle_socket_message)
            self.log("High-Speed Financial Stream Protocol Connected", "INFO")
        except Exception as e:
            self.log(f"Data Stream Connection Fault: {str(e)}", "ERROR")

    def update_account_core(self):
        """Continuous background thread to update account balance and positions."""
        while True:
            try:
                acc = self.client.futures_account()
                self.active_positions = [p for p in acc["positions"] if float(p["positionAmt"]) != 0]
                self.account_info.update({
                    "total_wallet": float(acc["totalWalletBalance"]), 
                    "equity": float(acc["totalMarginBalance"]), 
                    "pnl": float(acc["totalUnrealizedProfit"]),
                    "active_orders": len(self.active_positions),
                    "last_update": datetime.now().strftime("%H:%M:%S")
                })
                # เรียกฟังก์ชันตรวจสอบการปิดสถานะเพื่ออัปเดตระบบคูลดาวน์ 15 นาที
                self.check_and_update_cooldown()
            except Exception as e:
                self.log(f"Portfolio Account Synchronization Delayed: {str(e)}", "WARN")
            time.sleep(SCAN_INTERVAL)

    def check_and_update_cooldown(self):
        """ตรวจสอบออเดอร์ที่พึ่งปิดล่าสุดในตลาด เพื่อนำมาเข้า Cooldown 15 นาทีป้องกันการเข้าซ้ำ"""
        try:
            for symbol in self.symbols:
                if any(p['symbol'] == symbol for p in self.active_positions):
                    continue
                
                trades = self.client.futures_get_open_orders(symbol=symbol)
                all_orders = self.client.futures_get_all_orders(symbol=symbol, limit=5)
                
                if all_orders:
                    sorted_orders = sorted(all_orders, key=lambda x: x['updateTime'], reverse=True)
                    last_order = sorted_orders[0]
                    
                    if last_order['status'] == 'FILLED' and (last_order.get('reduceOnly') is True or last_order.get('closePosition') is True):
                        closed_time = datetime.fromtimestamp(last_order['updateTime'] / 1000.0)
                        if datetime.now() - closed_time < timedelta(minutes=TRADE_COOLDOWN_MINUTES):
                            if symbol not in self.cooldown_trades:
                                self.cooldown_trades[symbol] = closed_time
                                self.log(f"Risk Management Active: {symbol} Position Liquidation Completed. Lock Period Triggered.", "WARN")
        except Exception as e:
            pass

    def update_deep_analysis(self):
        """Macro market analysis focusing on BTC 4H trend."""
        while True:
            try:
                bars = self.client.futures_klines(symbol="BTCUSDT", interval="4h", limit=200)
                df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                df['ema200'] = self.clean_series(ta.ema(df['c'], length=200))
                df['rsi'] = self.clean_series(ta.rsi(df['c'], length=14))
                last = df.iloc[-1]
                
                direction = "BULLISH ACCELERATION" if last['c'] > last['ema200'] else "BEARISH DISTRIBUTION"
                self.deep_analysis_report = (
                    f"[bold yellow]📊 MACRO STRUCTURE (BTCUSDT 4H)[/]\n"
                    f"Index Price: ${last['c']:,.2f} | Regime: [bold cyan]{direction}[/]\n"
                    f"Momentum Index (RSI): {last['rsi']:.1f} | institutional Baseline (EMA200): ${last['ema200']:,.2f}"
                )
            except Exception: 
                pass
            time.sleep(30)

    # --------------------------------------------------------------------------
    # TRADING LOGIC & RISK MITIGATION ENGINE
    # --------------------------------------------------------------------------
    def is_on_cooldown(self, symbol):
        """Checks if a symbol is temporarily blacklisted after a trade."""
        now = datetime.now()
        
        if symbol in self.cooldown_trades:
            expire_cooldown = self.cooldown_trades[symbol] + timedelta(minutes=TRADE_COOLDOWN_MINUTES)
            if now < expire_cooldown:
                return True, f"LOCK: {int((expire_cooldown-now).total_seconds())}s"
            else:
                del self.cooldown_trades[symbol]

        if symbol in self.closed_trades:
            expire = self.closed_trades[symbol] + timedelta(minutes=BLACKLIST_AUTO_RELEASE_MINUTES)
            if now < expire: 
                return True, f"BL: {int((expire-now).total_seconds())}s"
        return False, None

    def execute_trade(self, symbol, side, entry, atr):
        """
        Execute market orders with automatic Take Profit and Stop Loss.
        Uses ATR for dynamic TP/SL distance calculation.
        
        Strict Institutional Protocol: Pyramiding Block implemented to prevent manual/automated averaging down.
        """
        try:
            # NO ORDER PYRAMIDING CHECK (ป้องกันออเดอร์ที่เข้าแล้วเด็ดขาด ไม่มีการช้อนเพิ่ม)
            if any(position['symbol'] == symbol for position in self.active_positions):
                return

            if self.account_info["active_orders"] >= MAX_ACTIVE_TRADES: 
                self.log("Risk Threshold Reached: Maximum Asset Allocation Capacity Utilized.", "WARN")
                return

            on_cd, _ = self.is_on_cooldown(symbol)
            if on_cd:
                return

            s_info = self.symbol_info.get(symbol)
            if not s_info: return

            p_prec = s_info["price_precision"]
            l_prec = s_info["lot_precision"]
            min_qty = s_info.get("min_qty", 0.0)

            # Calculation TP/SL
            sl_dist = atr * ATR_MULTIPLIER_SL
            tp_dist = atr * ATR_MULTIPLIER_TP

            if side == SIDE_BUY:
                sl = round(entry - sl_dist, p_prec)
                tp = round(entry - tp_dist, p_prec) 
                exit_side = SIDE_SELL
            else:
                sl = round(entry + sl_dist, p_prec)
                tp = round(entry - tp_dist, p_prec) 
                exit_side = SIDE_BUY

            # Position Sizing
            risk_amt = self.account_info["total_wallet"] * RISK_PER_TRADE
            qty = round(risk_amt / abs(entry - sl), l_prec)

            if qty < min_qty:
                self.log(f"Execution Aborted: Sizing Model {qty} falls below exchanges requirements for {symbol}", "WARN")
                return

            # Execution Flow
            self.log(f"Routing Strategic Position {side} - {symbol} | Risk Anchor (SL): {sl} Target (TP): {tp}", "TRADE")
            
            # 1. Set Leverage
            self.client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
            
            # 2. Market Entry
            self.client.futures_create_order(
                symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty
            )

            # 3. Stop Loss (Stop Market)
            self.client.futures_create_order(
                symbol=symbol, side=exit_side, 
                type=FUTURE_ORDER_TYPE_STOP_MARKET, 
                stopPrice=sl, closePosition=True
            )

            # 4. Take Profit (Take Profit Market or Limit)
            self.client.futures_create_order(
                symbol=symbol, side=exit_side,
                type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                stopPrice=tp, closePosition=True
            )
            
            self.log(f"Algorithmic Risk/Reward Bracket Array Standardized for {symbol}", "INFO")

        except Exception as e: 
            self.log(f"Order Placement Interrupted by Liquidity Engine: {str(e)}", "ERROR")

    def scanner_loop(self):
        """Main loop for technical scanning and volume analysis."""
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
            
            self.log("Syncing Node Exchange Specifications Matrix...", "INFO")
                    
        except Exception as e: 
            self.log(f"Infrastructure Scanning Module Error: {e}", "ERROR")

        while True:
            start_time = time.time()
            for symbol in self.symbols:
                self.current_scanning = symbol
                try:
                    on_cd, _ = self.is_on_cooldown(symbol)
                    if on_cd:
                        time.sleep(0.05)
                        continue

                    if any(p['symbol'] == symbol for p in self.active_positions):
                        time.sleep(0.05)
                        continue

                    bars = self.client.futures_klines(symbol=symbol, interval="15m", limit=50)
                    df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                    
                    # Technicals
                    atr = ta.atr(df['h'], df['l'], df['c'], length=14).iloc[-1]
                    vol_sma = ta.sma(df['v'], length=20).iloc[-1]
                    current_vol = df['v'].iloc[-1]
                    ratio = (current_vol / vol_sma) if vol_sma > 0 else 0
                    
                    self.market_vol_ratio[symbol] = ratio
                    self.market_vol_status[symbol] = ratio > VOL_MULTIPLIER
                    
                    rsi = ta.rsi(df['c']).iloc[-1]
                    self.market_scores[symbol] = 3 if (rsi < 35 or rsi > 65) else 1
                    
                    if self.market_vol_status[symbol] and self.account_info["active_orders"] < MAX_ACTIVE_TRADES:
                        if rsi < 30:
                            self.execute_trade(symbol, SIDE_BUY, df['c'].iloc[-1], atr)
                        elif rsi > 70:
                            self.execute_trade(symbol, SIDE_SELL, df['c'].iloc[-1], atr)

                    time.sleep(0.1) 
                except Exception: 
                    continue
            self.ping = int((time.time() - start_time) * 1000)
            time.sleep(SCAN_INTERVAL)

    # --------------------------------------------------------------------------
    # RENDER ENGINE (INSTITUTIONAL TERMINAL)
    # --------------------------------------------------------------------------
    def run(self):
        """Launches background threads and starts the Live UI render engine."""
        threading.Thread(target=self.start_ticker_websocket, daemon=True).start()
        threading.Thread(target=self.update_account_core, daemon=True).start()
        threading.Thread(target=self.update_deep_analysis, daemon=True).start()
        threading.Thread(target=self.scanner_loop, daemon=True).start()
        
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4), 
            Layout(name="main", ratio=1), 
            Layout(name="footer", size=8)
        )
        layout["main"].split_row(
            Layout(name="left", ratio=12), 
            Layout(name="right", ratio=10)
        )
        layout["left"].split_column(
            Layout(name="market", ratio=1)
        )
        layout["right"].split_column(
            Layout(name="vol_spike", size=10),
            Layout(name="active", size=12),
            Layout(name="extra", ratio=1)
        )
        layout["extra"].split_row(
            Layout(name="analysis", ratio=1),
            Layout(name="cooldown", ratio=1), 
            Layout(name="wallet", ratio=1)
        )

        with Live(layout, refresh_per_second=2, screen=True):
            while True:
                # --- Header Render ---
                header_text = Text.assemble(
                    (" institutional Quantitative Desk ", "bold black on bright_white"), 
                    ("  SYSTEM NODE: CORE-ENG-v4.0.0", "white"),
                    (" | ", "dim"), 
                    (f"NETWORK LATENCY: {self.ping}ms", "bright_green" if self.ping < 1500 else "bright_red")
                )
                layout["header"].update(Panel(Align.center(header_text), subtitle=f"Current Asset In-Focus: {self.current_scanning}", border_style="white", box=box.SQUARE))

                # --- Market Scanner ---
                m_table = Table(expand=True, box=box.SQUARE, padding=(0,1))
                m_table.add_column("RANK", justify="center", style="dim")
                m_table.add_column("ASSET CLASS", style="bright_cyan")
                m_table.add_column("MARK PRICE", justify="right", style="bright_white") 
                m_table.add_column("SIGMA SCORE", justify="center")
                m_table.add_column("FLOW", justify="center")
                m_table.add_column("ALLOCATION STATUS")

                for i, s in enumerate(self.symbols[:25]):
                    hold, reason = self.is_on_cooldown(s)
                    price = self.get_symbol_price(s)
                    price_str = f"{price:,.6f}" if price < 1 else f"{price:,.2f}"
                    
                    flow_indicator = "[bright_green]STABLE[/]" if self.market_scores.get(s,1) == 1 else "[bold magenta]IMBALANCE[/]"
                    m_table.add_row(
                        str(i+1), s, price_str, 
                        f"RANK {self.market_scores.get(s,0)}",
                        flow_indicator, 
                        f"[bold orange1]{reason}[/]" if hold else "[bold green]ELIGIBLE[/]"
                    )
                layout["market"].update(Panel(m_table, title="📡 CORE MARKET FLOW ANALYSIS ENGINE", border_style="white", box=box.SQUARE))

                # --- Order Flow Imbalances (OFI) ---
                v_table = Table(expand=True, box=box.SQUARE)
                v_table.add_column("TICKER", style="bright_yellow")
                v_table.add_column("OFI RATIO", justify="right", style="bold magenta")
                v_table.add_column("VOLATILITY STATUS", justify="center")
                top_spikes = sorted(self.market_vol_ratio.items(), key=lambda x: x[1], reverse=True)[:5]
                for sym, rat in top_spikes:
                    action = "[bold red]TAIL RISK CRITICAL[/]" if rat > 3 else "[bold bright_cyan]LIQUIDITY EXPANSION[/]"
                    v_table.add_row(sym, f"{rat:.2f}x", action)
                layout["vol_spike"].update(Panel(v_table, title="⚡ ORDER FLOW IMBALANCE DETECTOR (OFI)", border_style="white", box=box.SQUARE))

                # --- Active Positions (Enhanced with Real-time Direction Forecast Engine) ---
                p_table = Table(expand=True, box=box.SQUARE)
                p_table.add_column("ASSET", style="bright_white", no_wrap=True)
                p_table.add_column("DIRECTION", justify="center")
                p_table.add_column("STRIKE ENTRY", justify="right", style="bright_yellow")
                p_table.add_column("UNREALIZED ($)", justify="right")
                p_table.add_column("DIRECTION FORECAST", justify="center") # คอลัมน์พยากรณ์ราคาขึ้น/ลงเรียลไทม์ที่เพิ่มขึ้นมา

                for p in self.active_positions.copy():
                    pos_amt = float(p.get('positionAmt', 0))
                    if pos_amt == 0: continue
                    
                    symbol = p['symbol']
                    side_text = "[bold green]▲ INST LONG[/]" if pos_amt > 0 else "[bold red]▼ INST SHORT[/]"
                    entry_price = float(p.get('entryPrice', 0))
                    pnl = float(p.get('unrealizedProfit', 0))
                    
                    # REAL-TIME DIRECTION FORECAST LOGIC (ดึงราคาตลาดสดจาก WebSocket Buffer มาคำนวณเวกเตอร์โมเมนตัมปัจจุบัน)
                    current_spot_price = self.get_symbol_price(symbol)
                    
                    if current_spot_price > 0:
                        price_diff_pct = ((current_spot_price - entry_price) / entry_price) * 100
                        
                        if pos_amt > 0: # สำหรับออเดอร์ฝั่ง LONG
                            if price_diff_pct > 0.4:
                                forecast_signal = "[blink bright_green]▲ ACCELERATING[/]" # กำลังขึ้นแรงมาก
                            elif price_diff_pct > 0:
                                forecast_signal = "[bright_green]▲ BULLISH HOLD[/]"   # ค่อยๆ ขึ้น/ทรงตัวแดนบวก
                            elif price_diff_pct < -0.4:
                                forecast_signal = "[blink red]▼ LIQUIDATING[/]"       # ราคากำลังดิ่งลงลึก
                            else:
                                forecast_signal = "[red]▼ BEARISH HOLD[/]"          # ติดลบเล็กน้อยทรงตัวแดนลบ
                        else: # สำหรับออเดอร์ฝั่ง SHORT
                            if price_diff_pct < -0.4:
                                forecast_signal = "[blink bright_green]▲ ACCELERATING[/]" # กำลังลงแรงมาก (บวกสัญญาสั้น)
                            elif price_diff_pct < 0:
                                forecast_signal = "[bright_green]▲ BULLISH HOLD[/]"   # กำลังลง/ทรงตัวแดนบวกของ Short
                            elif price_diff_pct > 0.4:
                                forecast_signal = "[blink red]▼ LIQUIDATING[/]"       # ราคากำลังพุ่งสวนทางทุบสัญญาสั้น
                            else:
                                forecast_signal = "[red]▼ BEARISH HOLD[/]"          # ขาดทุนเล็กน้อยทรงตัวแดนลบ
                    else:
                        forecast_signal = "[dim white]■ CONSOLIDATING[/]"           # ขาดการเชื่อมต่อ/ราคาคงที่ชั่วคราว
                    
                    p_table.add_row(
                        symbol, 
                        side_text, 
                        f"{entry_price:,.4f}",
                        f"[{'green' if pnl >= 0 else 'red'}]{pnl:+.2f}[/]", 
                        forecast_signal
                    )
                layout["active"].update(Panel(p_table, title="⚔️ INSTITUTIONAL ACTIVE RISK EXPOSURE", border_style="bright_green", box=box.SQUARE))

                # --- Macro Analysis & Capital Portfolio ---
                layout["analysis"].update(Panel(Align.left(self.deep_analysis_report), title="🛰️ QUANT MACRO INTELLIGENCE", border_style="white", box=box.SQUARE))
                
                # --- Cooldown Display Panel ---
                cd_text = Text()
                now = datetime.now()
                if not self.cooldown_trades:
                    cd_text.append("Risk Matrix: Nominal.\n", style="white")
                    cd_text.append("All Asset Corridors Verified 🟢", style="green")
                else:
                    for sym, closed_time in list(self.cooldown_trades.items()):
                        expire_cooldown = closed_time + timedelta(minutes=TRADE_COOLDOWN_MINUTES)
                        rem_seconds = int((expire_cooldown - now).total_seconds())
                        if rem_seconds > 0:
                            cd_text.append(f"🔒 LOCK {sym}: {rem_seconds}s\n", style="bold red")
                        else:
                            if sym in self.cooldown_trades:
                                del self.cooldown_trades[sym]
                layout["cooldown"].update(Panel(cd_text, title="⏳ ASSET RISK LOCK QUARANTINE", border_style="bright_red", box=box.SQUARE))

                w_text = Text.assemble(
                    ("NET EQUITY VALUE: ", "white"), (f"${self.account_info['equity']:.2f}\n", "bold cyan"),
                    ("TOTAL UNREALIZED PNL: ", "white"), (f"${self.account_info['pnl']:+.2f}", "bold green" if self.account_info['pnl'] >= 0 else "bold red")
                )
                layout["wallet"].update(Panel(w_text, title="💠 FIRM CAPITAL BALANCES", border_style="white", box=box.SQUARE))

                # --- Footer Feed ---
                layout["footer"].update(Panel("\n".join(self.logs), title="📡 LIVE INFRASTRUCTURE SYSTEM LOGS (IN-MEMORY CRYPTO FEED)", border_style="white", box=box.SQUARE))
                
                time.sleep(1)

if __name__ == "__main__":
    QuantumUltimateSystem().run()