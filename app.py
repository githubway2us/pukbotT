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
# CONFIGURATION & SYSTEM SETTINGS
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
BLACKLIST_AUTO_RELEASE_MINUTES = 10 
VOL_MULTIPLIER = 1.1      

# New Parameters for TP/SL
TP_RATIO = 2.0  # Reward per Risk (e.g., Risk 1 : Reward 2)
ATR_MULTIPLIER_SL = 1.5
ATR_MULTIPLIER_TP = ATR_MULTIPLIER_SL * TP_RATIO

# Setup Logging to file
logging.basicConfig(
    filename='quantum_system.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class QuantumUltimateSystem:
    """
    Advanced Trading Intelligence System with Real-time Monitoring,
    Volume Spike Detection, and Multi-target Take Profit Logic.
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
        self.market_scores = {} 
        self.market_vol_status = {} 
        self.market_vol_ratio = {}   
        self.price_cache = {}
        self.deep_analysis_report = "Analysing Market Data..."
        
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
    # CORE UTILITIES
    # --------------------------------------------------------------------------
    def log(self, msg, level="INFO"):
        """System logging with rich color formatting and file backup."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        emoji = {"INFO": "ℹ️", "TRADE": "🚀", "ERROR": "❌", "WARN": "⚠️"}.get(level, "🔹")
        color = {"INFO": "cyan", "TRADE": "bright_green", "ERROR": "bright_red", "WARN": "yellow"}.get(level, "white")
        
        formatted_msg = f"[{color}]{emoji} [{timestamp}] {msg}[/]"
        self.logs.append(formatted_msg)
        logging.info(f"[{level}] {msg}")
        
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
            self.log("WebSocket Online: Multi-stream Monitoring Activated", "INFO")
        except Exception as e:
            self.log(f"WS Connect Error: {str(e)}", "ERROR")

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
            except Exception as e:
                self.log(f"Account Update Fail: {str(e)}", "WARN")
            time.sleep(SCAN_INTERVAL)

    def update_deep_analysis(self):
        """Macro market analysis focusing on BTC 4H trend."""
        while True:
            try:
                bars = self.client.futures_klines(symbol="BTCUSDT", interval="4h", limit=200)
                df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                df['ema200'] = self.clean_series(ta.ema(df['c'], length=200))
                df['rsi'] = self.clean_series(ta.rsi(df['c'], length=14))
                last = df.iloc[-1]
                
                direction = "BULLISH 📈" if last['c'] > last['ema200'] else "BEARISH 📉"
                self.deep_analysis_report = (
                    f"[bold yellow]🛰️ BTC 4H STATUS[/]\n"
                    f"Price: ${last['c']:,.2f} | Trend: [bold cyan]{direction}[/]\n"
                    f"RSI: {last['rsi']:.1f} | EMA200: ${last['ema200']:,.2f}"
                )
            except Exception: 
                pass
            time.sleep(30)

    # --------------------------------------------------------------------------
    # TRADING LOGIC WITH TAKE PROFIT
    # --------------------------------------------------------------------------
    def is_on_cooldown(self, symbol):
        """Checks if a symbol is temporarily blacklisted after a trade."""
        now = datetime.now()
        if symbol in self.closed_trades:
            expire = self.closed_trades[symbol] + timedelta(minutes=BLACKLIST_AUTO_RELEASE_MINUTES)
            if now < expire: 
                return True, f"{int((expire-now).total_seconds())}s"
        return False, None

    def execute_trade(self, symbol, side, entry, atr):
        """
        Execute market orders with automatic Take Profit and Stop Loss.
        Uses ATR for dynamic TP/SL distance calculation.
        """
        try:
            # Pre-trade Checks
            if self.account_info["active_orders"] >= MAX_ACTIVE_TRADES: 
                self.log("Max trades reached. Skipping...", "WARN")
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
                tp = round(entry + tp_dist, p_prec)
                exit_side = SIDE_SELL
            else:
                sl = round(entry + sl_dist, p_prec)
                tp = round(entry - tp_dist, p_prec)
                exit_side = SIDE_BUY

            # Position Sizing
            risk_amt = self.account_info["total_wallet"] * RISK_PER_TRADE
            qty = round(risk_amt / abs(entry - sl), l_prec)

            if qty < min_qty:
                self.log(f"Qty {qty} too small for {symbol}", "WARN")
                return

            # Execution
            self.log(f"Opening {side} on {symbol} | SL: {sl} TP: {tp}", "TRADE")
            
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
            # Using TAKE_PROFIT_MARKET for guaranteed exit
            self.client.futures_create_order(
                symbol=symbol, side=exit_side,
                type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                stopPrice=tp, closePosition=True
            )
            
            self.log(f"Full Strategy Set: {symbol}", "INFO")

        except Exception as e: 
            self.log(f"Trade Execution Err: {str(e)}", "ERROR")

    # --------------------------------------------------------------------------
    # SCANNER & MONITORING ENGINE
    # --------------------------------------------------------------------------
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
            
            self.log("Pre-loading Market Data...", "INFO")
                    
        except Exception as e: 
            self.log(f"Scanner Init Error: {e}", "ERROR")

        while True:
            start_time = time.time()
            for symbol in self.symbols:
                self.current_scanning = symbol
                try:
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
                    
                    # Simple Signal Example: If Vol Spike + RSI Extreme
                    if self.market_vol_status[symbol] and self.account_info["active_orders"] < MAX_ACTIVE_TRADES:
                        if rsi < 30:
                            self.execute_trade(symbol, SIDE_BUY, df['c'].iloc[-1], atr)
                        elif rsi > 70:
                            self.execute_trade(symbol, SIDE_SELL, df['c'].iloc[-1], atr)

                    time.sleep(0.1) # Prevent Rate Limit
                except Exception: 
                    continue
            self.ping = int((time.time() - start_time) * 1000)
            time.sleep(SCAN_INTERVAL)

    # --------------------------------------------------------------------------
    # RENDER ENGINE (UI)
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
            Layout(name="analysis"),
            Layout(name="wallet")
        )

        with Live(layout, refresh_per_second=2, screen=True):
            while True:
                # --- Header Render ---
                header_text = Text.assemble(
                    ("💎 PUK QUANTUM ULTIMATE ", "bold white"), 
                    ("v4.0.0 (Auto-TP Mode)", "dim cyan"),
                    (" | ", "dim"), 
                    (f"⚡ LATENCY: {self.ping}ms", "green" if self.ping < 1500 else "red")
                )
                layout["header"].update(Panel(Align.center(header_text), subtitle=f"🔍 Core Tracking: {self.current_scanning}", border_style="bright_blue"))

                # --- Market Scanner ---
                m_table = Table(expand=True, box=box.SIMPLE, padding=(0,1))
                m_table.add_column("RANK", justify="center", style="dim")
                m_table.add_column("SYMBOL", style="cyan")
                m_table.add_column("PRICE", justify="right", style="bright_white") 
                m_table.add_column("SCORE", justify="center")
                m_table.add_column("VOL", justify="center")
                m_table.add_column("STATUS")

                for i, s in enumerate(self.symbols[:25]):
                    hold, reason = self.is_on_cooldown(s)
                    price = self.get_symbol_price(s)
                    price_str = f"{price:,.6f}" if price < 1 else f"{price:,.2f}"
                    m_table.add_row(
                        str(i+1), s, price_str, 
                        f"⭐ {self.market_scores.get(s,0)}",
                        "🔥" if self.market_vol_status.get(s) else "☁️", 
                        f"[bold red]{reason}[/]" if hold else "[bold green]READY[/]"
                    )
                layout["market"].update(Panel(m_table, title="📡 MARKET SCANNER", border_style="bright_blue"))

                # --- Vol Spikes ---
                v_table = Table(expand=True, box=box.SIMPLE)
                v_table.add_column("SYMBOL", style="yellow")
                v_table.add_column("VOL RATIO", justify="right", style="bold magenta")
                v_table.add_column("ACTION", justify="center")
                top_spikes = sorted(self.market_vol_ratio.items(), key=lambda x: x[1], reverse=True)[:5]
                for sym, rat in top_spikes:
                    action = "[blink red]‼️ SHOCK[/]" if rat > 3 else "[orange1]📈 SURGE[/]"
                    v_table.add_row(sym, f"{rat:.2f}x", action)
                layout["vol_spike"].update(Panel(v_table, title="⚡ VOLUME SPIKES", border_style="yellow"))

                # --- Active Positions (Enhanced with TP/SL Info) ---
                p_table = Table(expand=True, box=box.SIMPLE)
                p_table.add_column("SYMBOL", style="white", no_wrap=True)
                p_table.add_column("SIDE", justify="center")
                p_table.add_column("ENTRY 🚪", justify="right", style="yellow")
                p_table.add_column("PNL ($)", justify="right")
                p_table.add_column("ROE%", justify="right")

                for p in self.active_positions.copy():
                    pos_amt = float(p.get('positionAmt', 0))
                    if pos_amt == 0: continue
                    
                    side_text = "[bold green]📈 LONG[/]" if pos_amt > 0 else "[bold red]📉 SHORT[/]"
                    entry_price = float(p.get('entryPrice', 0))
                    pnl = float(p.get('unrealizedProfit', 0))
                    margin = float(p.get('initialMargin', 1))
                    roe = (pnl / margin * 100) if margin > 0 else 0
                    
                    p_table.add_row(
                        p['symbol'], 
                        side_text, 
                        f"{entry_price:,.4f}",
                        f"[{'green' if pnl >= 0 else 'red'}]{pnl:+.2f}[/]", 
                        f"[{'green' if roe >= 0 else 'red'}]{roe:+.1f}%[/]"
                    )
                layout["active"].update(Panel(p_table, title="⚔️ ACTIVE TRADES", border_style="green"))

                # --- Analysis & Wallet ---
                layout["analysis"].update(Panel(Align.left(self.deep_analysis_report), title="🛰️ ANALYSIS", border_style="yellow"))
                w_text = Text.assemble(
                    ("EQUITY: ", "white"), (f"${self.account_info['equity']:.2f}\n", "bold cyan"),
                    ("PNL:    ", "white"), (f"${self.account_info['pnl']:+.2f}", "bold green" if self.account_info['pnl'] >= 0 else "bold red")
                )
                layout["wallet"].update(Panel(w_text, title="💠 WALLET", border_style="magenta"))

                # --- Footer ---
                layout["footer"].update(Panel("\n".join(self.logs), title="📡 LOGS", border_style="dim yellow"))
                
                time.sleep(1)

if __name__ == "__main__":
    QuantumUltimateSystem().run()