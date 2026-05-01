import os
import threading
import time
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

# =========================
# CONFIG & ENHANCED SETTINGS
# =========================
load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

TOP_N = 50
LEVERAGE = 10
RISK_PER_TRADE = 0.03 
MAX_ACTIVE_TRADES = 6      
SCAN_INTERVAL = 15 
BLACKLIST_AUTO_RELEASE_MINUTES = 10 
VOL_MULTIPLIER = 1.1      

class QuantumUltimateSystem:
    def __init__(self):
        self.client = Client(API_KEY, API_SECRET)
        self.symbols = []
        self.active_positions = []
        self.logs = []
        self.ping = 0
        self.current_scanning = "INIT"
        
        self.closed_trades = {}      
        self.market_scores = {} 
        self.market_vol_status = {} 
        self.market_vol_ratio = {} # เก็บค่า Vol Ratio สำหรับจัดอันดับ
        self.price_cache = {}
        self.deep_analysis_report = "Analysing Market Data..."
        
        self.account_info = {
            "total_wallet": 0.0, "equity": 0.0, "pnl": 0.0, 
            "active_orders": 0, "last_update": "None"
        }
        self.symbol_info = {}
        self.twm = None 

    def log(self, msg, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        emoji = {"INFO": "ℹ️", "TRADE": "🚀", "ERROR": "❌", "WARN": "⚠️"}.get(level, "🔹")
        color = {"INFO": "cyan", "TRADE": "bright_green", "ERROR": "bright_red", "WARN": "yellow"}.get(level, "white")
        self.logs.append(f"[{color}]{emoji} [{timestamp}] {msg}[/]")
        if len(self.logs) > 12: self.logs.pop(0)

    def get_symbol_price(self, symbol):
        price = self.price_cache.get(symbol, 0.0)
        if price == 0.0:
            try:
                res = self.client.futures_symbol_ticker(symbol=symbol)
                price = float(res['price'])
                self.price_cache[symbol] = price
            except: return 0.0
        return price

    def start_ticker_websocket(self):
        try:
            self.twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
            self.twm.start()
            def handle_socket_message(msg):
                if isinstance(msg, list):
                    for ticker in msg:
                        self.price_cache[ticker['s']] = float(ticker['c'])
            self.twm.start_miniticker_socket(callback=handle_socket_message)
        except Exception as e:
            self.log(f"WS Error: {e}", "ERROR")

    def update_account_core(self):
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
            except: pass
            time.sleep(SCAN_INTERVAL)

    def scanner_loop(self):
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
                    }
        except: pass

        while True:
            start_time = time.time()
            for symbol in self.symbols:
                self.current_scanning = symbol
                try:
                    bars = self.client.futures_klines(symbol=symbol, interval="15m", limit=50)
                    df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                    
                    # คำนวณ Volume Spike Ratio
                    vol_sma = ta.sma(df['v'], length=20).iloc[-1]
                    current_vol = df['v'].iloc[-1]
                    ratio = (current_vol / vol_sma) if vol_sma > 0 else 0
                    
                    self.market_vol_ratio[symbol] = ratio
                    self.market_vol_status[symbol] = ratio > VOL_MULTIPLIER
                    
                    # RSI Score
                    rsi = ta.rsi(df['c']).iloc[-1]
                    self.market_scores[symbol] = 3 if (rsi < 35 or rsi > 65) else 1
                    
                    time.sleep(0.1)
                except: continue
            self.ping = int((time.time() - start_time) * 1000)
            time.sleep(SCAN_INTERVAL)

    def run(self):
        threading.Thread(target=self.start_ticker_websocket, daemon=True).start()
        threading.Thread(target=self.update_account_core, daemon=True).start()
        threading.Thread(target=self.scanner_loop, daemon=True).start()
        
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4), 
            Layout(name="main", ratio=1), 
            Layout(name="footer", size=8)
        )
        layout["main"].split_row(
            Layout(name="left", ratio=1), 
            Layout(name="right", ratio=1)
        )
        layout["left"].split_column(Layout(name="market", ratio=1))
        layout["right"].split_column(
            Layout(name="vol_spike", size=10), # ช่องใหม่สำหรับ Vol Spike
            Layout(name="active", size=10),
            Layout(name="wallet", size=6)
        )

        with Live(layout, refresh_per_second=2, screen=True):
            while True:
                # Header
                header_text = Text.assemble(("💎 QUANTUM ULTIMATE", "bold white"), (" | ", "dim"), (f"⚡ PING: {self.ping}ms", "cyan"))
                layout["header"].update(Panel(Align.center(header_text), subtitle=f"🔍 Scanning: {self.current_scanning}", border_style="bright_blue"))

                # 1. TOP 20 VOLUME SCANNER
                m_table = Table(expand=True, box=box.SIMPLE)
                m_table.add_column("RANK", justify="center", style="dim")
                m_table.add_column("SYMBOL", style="cyan")
                m_table.add_column("PRICE", justify="right")
                m_table.add_column("SCORE", justify="center")
                m_table.add_column("VOL", justify="center")

                for i, s in enumerate(self.symbols[:25]):
                    price = self.get_symbol_price(s)
                    m_table.add_row(str(i+1), s, f"{price:,.4f}", f"⭐ {self.market_scores.get(s,0)}", "🔥" if self.market_vol_status.get(s) else "☁️")
                layout["market"].update(Panel(m_table, title="📡 TOP 20 VOLUME", border_style="bright_blue"))

                # 2. ⚡ TOP 5 VOLUME SPIKE (เหรียญที่ Vol พุ่งผิดปกติ)
                v_table = Table(expand=True, box=box.SIMPLE)
                v_table.add_column("SYMBOL", style="yellow")
                v_table.add_column("VOL RATIO", justify="right", style="bold magenta")
                v_table.add_column("STATUS", justify="center")

                # จัดอันดับตาม vol_ratio
                top_spikes = sorted(self.market_vol_ratio.items(), key=lambda x: x[1], reverse=True)[:5]
                for sym, rat in top_spikes:
                    status = "[blink red]‼️ SHOCK[/]" if rat > 3 else "[orange1]📈 SURGE[/]"
                    v_table.add_row(sym, f"{rat:.2f}x", status)
                layout["vol_spike"].update(Panel(v_table, title="⚡ TOP 5 VOL SPIKE (15M)", border_style="yellow"))

                # --- Positions Table ---
                p_table = Table(expand=True, box=box.SIMPLE)
                p_table.add_column("SYMBOL", style="white", no_wrap=True)
                p_table.add_column("SIDE", justify="center")
                p_table.add_column("ENTRY 🚪", justify="right", style="yellow")
                p_table.add_column("LAST 🏷️", justify="right", style="bright_white") # เพิ่มคอลัมน์ราคาปัจจุบัน
                p_table.add_column("PNL ($)", justify="right")
                p_table.add_column("ROE%", justify="right")

                for p in self.active_positions.copy():
                    pos_amt = float(p.get('positionAmt', 0))
                    if pos_amt == 0: continue
                    
                    symbol = p['symbol']
                    side_text = "[bold green]📈 LONG[/]" if pos_amt > 0 else "[bold red]📉 SHORT[/]"
                    entry_price = float(p.get('entryPrice', 0))
                    
                    # ดึงราคาปัจจุบันจาก Cache (WebSocket)
                    last_price = self.get_symbol_price(symbol) 
                    
                    pnl = float(p.get('unrealizedProfit', 0))
                    margin = float(p.get('initialMargin', 1))
                    roe = (pnl / margin * 100) if margin > 0 else 0
                    
                    # จัดรูปแบบทศนิยมให้เหมาะสมกับราคา
                    last_price_str = f"{last_price:,.4f}" if last_price < 1 else f"{last_price:,.2f}"
                    entry_price_str = f"{entry_price:,.4f}" if entry_price < 1 else f"{entry_price:,.2f}"
                    
                    p_table.add_row(
                        symbol, 
                        side_text, 
                        entry_price_str,
                        last_price_str, # แสดงราคาล่าสุด
                        f"[{'green' if pnl >= 0 else 'red'}]{pnl:+.2f}[/]", 
                        f"[{'green' if roe >= 0 else 'red'}]{roe:+.1f}%[/]"
                    )
                layout["active"].update(Panel(p_table, title="⚔️ LIVE POSITIONS", border_style="green"))

                # 4. WALLET
                w_text = Text.assemble(
                    ("💰 EQUITY: ", "white"), (f"${self.account_info['equity']:.2f}\n", "bold cyan"),
                    ("📊 TOTAL PNL: ", "white"), (f"${self.account_info['pnl']:+.2f}\n", "bold green" if self.account_info['pnl'] >= 0 else "bold red"),
                    (f"🕒 UPDATED: {self.account_info['last_update']}", "dim")
                )
                bl_list = [f"{s}({self.is_on_cooldown(s)[1]})" for s in list(self.closed_trades.keys())[:3] if self.is_on_cooldown(s)[0]]
                layout["wallet"].update(Panel(
                    Group(w_text, Text("\n🚫 BLACKLISTED:\n", style="bold red"), Text(" | ".join(bl_list) if bl_list else "None", style="yellow")), 
                    title="💠 CORE WALLET", border_style="magenta"
                ))
                layout["wallet"].update(Panel(w_text, title="💠 WALLET", border_style="magenta"))

                # 5. LOGS
                layout["footer"].update(Panel("\n".join(self.logs), title="📡 LOGS", border_style="dim yellow"))
                
                time.sleep(1)

if __name__ == "__main__":
    QuantumUltimateSystem().run()