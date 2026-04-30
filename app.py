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
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TOP_N = 50
LEVERAGE = 28
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
        self.status = "SYSTEM ONLINE"
        self.current_scanning = "INIT"
        
        self.closed_trades = {}      
        self.market_scores = {} 
        self.market_vol_status = {} 
        self.deep_analysis_report = "Analysing Market Data..."
        
        self.account_info = {
            "total_wallet": 0.0, "equity": 0.0, "pnl": 0.0, 
            "active_orders": 0, "last_update": "None"
        }
        self.symbol_info = {}

    def log(self, msg, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        color = {"INFO": "cyan", "TRADE": "bright_green", "ERROR": "bright_red", "WARN": "yellow"}.get(level, "white")
        self.logs.append(f"[{color}][{timestamp}] {msg}[/]")
        if len(self.logs) > 12: self.logs.pop(0)

    def clean_series(self, data):
        if isinstance(data, pd.DataFrame): return data.iloc[:, 0]
        return data

    def update_account_core(self):
        while True:
            try:
                acc = self.client.futures_account()
                self.active_positions = [p for p in acc["positions"] if float(p["positionAmt"]) != 0]
                wallet = float(acc["totalWalletBalance"])
                equity = float(acc["totalMarginBalance"])
                self.account_info.update({
                    "total_wallet": wallet, 
                    "equity": equity, 
                    "pnl": float(acc["totalUnrealizedProfit"]),
                    "active_orders": len(self.active_positions),
                    "last_update": datetime.now().strftime("%H:%M:%S")
                })
            except Exception as e:
                self.log(f"Account Update Error: {e}", "ERROR")
            time.sleep(SCAN_INTERVAL)

    def update_deep_analysis(self):
        while True:
            try:
                symbol = "BTCUSDT"
                bars = self.client.futures_klines(symbol=symbol, interval="4h", limit=300)
                df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')

                df['ema200'] = self.clean_series(ta.ema(df['c'], length=200))
                df['rsi'] = self.clean_series(ta.rsi(df['c'], length=14))
                df['mfi'] = self.clean_series(ta.mfi(df['h'], df['l'], df['c'], df['v'], length=14))
                
                last = df.iloc[-1]
                
                ema_val = last['ema200'] if pd.notnull(last['ema200']) else last['c']
                rsi_val = last['rsi'] if pd.notnull(last['rsi']) else 50
                mfi_val = last['mfi'] if pd.notnull(last['mfi']) else 50

                score = (2 if last['c'] > ema_val else 0) + (1 if rsi_val > 50 else 0) + (1 if mfi_val > 50 else 0)
                direction = "BULLISH 📈" if score >= 3 else "BEARISH 📉"
                
                self.deep_analysis_report = (
                    f"[bold yellow]BTC 4H ANALYSIS[/] | {datetime.now().strftime('%H:%M:%S')}\n"
                    f"Price: [white]${last['c']:,.2f}[/]\n"
                    f"Trend: [bold cyan]{direction}[/] | Score: [bold]{score}/4[/]\n"
                    f"RSI: {rsi_val:.1f} | MFI: {mfi_val:.1f}\n"
                    f"EMA200: ${ema_val:,.2f}"
                )
                
                fig = go.Figure(data=[go.Candlestick(x=df['timestamp'], open=df['o'], high=df['h'], low=df['l'], close=df['c'])])
                fig.add_trace(go.Scatter(x=df['timestamp'], y=df['ema200'], line=dict(color='orange', width=2), name='EMA 200'))
                
                if len(df) > 50:
                    min_idx = df['l'].tail(50).idxmin()
                    fig.add_shape(type="line", x0=df['timestamp'].loc[min_idx], y0=df['l'].loc[min_idx],
                                  x1=df['timestamp'].iloc[-1], y1=df['l'].iloc[-1],
                                  line=dict(color="cyan", width=2, dash="dash"))
                
                fig.update_layout(template='plotly_dark', xaxis_rangeslider_visible=False)
                fig.write_html("market_trend.html")
                
            except Exception as e:
                self.log(f"Analysis Error: {e}", "ERROR")
            time.sleep(SCAN_INTERVAL)

    def is_on_cooldown(self, symbol):
        now = datetime.now()
        if symbol in self.closed_trades:
            expire = self.closed_trades[symbol] + timedelta(minutes=BLACKLIST_AUTO_RELEASE_MINUTES)
            if now < expire: return True, f"{int((expire-now).total_seconds())}s"
        return False, None

    def execute_trade(self, symbol, side, entry, atr):
        try:
            if self.account_info["active_orders"] >= MAX_ACTIVE_TRADES: return
            s_info = self.symbol_info.get(symbol)
            if not s_info: return

            p_prec, l_prec = s_info["price_precision"], s_info["lot_precision"]
            sl = round(entry - (atr*1.5) if side == SIDE_BUY else entry + (atr*1.5), p_prec)
            risk_amt = self.account_info["total_wallet"] * RISK_PER_TRADE
            qty = round(risk_amt / abs(entry - sl), l_prec)

            self.client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
            self.client.futures_create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
            self.client.futures_create_order(symbol=symbol, side=SIDE_SELL if side == SIDE_BUY else SIDE_BUY,
                                           type=FUTURE_ORDER_TYPE_STOP_MARKET, stopPrice=sl, closePosition=True)
            self.log(f"🚀 {side} {symbol} @ {entry}", "TRADE")
        except Exception as e: self.log(f"Trade Err: {e}", "ERROR")

    def scanner_loop(self):
        tickers = self.client.futures_ticker()
        sorted_pairs = sorted([x for x in tickers if x["symbol"].endswith("USDT")], key=lambda x: float(x["quoteVolume"]), reverse=True)
        self.symbols = [x["symbol"] for x in sorted_pairs[:TOP_N]]
        
        ex_info = self.client.futures_exchange_info()
        for s in ex_info['symbols']:
            if s['symbol'] in self.symbols:
                lot_f = next(f for f in s['filters'] if f['filterType']=='LOT_SIZE')
                price_f = next(f for f in s['filters'] if f['filterType']=='PRICE_FILTER')
                self.symbol_info[s['symbol']] = {
                    "lot_precision": len(lot_f['stepSize'].rstrip('0').split('.')[1]) if '.' in lot_f['stepSize'] else 0,
                    "price_precision": len(price_f['tickSize'].rstrip('0').split('.')[1]) if '.' in price_f['tickSize'] else 0,
                }

        while True:
            start_time = time.time()
            for symbol in self.symbols:
                self.current_scanning = symbol
                try:
                    bars = self.client.futures_klines(symbol=symbol, interval="15m", limit=200)
                    df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                    
                    rsi = ta.rsi(df['c']).iloc[-1]
                    ema200 = ta.ema(df['c'], length=200).iloc[-1]
                    price = df['c'].iloc[-1]
                    vol_sma = ta.sma(df['v'], length=20).iloc[-1]
                    vol_spike = df['v'].iloc[-1] > (vol_sma * VOL_MULTIPLIER)
                    
                    if pd.notnull(rsi):
                        self.market_scores[symbol] = 3 if (rsi < 35 or rsi > 65) else 1
                        self.market_vol_status[symbol] = vol_spike

                        on_hold, _ = self.is_on_cooldown(symbol)
                        if not on_hold and vol_spike and not any(p['symbol']==symbol for p in self.active_positions):
                            atr = ta.atr(df['h'],df['l'],df['c']).iloc[-1]
                            if pd.notnull(ema200) and pd.notnull(atr):
                                if rsi < 35 and price > ema200: self.execute_trade(symbol, SIDE_BUY, price, atr)
                                elif rsi > 65 and price < ema200: self.execute_trade(symbol, SIDE_SELL, price, atr)
                except: continue
            
            self.current_scanning = "IDLE"
            self.ping = int((time.time() - start_time) * 1000)
            time.sleep(SCAN_INTERVAL)

    def run(self):
        threading.Thread(target=self.update_account_core, daemon=True).start()
        threading.Thread(target=self.update_deep_analysis, daemon=True).start()
        threading.Thread(target=self.scanner_loop, daemon=True).start()
        
        layout = Layout()
        layout.split_column(Layout(name="header", size=4), Layout(name="main", ratio=1), Layout(name="footer", size=10))
        layout["main"].split_row(Layout(name="left", ratio=1), Layout(name="right", ratio=1))
        layout["left"].split_column(Layout(name="market", ratio=1), Layout(name="active", ratio=1))
        layout["right"].split_column(Layout(name="analysis", ratio=1), Layout(name="wallet", size=12))

        with Live(layout, refresh_per_second=4, screen=True):
            while True:
                header_text = Text.assemble(("🚀 QUANTUM ULTIMATE HYBRID", "bold white"), (" | ", "dim"), 
                                            (f"REFRESH: {SCAN_INTERVAL}s", "yellow"), (" | ", "dim"), (f"PING: {self.ping}ms", "cyan"))
                layout["header"].update(Panel(Align.center(header_text), subtitle=f"Scanning: {self.current_scanning}", border_style="blue", box=box.DOUBLE))

                m_table = Table(expand=True, box=box.SIMPLE)
                m_table.add_column("SYM", style="cyan"); m_table.add_column("SCORE"); m_table.add_column("VOL"); m_table.add_column("STATUS")
                for s in self.symbols[:8]:
                    hold, reason = self.is_on_cooldown(s)
                    m_table.add_row(s, f"{self.market_scores.get(s,0)}/3", "🔥" if self.market_vol_status.get(s) else "☁️", f"[red]{reason}[/]" if hold else "[green]READY[/]")
                layout["market"].update(Panel(m_table, title="📡 TOP VOLUME SCANNER"))

                p_table = Table(expand=True, box=box.SIMPLE)
                p_table.add_column("SYMBOL"); p_table.add_column("PnL ($)"); p_table.add_column("ROE%", justify="right")
                for p in self.active_positions:
                    pnl = float(p.get('unrealizedProfit', 0))
                    margin = float(p.get('initialMargin', 1))
                    roe = (pnl / margin * 100) if margin > 0 else 0
                    p_table.add_row(p['symbol'], f"[{'green' if pnl >= 0 else 'red'}]{pnl:+.2f}[/]", f"[{'green' if roe >= 0 else 'red'}]{roe:+.1f}%[/]")
                layout["active"].update(Panel(p_table, title="⚔️ LIVE POSITIONS", border_style="green"))

                layout["analysis"].update(Panel(Align.left(self.deep_analysis_report), title="🛰️ BTC INTELLIGENCE", border_style="yellow"))

                w_text = Text.assemble(("EQUITY: ", "white"), (f"${self.account_info['equity']:.2f}\n", "bold cyan"),
                                       ("PNL:    ", "white"), (f"${self.account_info['pnl']:+.2f}\n", "bold green" if self.account_info['pnl'] >= 0 else "bold red"),
                                       (f"UPDATED: {self.account_info['last_update']}", "dim"))
                bl_list = [f"{s}({self.is_on_cooldown(s)[1]})" for s in list(self.closed_trades.keys())[:3] if self.is_on_cooldown(s)[0]]
                layout["wallet"].update(Panel(Group(w_text, Text("\n[dim]BLACKLIST:[/]\n"), Text(" | ".join(bl_list) if bl_list else "None", style="yellow")), title="💠 CORE WALLET", border_style="magenta"))

                layout["footer"].update(Panel("\n".join(self.logs), title="📡 SYSTEM LOGS", border_style="dim yellow"))
                time.sleep(0.25)

if __name__ == "__main__":
    QuantumUltimateSystem().run()