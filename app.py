import os
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import pandas_ta as ta
import requests
from binance.client import Client
from binance.enums import *
from dotenv import load_dotenv
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
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
RISK_PER_TRADE = 0.05 
RR_RATIO = 2.0
MAX_ACTIVE_TRADES = 4
GLOBAL_TP_TARGET = 20.0 
MAX_DRAWDOWN_LIMIT = -20.0 
SCAN_INTERVAL = 30 
TRAILING_STOP_CALLBACK = 0.03 
COOLDOWN_MINUTES = 15 
BLACKLIST_AUTO_RELEASE_MINUTES = 30 

class QuantumProEnhanced:
    def __init__(self):
        self.client = Client(API_KEY, API_SECRET)
        self.symbols = []
        self.active_positions = []
        self.logs = []
        self.ping = 0
        self.status = "SYSTEM ONLINE"
        self.current_scanning = "None"
        self.goal_reached = False
        self.circuit_break = False
        
        # ระบบคัดกรองเหรียญ
        self.closed_trades = {}      
        self.cooldown_dict = {}      
        self.bot_start_time = int(time.time() * 1000)
        
        self.account_info = {
            "total_wallet": 0.0,
            "available": 0.0,
            "equity": 0.0,
            "pnl": 0.0,
            "active_orders": 0,
            "drawdown": 0.0
        }
        self.symbol_info = {}
        self.console = Console()

    def log(self, msg, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        color = {"INFO": "cyan", "TRADE": "bright_green", "ERROR": "bright_red", "WARN": "yellow"}.get(level, "white")
        log_entry = f"[{color}][{timestamp}] {msg}[/]"
        self.logs.append(log_entry)
        if len(self.logs) > 15: 
            self.logs.pop(0)
        with open("bot_history.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()} | {level} | {msg}\n")

    def notify(self, msg):
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": f"🚀 QUANTUM PRO: {msg}"})
            except: 
                pass

    def _precision(self, step):
        return len(step.rstrip("0").split(".")[1]) if "." in step else 0

    def check_trade_history(self):
        """ ตรวจสอบการปิดออเดอร์เพื่ออัปเดต Blacklist และ Cooldown """
        try:
            trades = self.client.futures_account_trades(limit=50, startTime=self.bot_start_time)
            for t in trades:
                symbol = t['symbol']
                realized_pnl = float(t['realizedPnl'])
                
                if realized_pnl != 0:
                    if realized_pnl < 0:
                        if symbol not in self.cooldown_dict:
                            self.log(f"📉 {symbol} Loss detected. Cooldown active.", "WARN")
                        self.cooldown_dict[symbol] = datetime.now()
                    
                    if symbol not in self.closed_trades:
                        self.log(f"🚫 {symbol} closed. Blacklisted for {BLACKLIST_AUTO_RELEASE_MINUTES}m.", "WARN")
                    self.closed_trades[symbol] = datetime.now()
        except Exception as e:
            self.log(f"History check error: {e}", "ERROR")

    def is_on_cooldown(self, symbol):
        """ เช็คการหมดอายุของ Blacklist และ Cooldown """
        now = datetime.now()
        
        # Blacklist 30 นาที
        if symbol in self.closed_trades:
            close_time = self.closed_trades[symbol]
            if now < close_time + timedelta(minutes=BLACKLIST_AUTO_RELEASE_MINUTES):
                return True, "IN_BLACKLIST"
            else:
                del self.closed_trades[symbol]
                self.log(f"🔓 {symbol} Blacklist expired.", "INFO")
        
        # Cooldown หลังแพ้ 15 นาที
        if symbol in self.cooldown_dict:
            last_loss_time = self.cooldown_dict[symbol]
            if now < last_loss_time + timedelta(minutes=COOLDOWN_MINUTES):
                return True, "COOLDOWN"
            else:
                del self.cooldown_dict[symbol]
        
        return False, None

    def update_symbols(self):
        try:
            tickers = self.client.futures_ticker()
            usdt_pairs = [x for x in tickers if x["symbol"].endswith("USDT")]
            sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True)
            self.symbols = [x["symbol"] for x in sorted_pairs[:TOP_N]]

            info = self.client.futures_exchange_info()
            for symbol in self.symbols:
                s_info = next((s for s in info["symbols"] if s["symbol"] == symbol), None)
                if s_info:
                    lot_filter = next(f for f in s_info["filters"] if f["filterType"] == "LOT_SIZE")
                    price_filter = next(f for f in s_info["filters"] if f["filterType"] == "PRICE_FILTER")
                    min_notional = next(f for f in s_info["filters"] if f["filterType"] == "MIN_NOTIONAL")
                    self.symbol_info[symbol] = {
                        "lot_precision": self._precision(lot_filter["stepSize"]),
                        "price_precision": self._precision(price_filter["tickSize"]),
                        "min_notional": float(min_notional.get("notional", 5.0))
                    }
            self.log("✅ Symbols & Precision Updated", "INFO")
        except Exception as e:
            self.log(f"Update symbols error: {e}", "ERROR")

    def execute_trade(self, symbol, side, entry, atr):
        if self.goal_reached or self.circuit_break: 
            return
        
        on_hold, reason = self.is_on_cooldown(symbol)
        if on_hold:
            return

        try:
            if self.account_info["active_orders"] >= MAX_ACTIVE_TRADES: 
                return

            s_info = self.symbol_info.get(symbol)
            if not s_info:
                return

            p_prec = s_info["price_precision"]
            l_prec = s_info["lot_precision"]

            sl_dist = atr * 1.5
            sl = round(entry - sl_dist if side == SIDE_BUY else entry + sl_dist, p_prec)
            risk_amt = self.account_info["total_wallet"] * RISK_PER_TRADE
            qty = round(risk_amt / abs(entry - sl), l_prec)

            if (qty * entry) < s_info["min_notional"]:
                qty = round((s_info["min_notional"] + 1) / entry, l_prec)

            # ส่งออเดอร์
            self.client.futures_create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=qty)
            
            # Stop Loss
            self.client.futures_create_order(
                symbol=symbol, 
                side=SIDE_SELL if side == SIDE_BUY else SIDE_BUY,
                type=FUTURE_ORDER_TYPE_STOP_MARKET, 
                stopPrice=sl, 
                closePosition=True
            )
            
            # Trailing Stop
            self.client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if side == SIDE_BUY else SIDE_BUY,
                type="TRAILING_STOP_MARKET",
                callbackRate=TRAILING_STOP_CALLBACK * 100, 
                quantity=qty,
                reduceOnly=True
            )

            msg = f"Entered {symbol} {side} @ {entry:.4f}"
            self.log(f"🚀 {msg}", "TRADE")
            self.notify(msg)

        except Exception as e:
            self.log(f"Trade execution error {symbol}: {e}", "ERROR")

    def data_loop(self):
        self.update_symbols()
        while True:
            start = time.time()
            try:
                account = self.client.futures_account()
                self.active_positions = [p for p in account["positions"] if float(p["positionAmt"]) != 0]
                
                self.check_trade_history()

                total_pnl = float(account["totalUnrealizedProfit"])
                wallet_bal = float(account["totalWalletBalance"])
                equity = float(account["totalMarginBalance"])

                self.account_info.update({
                    "total_wallet": wallet_bal,
                    "available": float(account["availableBalance"]),
                    "equity": equity,
                    "pnl": total_pnl,
                    "active_orders": len(self.active_positions),
                    "drawdown": ((equity - wallet_bal) / wallet_bal * 100) if wallet_bal > 0 else 0
                })

                if total_pnl >= GLOBAL_TP_TARGET:
                    self.goal_reached = True
                    self.status = "🎯 TARGET REACHED - MISSION COMPLETE"
                if total_pnl <= MAX_DRAWDOWN_LIMIT:
                    self.circuit_break = True
                    self.status = "🛑 CIRCUIT BREAKER ACTIVATED"

                for symbol in self.symbols:
                    if self.goal_reached or self.circuit_break: 
                        break
                    
                    on_hold, _ = self.is_on_cooldown(symbol)
                    if on_hold: 
                        continue

                    self.current_scanning = symbol
                    try:
                        bars = self.client.futures_klines(symbol=symbol, interval=KLINE_INTERVAL_15MINUTE, limit=100)
                        df = pd.DataFrame(bars, columns=['t','o','h','l','c','v','ct','qv','tr','tb','tq','ig']).astype(float)
                        
                        rsi = ta.rsi(df['c']).iloc[-1] if not ta.rsi(df['c']).empty else 50
                        ema200 = ta.ema(df['c'], length=200).iloc[-1] if not ta.ema(df['c'], length=200).empty else 0
                        atr = ta.atr(df['h'], df['l'], df['c']).iloc[-1] if not ta.atr(df['h'], df['l'], df['c']).empty else 0
                        price = df['c'].iloc[-1]

                        b_score = s_score = 0
                        if price > ema200 and ema200 != 0: b_score += 1
                        else: s_score += 1
                        if rsi < 30: b_score += 2
                        if rsi > 70: s_score += 2

                        side = None
                        if b_score >= 3: 
                            side = SIDE_BUY
                        elif s_score >= 3: 
                            side = SIDE_SELL

                        is_in_trade = any(p['symbol'] == symbol for p in self.active_positions)
                        if side and not is_in_trade:
                            self.execute_trade(symbol, side, price, atr if atr > 0 else price * 0.02)

                    except: 
                        continue

                self.current_scanning = "IDLE"
                if not (self.goal_reached or self.circuit_break): 
                    self.status = "🟢 QUANTUM DRIVE ACTIVE"

            except Exception as e:
                self.status = "🔴 SYSTEM ERROR"
                self.log(f"Main loop error: {e}", "ERROR")

            self.ping = int((time.time() - start) * 1000)
            time.sleep(SCAN_INTERVAL)

    def run(self):
        threading.Thread(target=self.data_loop, daemon=True).start()

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=10)
        )

        layout["main"].split_row(
            Layout(name="left", ratio=3),
            Layout(name="right", ratio=2)
        )

        layout["left"].split_column(
            Layout(name="market", ratio=1),
            Layout(name="active", ratio=1)
        )

        layout["right"].split_column(
            Layout(name="wallet", size=9),
            Layout(name="system", ratio=1)
        )

        with Live(layout, refresh_per_second=5, screen=True, redirect_stderr=False):
            while True:
                # Header
                header_text = Text.assemble(
                    ("🛰️  ", "bright_cyan"),
                    ("QUANTUM PRO ENHANCED", "bold bright_white"),
                    ("  •  ", "dim"),
                    (self.status, "bold bright_green" if "ACTIVE" in self.status or "ONLINE" in self.status else "bold bright_red"),
                    ("  •  ", "dim"),
                    (f"Ping: {self.ping}ms", "bright_yellow")
                )

                scan_text = f"[blink yellow]🔍 HYPERSCAN: {self.current_scanning}[/]" if self.current_scanning != "IDLE" else "[bright_green]✓ SCAN CYCLE COMPLETE[/]"

                layout["header"].update(
                    Panel(
                        Align.center(header_text),
                        subtitle=scan_text,
                        border_style="bright_blue",
                        title="═ PUK TRADING CORE v2.4 ═",
                        box=box.HEAVY
                    )
                )

                # Market Scan
                m_table = Table(expand=True, box=box.SIMPLE_HEAD, border_style="dim blue")
                m_table.add_column("SYMBOL", style="cyan")
                m_table.add_column("SCORE", justify="center")
                m_table.add_column("STATUS", justify="center")
                
                for s in self.symbols[:12]:
                    on_hold, reason = self.is_on_cooldown(s)
                    status = f"[bright_red]{reason}[/]" if on_hold else "[bright_green]NOMINAL[/]"
                    m_table.add_row(s, "—", status)

                layout["market"].update(
                    Panel(m_table, title="📡 MARKET HYPERSCAN", border_style="bright_cyan", box=box.ROUNDED)
                )

                # Active Positions
                p_table = Table(expand=True, box=box.SIMPLE_HEAD, border_style="bright_green")
                p_table.add_column("SYMBOL", style="white")
                p_table.add_column("SIZE", justify="right")
                p_table.add_column("PnL (USDT)", justify="right")
                
                for p in self.active_positions:
                    pnl = float(p.get('unrealizedProfit', 0))
                    color = "bright_green" if pnl >= 0 else "bright_red"
                    p_table.add_row(p['symbol'], p['positionAmt'], f"[{color}]{pnl:+.2f}[/]")

                layout["active"].update(
                    Panel(p_table, title="⚔️ ACTIVE WARP POSITIONS", border_style="bright_green", box=box.ROUNDED)
                )

                # Wallet Monitor
                equity_pct = (self.account_info['pnl'] / self.account_info['total_wallet'] * 100) if self.account_info['total_wallet'] > 0 else 0

                wallet_content = Table.grid(expand=True, padding=(0, 2))
                wallet_content.add_row("TOTAL EQUITY", f"[bold bright_cyan]${self.account_info['equity']:.2f}[/]")
                wallet_content.add_row("UNREALIZED PnL", f"[{'bright_green' if self.account_info['pnl'] >= 0 else 'bright_red'}]{self.account_info['pnl']:+.2f}[/]")
                wallet_content.add_row("DRAWDOWN", f"[{'bright_red' if self.account_info['drawdown'] < -5 else 'yellow'}]{self.account_info['drawdown']:.1f}%[/]")
                wallet_content.add_row("ACTIVE TRADES", f"[bright_magenta]{self.account_info['active_orders']}/{MAX_ACTIVE_TRADES}[/]")
                wallet_content.add_row("BLACKLISTED", f"[red]{len(self.closed_trades)}[/] symbols")

                # Equity Health Bar
                progress = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=35, style="bright_blue"),
                    TextColumn("{task.percentage:>3.0f}%"),
                    expand=False
                )
                task_id = progress.add_task("EQUITY INTEGRITY", total=100, completed=min(100, max(0, 100 + equity_pct)))

                layout["wallet"].update(
                    Panel(wallet_content, title="💠 WALLET CORE", border_style="magenta", box=box.ROUNDED)
                )
                layout["system"].update(
                    Panel(progress, title="SYSTEM STATUS", border_style="blue", box=box.ROUNDED)
                )

                # Footer Logs
                log_content = "\n".join(self.logs) if self.logs else "[dim]Waiting for neural signals...[/]"
                layout["footer"].update(
                    Panel(
                        Text.from_markup(log_content),
                        title="📡 NEURAL LOG TRANSMISSION",
                        border_style="bright_yellow",
                        box=box.SQUARE
                    )
                )

                time.sleep(0.25)


if __name__ == "__main__":
    QuantumProEnhanced().run()