import time
import sqlite3
import random
import math
import datetime
from typing import List, Any, Optional
from config import DB_PATH

def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def black_scholes(s: float, k: float, t: float, r: float, sigma: float, is_call: bool) -> float:
    if t <= 0.0:
        return max(s - k, 0.0) if is_call else max(k - s, 0.0)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    if is_call:
        return s * normal_cdf(d1) - k * math.exp(-r * t) * normal_cdf(d2)
    else:
        return k * math.exp(-r * t) * normal_cdf(-d2) - s * normal_cdf(-d1)

def parse_option_symbol(symbol: str) -> Optional[tuple]:
    if not symbol.startswith("NSE:NIFTY"):
        return None
    remaining = symbol[9:]
    if len(remaining) < 7:
        return None
    details = remaining[5:]
    is_call = details.endswith("CE")
    is_put = details.endswith("PE")
    if not is_call and not is_put:
        return None
    try:
        strike = float(details[:-2])
        return strike, is_call
    except ValueError:
        return None

def get_nifty_expiry(dt_date) -> datetime.date:
    # Find next Thursday (weekday = 3)
    days_ahead = 3 - dt_date.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return dt_date + datetime.timedelta(days=days_ahead)

def generate_mock_candles(symbol: str, range_from: str, range_to: str) -> List[List[Any]]:
    date_from = datetime.datetime.strptime(range_from, "%Y-%m-%d").date()
    date_to = datetime.datetime.strptime(range_to, "%Y-%m-%d").date()
    
    is_vix = "VIX" in symbol
    is_option = "CE" in symbol or "PE" in symbol
    
    # If it is an option, we want to align it with Nifty spot index candles
    if is_option:
        parsed = parse_option_symbol(symbol)
        if parsed is not None:
            strike, is_call = parsed
            
            # Fetch or generate Nifty spot candles first
            spot_symbol = "NSE:NIFTY50-INDEX"
            spot_candles_map = {}
            
            # Check SQLite DB first
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                epoch_from = int(datetime.datetime.combine(date_from, datetime.time.min).timestamp())
                epoch_to = int(datetime.datetime.combine(date_to, datetime.time.max).timestamp())
                cursor.execute(
                    "SELECT timestamp, open, high, low, close, volume FROM historical_candles WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
                    (spot_symbol, epoch_from, epoch_to)
                )
                rows = cursor.fetchall()
                conn.close()
                for r in rows:
                    spot_candles_map[r[0]] = (r[1], r[2], r[3], r[4], r[5])
            except Exception as e:
                print(f"SQLite reading spot for option generation failed: {e}")
            
            # If not in database, generate spot candles recursively
            if not spot_candles_map:
                print(f"Generating spot index candles first to calculate options candles for {symbol}...")
                spot_candles = generate_mock_candles(spot_symbol, range_from, range_to)
                # Save generated spot candles to DB so we keep them persistent
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    for c in spot_candles:
                        cursor.execute(
                            "INSERT OR REPLACE INTO historical_candles (symbol, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (spot_symbol, int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), int(c[5]))
                        )
                    conn.commit()
                    conn.close()
                except Exception as e:
                    print(f"SQLite saving spot for option generation failed: {e}")
                    
                for c in spot_candles:
                    spot_candles_map[int(c[0])] = (c[1], c[2], c[3], c[4], c[5])
            
            # Now, construct option candles based on spot candles
            candles = []
            for ts, (s_open, s_high, s_low, s_close, s_vol) in sorted(spot_candles_map.items()):
                dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                expiry_date = get_nifty_expiry(dt.date())
                expiry_datetime = datetime.datetime.combine(expiry_date, datetime.time(15, 30, 0), tzinfo=datetime.timezone.utc)
                diff_sec = max(0.0, (expiry_datetime - dt).total_seconds())
                t_years = diff_sec / (365.0 * 24.0 * 3600.0)
                
                # Spot index vs VIX
                # Let's read VIX if available, else fallback to 15.0
                vix_val = 15.0
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT close FROM historical_candles WHERE symbol = 'NSE:INDIAVIX-INDEX' AND timestamp = ?",
                        (ts,)
                    )
                    v_row = cursor.fetchone()
                    conn.close()
                    if v_row:
                        vix_val = v_row[0]
                except Exception:
                    pass
                    
                sigma = vix_val / 100.0
                r = 0.07
                
                o_open = max(0.05, black_scholes(s_open, strike, t_years, r, sigma, is_call))
                o_close = max(0.05, black_scholes(s_close, strike, t_years, r, sigma, is_call))
                
                if is_call:
                    o_high = max(o_open, o_close, black_scholes(s_high, strike, t_years, r, sigma, is_call))
                    o_low = max(0.05, min(o_open, o_close, black_scholes(s_low, strike, t_years, r, sigma, is_call)))
                else:
                    o_high = max(o_open, o_close, black_scholes(s_low, strike, t_years, r, sigma, is_call))
                    o_low = max(0.05, min(o_open, o_close, black_scholes(s_high, strike, t_years, r, sigma, is_call)))
                    
                # Add tiny random variation for bid/ask spread realistic noise
                o_open = round(o_open, 2)
                o_high = round(o_high, 2)
                o_low = round(o_low, 2)
                o_close = round(o_close, 2)
                
                # Option volume is normally a fraction of spot volume
                o_vol = int(s_vol * 0.1)
                candles.append([ts, o_open, o_high, o_low, o_close, o_vol])
                
            return candles

    # Simple hash-based seed
    seed_val = 0
    for char in symbol:
        seed_val = (seed_val * 31 + ord(char)) & 0xFFFFFFFF
    for char in range_from:
        seed_val = (seed_val * 31 + ord(char)) & 0xFFFFFFFF
    random.seed(seed_val)
    
    if is_vix:
        base_price = 15.0
    else:
        base_price = 24000.0
        
    price = base_price
    candles = []
    
    current_date = date_from
    one_day = datetime.timedelta(days=1)
    
    while current_date <= date_to:
        if current_date.weekday() < 5:
            for hour in range(9, 16):
                start_min = 15 if hour == 9 else 0
                end_min = 60 if hour < 15 else 31
                
                for minute in range(start_min, end_min):
                    dt_time = datetime.datetime.combine(current_date, datetime.time(hour, minute, 0))
                    ts = int(dt_time.timestamp())
                    
                    if is_vix:
                        # VIX random walk around 15.0
                        noise = random.normalvariate(0, 0.15)
                        price = max(8.0, min(35.0, price + noise))
                        c_open = price
                        c_close = price + random.normalvariate(0, 0.05)
                        c_high = max(c_open, c_close) + random.uniform(0, 0.08)
                        c_low = min(c_open, c_close) - random.uniform(0, 0.08)
                        volume = int(random.uniform(500, 2000))
                    else:
                        time_factor = (hour - 9) * 60 + minute
                        wave = math.sin(time_factor / 30.0) * 15.0
                        trend = time_factor * 0.1
                        noise = random.normalvariate(0, 8.0)
                        
                        price = base_price + wave + trend + noise
                        c_open = price
                        c_close = price + random.normalvariate(2.0, 4.0)
                        c_high = max(c_open, c_close) + random.uniform(0, 6.0)
                        c_low = min(c_open, c_close) - random.uniform(0, 6.0)
                        volume = int(random.uniform(2000, 15000))
                        
                    candles.append([ts, c_open, c_high, c_low, c_close, volume])
                    
        current_date += one_day
        
    return candles
