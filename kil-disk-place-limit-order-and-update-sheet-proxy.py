from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException, TokenException
import os
import sys
import csv
from urllib.parse import quote
import time
import io
import requests
from datetime import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from dotenv import load_dotenv, dotenv_values
import warnings

# Google Sheets setup
scopes = ["https://www.googleapis.com/auth/spreadsheets"]

# Load environment variables from .env if present
load_dotenv()
# Suppress deprecation warnings from dependencies to keep output clean
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Single dict for config: .env file first, then fill gaps from OS env (Cloud Run console vars)
_env_values = dict(dotenv_values() or {})
for _k in (
    "SHEET_ID",
    "OXYLABS_PROXY_HOST",
    "OXYLABS_PROXY_PORT",
    "OXYLABS_PROXY_USER",
    "OXYLABS_PROXY_PASSWORD",
):
    if not (_env_values.get(_k) or "").strip():
        _v = (os.environ.get(_k) or "").strip()
        if _v:
            _env_values[_k] = _v


def _running_in_cloud():
    """Cloud Run / Cloud Functions (K_SERVICE or FUNCTION_TARGET set)."""
    return bool(os.environ.get("K_SERVICE") or os.environ.get("FUNCTION_TARGET"))

SHEET_ID = _env_values.get("SHEET_ID")
if not SHEET_ID:
    print("SHEET_ID is not set in .env. Please create a .env file with SHEET_ID=<your_sheet_id>.")
    exit(1)


def get_oxylabs_proxies():
    """
    Same proxy dict as KiteConnect uses — all HTTPS to api.kite.trade must go through this.
    Uses module _env_values (same snapshot as SHEET_ID). Required keys, no defaults:
    OXYLABS_PROXY_HOST, OXYLABS_PROXY_PORT, OXYLABS_PROXY_USER, OXYLABS_PROXY_PASSWORD.
    """
    host = (_env_values.get("OXYLABS_PROXY_HOST") or "").strip()
    port = (_env_values.get("OXYLABS_PROXY_PORT") or "").strip()
    user = (_env_values.get("OXYLABS_PROXY_USER") or "").strip()
    password = (_env_values.get("OXYLABS_PROXY_PASSWORD") or "").strip()
    missing = [
        name
        for name, val in (
            ("OXYLABS_PROXY_HOST", host),
            ("OXYLABS_PROXY_PORT", port),
            ("OXYLABS_PROXY_USER", user),
            ("OXYLABS_PROXY_PASSWORD", password),
        )
        if not val
    ]
    if missing:
        raise RuntimeError(
            "Add to .env (no defaults): " + ", ".join(missing)
        )
    auth = f"{quote(user, safe='')}:{quote(password, safe='')}"
    proxy_url = f"http://{auth}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}

# Indian timezone
IST = pytz.timezone('Asia/Kolkata')

def get_indian_time():
    """
    Get current time in Indian Standard Time (IST)
    """
    return datetime.now(IST)

def get_indian_timestamp():
    """
    Get current timestamp in IST format: YYYY-MM-DD HH:MM:SS
    """
    return get_indian_time().strftime('%Y-%m-%d %H:%M:%S')

def get_indian_time_log():
    """
    Get current time in IST format for logging: HH:MM:SS
    """
    return get_indian_time().strftime('%H:%M:%S')

def get_credentials_from_sheet():
    """
    Get API credentials and access token from Google Sheet 'Info'
    """
    try:
        # Initialize Google Sheets API
        creds = Credentials.from_service_account_file(
            'service_account.json',  # You'll need to create this file
            scopes=scopes
        )
        
        client = gspread.authorize(creds)
        
        # Open the spreadsheet and Info sheet
        spreadsheet = client.open_by_key(SHEET_ID)
        info_sheet = spreadsheet.worksheet('Info')
        
        # Read API credentials from B column
        api_key = info_sheet.acell('B1').value  # B1 for api_key
        api_secret = info_sheet.acell('B2').value  # B2 for api_secret
        
        # Read access token from B column (B3)
        access_token = info_sheet.acell('B3').value
        
        # Read order modification threshold from B7 (in minutes)
        threshold_minutes_str = info_sheet.acell('B7').value
        try:
            threshold_minutes = int(float(threshold_minutes_str)) if threshold_minutes_str else 600  # Default 10 hours (600 minutes)
        except (ValueError, TypeError):
            threshold_minutes = 600  # Default 10 hours (600 minutes) if invalid value
            print(f"Invalid threshold value '{threshold_minutes_str}', using default 30 minutes")
        
        print("Successfully loaded credentials and threshold from Google Sheet")
        return api_key, api_secret, access_token, threshold_minutes
        
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        print("Please ensure you have:")
        print("1. service_account.json file in the same directory")
        print(f"2. Google Sheet with ID '{SHEET_ID}' with 'Info' sheet")
        print("3. API credentials in B1, B2, and access token in B3")
        return None, None, None, 600  # Default 10 hours (600 minutes) threshold

# Get credentials from Google Sheet
api_key, api_secret, access_token, threshold_minutes = get_credentials_from_sheet()

if not api_key or not api_secret:
    print("Failed to load API credentials. Exiting...")
    exit()

try:
    _kite_proxies = get_oxylabs_proxies()
except RuntimeError as e:
    print(str(e))
    exit(1)

# All Kite REST calls (orders, quotes, margins, …) use this proxies mapping
kite = KiteConnect(api_key=api_key, proxies=_kite_proxies, timeout=60)

def load_tick_map(api_key: str, access_token: str, proxies: dict):
    """
    Load tick sizes for instruments from Zerodha instruments API.
    Uses the same logic as test-1-zerodha-instrument.py but builds the token
    from the already available api_key and access_token.
    """
    tick_map = {}
    if not api_key or not access_token:
        return tick_map

    url = "https://api.kite.trade/instruments"
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15, proxies=proxies)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            if row.get("exchange") in ["NSE", "NFO", "CDS"]:
                symbol = row.get("tradingsymbol")
                tick_raw = row.get("tick_size")
                if not symbol or tick_raw is None:
                    continue
                try:
                    tick = float(tick_raw)
                    if tick > 0:
                        tick_map[symbol] = tick
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        print(f"Error loading tick map: {e}")
    return tick_map


_tick_map_cache: dict | None = None
_tick_map_token: str | None = None


def _tick_map() -> dict:
    """Lazy load — avoids blocking Cloud Run startup on a large instruments CSV over proxy."""
    global _tick_map_cache, _tick_map_token
    if _tick_map_cache is None or _tick_map_token != access_token:
        _tick_map_cache = load_tick_map(api_key, access_token, _kite_proxies)
        _tick_map_token = access_token
    return _tick_map_cache


def set_access_token_from_sheet():
    """
    Set access token from Google Sheet
    """
    global access_token
    if access_token:
        try:
            kite.set_access_token(access_token)
            kite.profile()
            print("Using access token from Google Sheet.")
            return True
        except Exception:
            print("Access token from sheet is invalid or expired.")
            return False
    return False

if not set_access_token_from_sheet():
    if _running_in_cloud():
        # Cloud Run must bind :8080; no stdin. hello_http reloads Sheet B3 each request.
        print(
            "Warning: Kite access token invalid or missing at startup (Sheet Info → B3). "
            "Service still starts; update B3 with a fresh token from Kite Connect, then "
            "invoke this URL again (no redeploy needed).",
            flush=True,
        )
    else:
        # Local CLI: same as cloud — refresh B3 only; no interactive login (deployment-safe).
        print(
            "Kite access token invalid or missing (Sheet Info → B3). "
            "Paste a fresh access token into B3, then run again.",
            flush=True,
        )
        sys.exit(1)


def place_order_with_kite(kite, symbol, direction, quantity, product=None, limit_price=None):
    # Automatically detect exchange based on symbol
    if sum(1 for char in symbol if char.isdigit()) >= 2:
        # Check if it's CDS (Currency Derivatives) first
        if any(currency in symbol.upper() for currency in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR', 'INR']):
            exchange = "CDS"  # Currency derivatives
            # concise logs: no per-symbol exchange print
        else:
            exchange = "NFO"  # Other derivatives (options/futures)
            # concise logs: no per-symbol exchange print
    else:
        exchange = "NSE"  # No numbers = equity shares
        # concise logs: no per-symbol exchange print
    
    exchanges = {"NSE": kite.EXCHANGE_NSE, "NFO": kite.EXCHANGE_NFO, "CDS": kite.EXCHANGE_CDS}
    directions = {"BUY": kite.TRANSACTION_TYPE_BUY, "SELL": kite.TRANSACTION_TYPE_SELL}
    products = {"CNC": kite.PRODUCT_CNC, "MIS": kite.PRODUCT_MIS, "NRML": kite.PRODUCT_NRML}
    
    # Set default product based on exchange if not specified
    if product is None:
        if exchange == "NSE":
            product = "CNC"  # Cash and Carry for equity shares
        elif exchange in ["NFO", "CDS"]:
            product = "NRML"  # Normal margin for derivatives
        # concise logs: no per-symbol exchange print
    
    # Use provided price or try to get the best price from quotes for LIMIT orders
    best_price = limit_price
    if best_price is None:
        try:
            # Get quote for the symbol
            quote_symbol = f"{exchange}:{symbol}"
            quotes = kite.quote(quote_symbol)
            
            if direction == "BUY":
                # For BUY order, use best bid price (what buyers are willing to pay)
                best_price = quotes[quote_symbol]['depth']['buy'][0]['price']
                # concise logs: no per-symbol exchange print
            else:  # SELL
                # For SELL order, use best ask price (what sellers are asking)
                best_price = quotes[quote_symbol]['depth']['sell'][0]['price']
                # concise logs: no per-symbol exchange print
            
        except Exception as e:
            print(f"Error getting quote for price: {e}")
            # Always return a tuple to avoid unpacking errors upstream
            return None, None
    
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchanges[exchange],
            tradingsymbol=symbol,
            transaction_type=directions[direction],
            quantity=quantity,
            product=products[product],
            order_type=kite.ORDER_TYPE_LIMIT,  # Always LIMIT
            price=best_price,  # Use the fetched price
            validity=kite.VALIDITY_DAY
        )
        # concise logs: no per-symbol exchange print
        return order_id, best_price
    except Exception as e:
        print(f"Order error: {e}")
        return None, None

def calculate_time_difference_minutes(order_time_str, status_time_str):
    """
    Calculate time difference in minutes between order placement and status check
    """
    try:
        # Parse timestamps
        order_time = datetime.strptime(order_time_str, '%Y-%m-%d %H:%M:%S')
        status_time = datetime.strptime(status_time_str, '%Y-%m-%d %H:%M:%S')
        
        # Calculate difference in minutes
        time_diff = (status_time - order_time).total_seconds() / 60
        return time_diff
    except Exception as e:
        print(f"Error calculating time difference: {e}")
        return 0

def modify_order_with_new_price(kite, order_id, symbol, direction, quantity, use_tick_adjustment=False):
    """
    Modify an existing order with new limit price based on current market data
    """
    """
    If use_tick_adjustment is True (column N = "Yes"), adjust the price by the exact
    Zerodha tick size fetched from the instruments API:
      - Use best bid/ask from depth as base price.
      - BUY: base_price + tick_size
      - SELL: base_price - tick_size
    No more 0.01 trial, error parsing, or floor/ceiling alignment logic.
    On success returns (modified_order_id, final_price, None),
    on failure returns (None, None, error_message).
    """
    # Automatically detect exchange based on symbol (same logic as place_order)
    try:
        if sum(1 for char in symbol if char.isdigit()) >= 2:
            if any(currency in symbol.upper() for currency in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR', 'INR']):
                exchange = "CDS"
            else:
                exchange = "NFO"
        else:
            exchange = "NSE"

        # Get current market price as base
        quote_symbol = f"{exchange}:{symbol}"
        quotes = kite.quote(quote_symbol)

        if direction == "BUY":
            base_price = quotes[quote_symbol]['depth']['buy'][0]['price']
        else:  # SELL
            base_price = quotes[quote_symbol]['depth']['sell'][0]['price']
    except Exception as e:
        msg = f"Error getting quote for modification of order {order_id}: {e}"
        print(msg)
        return None, None, msg

    try:
        # Decide final price:
        # - if use_tick_adjustment == False: just use base_price as-is
        # - if use_tick_adjustment == True: move one tick in favour of faster fill using
        #   the exact Zerodha tick size from instruments map (no 0.01 trial or error parsing)
        if use_tick_adjustment:
            tick = _tick_map().get(symbol)
            if tick:
                if direction == "BUY":
                    new_price = base_price + tick
                else:
                    new_price = base_price - tick
            else:
                # If we don't have a tick for this symbol, fall back to base_price
                new_price = base_price
        else:
            new_price = base_price

        modified_order_id = kite.modify_order(
            variety=kite.VARIETY_REGULAR,
            order_id=order_id,
            quantity=quantity,
            price=new_price,
            order_type=kite.ORDER_TYPE_LIMIT,
            validity=kite.VALIDITY_DAY
        )
        print(f"Order {order_id} modified with new price {new_price}")
        return modified_order_id, new_price, None
    except Exception as e:
        error_msg = str(e)
        print(f"Error modifying order {order_id}: {error_msg}")
        return None, None, error_msg

def check_order_status(kite, order_id):
    """
    Check the status of an order using order ID
    Returns the LATEST (LAST) order status or None if error
    """
    try:
        order_history = kite.order_history(order_id)
        if order_history and len(order_history) > 0:
            # Sort by exchange_timestamp to get the most recent status
            # Convert timestamp to datetime for proper sorting
            def get_timestamp(status_item):
                timestamp_str = status_item.get('exchange_timestamp', '')
                if timestamp_str:
                    try:
                        # Parse the timestamp (format: 2024-01-15 14:30:25)
                        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        return datetime.min
                return datetime.min
            
            # Sort by timestamp ascending (oldest first) to get the LAST (most recent) status
            sorted_history = sorted(order_history, key=get_timestamp, reverse=False)
            latest_status = sorted_history[-1]  # LAST (most recent) status
            
            # Keep output clean: no verbose per-status logs
            
            # Get the status and additional details from the LAST entry
            status = latest_status.get('status', 'UNKNOWN')
            filled_quantity = latest_status.get('filled_quantity', 0)
            pending_quantity = latest_status.get('pending_quantity', 0)
            exchange_timestamp = latest_status.get('exchange_timestamp', '')
            
            # Create a more detailed status
            if status == 'COMPLETE':
                return f"COMPLETE ({filled_quantity} filled)"
            elif status == 'OPEN' and pending_quantity > 0:
                return f"OPEN PENDING ({pending_quantity} pending)"
            elif status == 'OPEN':
                return "OPEN"
            elif status == 'CANCELLED':
                return "CANCELLED"
            elif status == 'REJECTED':
                return "REJECTED"
            else:
                return status
                
        return None
    except Exception as e:
        print(f"Error checking order status for {order_id}: {e}")
        return None

def update_order_statuses(kite, sheet_id, threshold_minutes):
    """
    Check and update order statuses for all orders with action_status = "Order_Placed"
    """
    try:
        print(f"[{get_indian_time_log()}] Checking order statuses...", flush=True)
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet('Actions')

        rows = sheet.get_all_values()
        if len(rows) <= 1:
            print("No data rows found for status update.", flush=True)
            return "No data rows found"
        
        updated_count = 0
        error_count = 0
        skipped_terminal_count = 0
        modified_count = 0
        status_updates = []
        
        # Check each row for orders that need status update
        for idx in range(1, len(rows)):
            row = rows[idx]
            
            # Safely access columns with defaults
            action_status = (row[3] or "").strip().upper() if len(row) > 3 else ""
            order_time = (row[4] or "").strip() if len(row) > 4 else ""  # Column E (index 4)
            order_id = (row[6] or "").strip() if len(row) > 6 else ""  # Column G (index 6)
            current_order_status = (row[7] or "").strip().upper() if len(row) > 7 else ""  # Column H (index 7)
            status_timestamp = (row[8] or "").strip() if len(row) > 8 else ""  # Column I (index 8)
            modification_count = (row[9] or "").strip() if len(row) > 9 else ""  # Column J (index 9) - modification count
            modification_limit_prices = (row[10] or "").strip() if len(row) > 10 else ""  # Column K (index 10) - modification limit prices
            modification_time = (row[11] or "").strip() if len(row) > 11 else ""  # Column L (index 11) - modification time
            symbol = (row[0] or "").strip() if len(row) > 0 else ""  # Column A (index 0)
            direction = (row[1] or "").strip().upper() if len(row) > 1 else ""  # Column B (index 1)
            quantity_str = (row[2] or "").strip() if len(row) > 2 else ""  # Column C (index 2)
            cancel_flag = (row[12] or "").strip().upper() if len(row) > 12 else ""  # Column M (index 12)
            cancel_status_aa = (row[26] or "").strip() if len(row) > 26 else ""  # Column AA (index 26)
            modify_flag_n = (row[13] or "").strip().lower() if len(row) > 13 else ""  # Column N (index 13)
            
            # Terminal states that don't need further checking
            terminal_states = ["COMPLETE", "CANCELLED", "REJECTED"]
            
            # Only check status for orders that are placed AND not in terminal state
            if (action_status == "ORDER_PLACED" and order_id and 
                not any(terminal_state in current_order_status for terminal_state in terminal_states)):
                try:
                    # Check if we need to modify the order first
                    should_modify = False
                    if (current_order_status and "OPEN" in current_order_status and 
                        order_time and status_timestamp):
                        time_diff = calculate_time_difference_minutes(order_time, status_timestamp)
                        if time_diff > threshold_minutes:
                            should_modify = True
                            print(f"Order {order_id} exceeded threshold ({time_diff:.1f} > {threshold_minutes} min), modifying...")
                    
                    # Modify order if needed (infinite modifications allowed)
                    if should_modify and symbol and direction and quantity_str:
                        try:
                            quantity = int(float(quantity_str))
                            use_tick_adjustment = (modify_flag_n == "yes")
                            modified_order_id, new_price, error_msg = modify_order_with_new_price(
                                kite,
                                order_id,
                                symbol,
                                direction,
                                quantity,
                                use_tick_adjustment=use_tick_adjustment,
                            )
                            if modified_order_id:
                                modified_count += 1
                                
                                if modification_count and "MODIFIED" in modification_count:
                                    try:
                                        current_mod_count = int(modification_count.split("_")[-1])
                                    except:
                                        current_mod_count = 0
                                else:
                                    current_mod_count = 0

                                new_mod_count = current_mod_count + 1

                                # Decide label based on N column
                                if use_tick_adjustment:
                                    new_mod_label = f"MODIFIED_TICK_{new_mod_count}"
                                else:
                                    new_mod_label = f"MODIFIED_{new_mod_count}"




                                # Get current timestamp for modification
                                mod_timestamp = get_indian_timestamp()
                                
                                # Build modification history with timestamps (append to existing)
                                if modification_time and modification_time.strip():
                                    # Append to existing history
                                    mod_history = f"{modification_time}, {mod_timestamp}"
                                else:
                                    # First modification
                                    mod_history = mod_timestamp
                                
                                # Determine logic tag
                                logic_tag = "T" if use_tick_adjustment else "N"

                                # Price with logic marker
                                price_with_logic = f"{round(new_price, 2)}({logic_tag})"

                                # Build modification limit prices history
                                if modification_limit_prices and modification_limit_prices.strip():
                                    mod_prices_history = f"{modification_limit_prices}, {price_with_logic}"
                                else:
                                    mod_prices_history = price_with_logic
                                
                                # Update the modification tracking columns J, K, and L
                                row_num = idx + 1
                                status_updates.append({
                                    'range': f"J{row_num}:L{row_num}",
                                    'values': [[new_mod_label, mod_prices_history, mod_history]]
                                })
                                
                                print(f"Order {order_id} modified #{new_mod_count} times at {mod_timestamp}")
                            elif error_msg:
                                print(f"Order {order_id} modification failed: {error_msg}")
                        except Exception as e:
                            print(f"Error modifying order {order_id}: {e}")

                    
                    # Check the order status
                    status = check_order_status(kite, order_id)
                    if status:
                        row_num = idx + 1  # 1-based row in sheet
                        timestamp = get_indian_timestamp()
                        # Accumulate updates for batch write
                        status_updates.append({
                            'range': f"H{row_num}:I{row_num}",
                            'values': [[status, timestamp]]
                        })
                        updated_count += 1
                    else:
                        print(f"Could not get status for order {order_id}")
                        error_count += 1
                        
                except Exception as e:
                    print(f"Error updating status for order {order_id}: {e}")
                    error_count += 1
                
                # Add a small delay between API calls
                time.sleep(0.5)
            elif action_status == "ORDER_PLACED" and order_id and any(terminal_state in current_order_status for terminal_state in terminal_states):
                # Skip orders that are already in terminal state
                skipped_terminal_count += 1

        # Perform one batch update for all status rows
        if status_updates:
            try:
                sheet.batch_update(status_updates)
                print(f"Applied {len(status_updates)} status updates in batch.")
            except Exception as e:
                print(f"Batch status update error: {e}")
        
        result = f"status_updated={updated_count}, errors={error_count}, skipped_terminal={skipped_terminal_count}, modified={modified_count}"
        print(f"Status update cycle done: {result}", flush=True)
        return result
        
    except Exception as e:
        print(f"update_order_statuses error: {e}", flush=True)
        return f"Error: {e}"


QUOTE_MAX_INSTRUMENTS_PER_REQUEST = 300  # Kite full quote API limit per request


def _first_depth_price(levels):
    """First bid/ask level with a positive price (Kite uses 0 for empty depth slots)."""
    if not levels or not isinstance(levels, list):
        return None
    for lvl in levels:
        if not isinstance(lvl, dict):
            continue
        p = lvl.get("price")
        try:
            pf = float(p)
            if pf > 0:
                return pf
        except (TypeError, ValueError):
            continue
    return None


def fetch_positions_quotes_via_kite(kite, instruments_list):
    """
    One kite.quote batch per chunk for all instruments (underlyings + row instruments).
    Returns dict: key -> last_price, close (prev day, from ohlc), best_bid, best_offer (depth).
    Covers N, O, P, Q, R without a separate ohlc() call.
    """
    if not instruments_list or not kite:
        return {}
    out = {}
    for start in range(0, len(instruments_list), QUOTE_MAX_INSTRUMENTS_PER_REQUEST):
        chunk = instruments_list[start:start + QUOTE_MAX_INSTRUMENTS_PER_REQUEST]
        if not chunk:
            continue
        try:
            result = kite.quote(chunk)
            data = result if isinstance(result, dict) else {}
            for key, obj in (data or {}).items():
                if not isinstance(obj, dict):
                    continue
                ohlc_obj = obj.get("ohlc")
                close = ohlc_obj.get("close") if isinstance(ohlc_obj, dict) else None
                bid, ask = None, None
                depth = obj.get("depth")
                if isinstance(depth, dict):
                    bid = _first_depth_price(depth.get("buy"))
                    ask = _first_depth_price(depth.get("sell"))
                out[key] = {
                    "last_price": obj.get("last_price"),
                    "close": close,
                    "best_bid": bid,
                    "best_offer": ask,
                }
        except Exception as e:
            print(f"[{get_indian_time_log()}] Quote kite.quote error for chunk: {e}", flush=True)
    return out


def _pct_change(last_price, close):
    """
    Percentage change from previous close (ohlc.close).
    Formula: ((last_price - close) / close) * 100
    Returns None if close is missing or zero.
    """
    if close is None:
        return None
    try:
        close_f = float(close)
        if close_f == 0:
            return None
        last_f = float(last_price)
        return ((last_f - close_f) / close_f) * 100
    except (TypeError, ValueError):
        return None


def _underlying_to_key(cell_value):
    """Convert cell value to exchange:tradingsymbol for quote API."""
    if not cell_value or not str(cell_value).strip():
        return None
    s = str(cell_value).strip()
    return s if ":" in s else f"NSE:{s}"


def update_positions_n_o_p_columns(ws, positions_data, kite):
    """
    Positions sheet only:
    - Read underlying names from column M.
    - Single batched kite.quote(all_keys) for underlyings (M) and row instruments (A/B).
    - N = Underlying LTP (quote last_price for underlying from M).
    - O = Instrument LTP (quote last_price for row instrument).
    - P = Underlying % change from quote last_price vs ohlc.close for underlying from M.
    - Q / R = Best bid / best offer (depth) for row instrument only.
    Does not modify column M.
    """
    if not ws or not positions_data or len(positions_data) < 2:
        return
    num_data_rows = len(positions_data) - 1
    end_row = num_data_rows + 1
    try:
        range_read = f"M2:M{end_row}"
        col_m = ws.get(range_read)
        if not col_m:
            col_m = []
        underlyings = []
        for row in col_m:
            cell = row[0] if isinstance(row, (list, tuple)) else row
            underlyings.append(_underlying_to_key(cell))
        # Row instrument keys from column A/B
        row_keys = []
        for i in range(1, len(positions_data)):
            row = positions_data[i]
            if len(row) >= 2 and row[0] and row[1]:
                row_keys.append(f"{row[1]}:{row[0]}")
            else:
                row_keys.append(None)
        # Pad underlyings if column M shorter than data rows
        while len(underlyings) < len(row_keys):
            underlyings.append(None)
        all_keys = list({k for k in (row_keys + underlyings) if k})
        if not all_keys:
            return
        quote_map = fetch_positions_quotes_via_kite(kite, all_keys)
        # Single N1:R{last} write (much faster than hundreds of batch_update ranges)
        nop_rows = [
            [
                "Underlying LTP",
                "Instrument LTP",
                "Underlying percentage change",
                "Best bid",
                "Best offer",
            ],
        ]
        for i in range(len(row_keys)):
            u_key = underlyings[i] if i < len(underlyings) else None
            r_key = row_keys[i]
            if u_key and quote_map.get(u_key) and quote_map[u_key].get("last_price") is not None:
                n_val = round(float(quote_map[u_key]["last_price"]), 2)
            else:
                n_val = ""
            if r_key and quote_map.get(r_key) and quote_map[r_key].get("last_price") is not None:
                o_val = round(float(quote_map[r_key]["last_price"]), 2)
            else:
                o_val = ""
            if u_key and quote_map.get(u_key):
                info = quote_map[u_key]
                pct = _pct_change(info["last_price"], info.get("close"))
                pct_str = f"{round(pct, 2)}%" if pct is not None else ""
                if pct_str and pct_str[0] in ("-", "+", "="):
                    pct_str = "\u200B" + pct_str
            else:
                pct_str = ""
            q_val, r_val = "", ""
            if r_key and quote_map.get(r_key):
                qinfo = quote_map[r_key]
                bb = qinfo.get("best_bid")
                bo = qinfo.get("best_offer")
                if bb is not None:
                    q_val = round(float(bb), 2)
                if bo is not None:
                    r_val = round(float(bo), 2)
            nop_rows.append([n_val, o_val, pct_str, q_val, r_val])
        last_row = len(nop_rows)
        ws.update(range_name=f"N1:R{last_row}", values=nop_rows)
    except Exception as e:
        print(f"[{get_indian_time_log()}] Positions N/O/P/Q/R columns update error: {e}", flush=True)


def update_portfolio_data(kite, sheet_id):
    """
    Update Google Sheets tabs: Holdings, then OrdersToday, then Positions (Kite data).
    Uses IST timestamps and gspread client already used elsewhere.
    """
    try:
        print(f"[{get_indian_time_log()}] Updating portfolio data (Holdings, OrdersToday, Positions)...", flush=True)
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        # Prepare worksheets (create if missing)
        def get_or_create_worksheet(name, rows=2000, cols=26):
            try:
                return spreadsheet.worksheet(name)
            except Exception:
                return spreadsheet.add_worksheet(title=name, rows=str(rows), cols=str(cols))

        ws_holdings = get_or_create_worksheet('Holdings')
        ws_positions = get_or_create_worksheet('Positions')
        ws_orders_today = get_or_create_worksheet('OrdersToday')

        # Clear: Holdings/Orders = data only. Positions = data + N/O/P/Q/R (not M = underlying names)
        try:
            ws_holdings.batch_clear(['A1:I2000'])
            ws_positions.batch_clear([
                'A1:J2000', 'N1:N2000', 'O1:O2000', 'P1:P2000', 'Q1:Q2000', 'R1:R2000',
            ])
            ws_orders_today.batch_clear(['A1:L2000'])
        except Exception:
            pass

        # Holdings
        holdings_data = [[
            "Instrument", "Exchange", "ISIN", "Qty", "T1 Qty", "Collateral Qty",
            "Avg Price", "Last Price", "P&L"
        ]]
        try:
            holdings = kite.holdings()
            for item in holdings:
                if not item.get('tradingsymbol'):
                    continue
                holdings_data.append([
                    item.get('tradingsymbol', ''),
                    item.get('exchange', ''),
                    item.get('isin', ''),
                    item.get('quantity', 0),
                    item.get('t1_quantity', 0),
                    item.get('collateral_quantity', 0),
                    item.get('average_price', 0.0),
                    item.get('last_price', 0.0),
                    item.get('pnl', 0.0),
                ])
        except Exception as e:
            print(f"Holdings fetch error: {e}")

        if holdings_data:
            ws_holdings.update(range_name='A1', values=holdings_data)

        # OrdersToday (before Positions)
        orders_today_data = [[
            "Order ID", "Instrument", "Exchange", "Order Type", "Product",
            "Transaction Type", "Variety", "Status", "Order Timestamp", "Qty",
            "Filled Qty", "Price"
        ]]
        try:
            orders = kite.orders()
            today_ist_date = get_indian_time().date()

            for o in orders:
                order_ts = o.get('order_timestamp')
                if not order_ts:
                    continue

                # Normalize to datetime
                if isinstance(order_ts, str):
                    try:
                        # Attempt ISO parse first
                        ts_dt = datetime.fromisoformat(order_ts)
                    except Exception:
                        # Fallback to known format
                        try:
                            ts_dt = datetime.strptime(order_ts, '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            continue
                else:
                    ts_dt = order_ts

                # Normalize to Indian Standard Time (IST)
                if ts_dt.tzinfo is None:
                    # Assume naive timestamps are already in IST (exchange local time)
                    ts_ist = ts_dt.replace(tzinfo=IST)
                else:
                    ts_ist = ts_dt.astimezone(IST)

                if ts_ist.date() == today_ist_date:
                    orders_today_data.append([
                        o.get('order_id', ''),
                        o.get('tradingsymbol', ''),
                        o.get('exchange', ''),
                        o.get('order_type', ''),
                        o.get('product', ''),
                        o.get('transaction_type', ''),
                        o.get('variety', ''),
                        o.get('status', ''),
                        ts_ist.strftime('%Y-%m-%d %H:%M:%S'),
                        o.get('quantity', 0),
                        o.get('filled_quantity', 0),
                        o.get('average_price', 0.0) or o.get('price', 0.0),
                    ])
        except Exception as e:
            print(f"Orders fetch error: {e}")

        if orders_today_data:
            ws_orders_today.update(range_name='A1', values=orders_today_data)

        # Positions (net) — after OrdersToday
        positions_data = [[
            "Instrument", "Exchange", "Product", "Qty", "Overnight Qty",
            "Avg Price", "Last Price", "P&L", "Realised", "Unrealised", "Margin Required"
        ]]
        try:
            positions = kite.positions().get('net', [])
            for p in positions:
                if not p.get('tradingsymbol'):
                    continue
                positions_data.append([
                    p.get('tradingsymbol', ''),
                    p.get('exchange', ''),
                    p.get('product', ''),
                    p.get('quantity', 0),
                    p.get('overnight_quantity', 0),
                    p.get('average_price', 0.0),
                    p.get('last_price', 0.0),
                    p.get('pnl', 0.0),
                    p.get('realised', 0.0),
                    p.get('unrealised', 0.0),
                    '',  # Margin Required will be calculated separately
                ])
        except Exception as e:
            print(f"Positions fetch error: {e}")

        if positions_data:
            ws_positions.update(range_name='A1', values=positions_data)

            # Brief pause so column M is readable before quote (optional rate-limit spacing)
            print(f"[{get_indian_time_log()}] Pausing 0.5 seconds before N/O/P/Q/R update...", flush=True)
            time.sleep(0.3)

            # N–R from batched kite.quote (LTP, underlying %, depth) — one API type, deduped keys
            print(f"[{get_indian_time_log()}] Updating Positions N/O/P/Q/R (underlying from column M)...", flush=True)
            update_positions_n_o_p_columns(ws_positions, positions_data, kite)
            print(
                f"[{get_indian_time_log()}] Positions N, O, P, Q (best bid), R (best offer) updated.",
                flush=True,
            )

            # Margin (column K) after N/O/P/Q/R
            print(f"[{get_indian_time_log()}] Pausing 0.5 seconds before margin calculation...", flush=True)
            time.sleep(0.3)
            print(f"[{get_indian_time_log()}] Calculating position margins...", flush=True)
            margin_result = calculate_position_margins(kite, positions_data, ws_positions)
            print(f"[{get_indian_time_log()}] Margin calculation result: {margin_result}", flush=True)

        # Update Info! last_updated timestamp (IST) similar to update-data.py logic
        try:
            ws_info = get_or_create_worksheet('Info')
            values = ws_info.get('A1:B20') or []
            target_row = None
            for idx, row in enumerate(values, start=1):
                if len(row) >= 1 and str(row[0]).strip().lower() == 'last_updated':
                    target_row = idx
                    break
            if target_row is not None:
                ws_info.update(range_name=f'B{target_row}', values=[[get_indian_timestamp()]])
            else:
                # If not found, append the key-value at the end (next row after current values)
                next_row = len(values) + 1
                ws_info.update(range_name=f'A{next_row}:B{next_row}', values=[['last_updated', get_indian_timestamp()]])
        except Exception as e:
            print(f"Info! last_updated write error: {e}")

        print(f"[{get_indian_time_log()}] Portfolio data update done.", flush=True)
        return "Portfolio update done"
    except Exception as e:
        print(f"update_portfolio_data error: {e}", flush=True)
        return f"Error: {e}"


def format_indian_number(number):
    """
    Format number in proper Indian number format with Rupee symbol (₹)
    Example: 9876543.21 -> ₹98,76,543.21
    Rule: 2 decimal places, comma after first 3 digits, then every 2 digits
    """
    try:
        if number == 0 or number is None:
            return "₹0.00"
        
        # Convert to float and round to 2 decimal places
        num = round(float(number), 2)
        
        # Split into integer and decimal parts
        integer_part, decimal_part = str(num).split('.')
        
        # Indian number format: comma after first 3 digits, then every 2 digits
        if len(integer_part) <= 3:
            return f"₹{integer_part}.{decimal_part}"
        
        # For numbers > 3 digits, add comma after first 3 digits, then every 2 digits
        formatted_integer = ""
        
        # Take first 3 digits
        first_part = integer_part[:-3] if len(integer_part) > 3 else ""
        last_part = integer_part[-3:] if len(integer_part) > 3 else integer_part
        
        if first_part:
            # Add commas every 2 digits from right for the remaining part
            first_part_reversed = first_part[::-1]
            formatted_first = ""
            for i, digit in enumerate(first_part_reversed):
                if i > 0 and i % 2 == 0:
                    formatted_first = "," + formatted_first
                formatted_first = digit + formatted_first
            
            formatted_integer = formatted_first + "," + last_part
        else:
            formatted_integer = last_part
        
        return f"₹{formatted_integer}.{decimal_part}"
    except Exception as e:
        print(f"Error formatting number {number}: {e}")
        return f"₹{str(number)}"

def calculate_position_margins(kite, positions_data, ws_positions):
    """
    Calculate margin required for each position using batch API and update column K in Positions sheet
    """
    try:
        print(f"[{get_indian_time_log()}] Calculating position margins using batch API...", flush=True)
        
        # No clearing needed - batch_update will directly overwrite with margin data while preserving formatting
        
        # Prepare order parameters for batch margin calculation
        order_params = []
        position_mapping = []  # To map results back to row numbers
        
        # Skip header row (index 0), process data rows
        for idx in range(1, len(positions_data)):
            if len(positions_data[idx]) < 11:  # Ensure we have enough columns
                continue
                
            symbol = positions_data[idx][0]  # Column A - Instrument
            exchange = positions_data[idx][1]  # Column B - Exchange
            quantity = positions_data[idx][3]  # Column D - Qty
            product = positions_data[idx][2]  # Column C - Product
            
            # Skip if no symbol or quantity
            if not symbol or not quantity:
                continue
                
            try:
                # Convert quantity to int
                qty = int(float(quantity))
                if qty == 0:
                    continue
                    
                # Determine transaction type based on quantity
                transaction_type = "BUY" if qty > 0 else "SELL"
                abs_quantity = abs(qty)
                
                # Set default product if not specified
                if not product or product.strip() == "":
                    if exchange == "NSE":
                        product = "CNC"  # Cash and Carry for equity shares
                    elif exchange in ["NFO", "CDS"]:
                        product = "NRML"  # Normal margin for derivatives
                    else:
                        product = "NRML"  # Default
                
                # Prepare order parameter for batch margin calculation
                order_param = {
                    "exchange": exchange,
                    "tradingsymbol": symbol,
                    "transaction_type": transaction_type,
                    "variety": "regular",
                    "product": product,
                    "order_type": "MARKET",  # Using MARKET for margin calculation
                    "quantity": abs_quantity,
                    "price": 0,  # Required parameter
                    "trigger_price": 0  # Required parameter
                }
                
                order_params.append(order_param)
                position_mapping.append({
                    'row_idx': idx,
                    'symbol': symbol,
                    'transaction_type': transaction_type,
                    'quantity': abs_quantity
                })
                
            except Exception as e:
                print(f"Error preparing order param for {symbol}: {e}")
                continue
        
        # Calculate margins for positions in batches of 200
        if not order_params:
            print("No valid positions found for margin calculation")
            return "No positions to calculate"
            
        # Process in batches of 200
        batch_size = 200
        total_positions = len(order_params)
        margin_updates = []
        successful_calculations = 0
        error_calculations = 0
        
        print(f"Calculating margins for {total_positions} positions in batches of {batch_size}...")
        
        for batch_start in range(0, total_positions, batch_size):
            batch_end = min(batch_start + batch_size, total_positions)
            batch_params = order_params[batch_start:batch_end]
            batch_mapping = position_mapping[batch_start:batch_end]
            
            print(f"Processing batch {batch_start//batch_size + 1}: positions {batch_start + 1}-{batch_end}")
            
            try:
                # Calculate margins for this batch
                margin_results = kite.order_margins(batch_params)
                print(f"Received margin results for {len(margin_results)} orders in this batch")
                
                # Process results for this batch
                for i, result in enumerate(margin_results):
                    if i >= len(batch_mapping):
                        break
                        
                    position_info = batch_mapping[i]
                    row_num = position_info['row_idx'] + 1  # 1-based row number
                    
                    try:
                        # Extract total margin required and format in Indian number format
                        total_margin = result.get('total', 0)
                        formatted_margin = format_indian_number(total_margin)
                        
                        margin_updates.append({
                            'range': f"K{row_num}",
                            'values': [[formatted_margin]]
                        })
                        
                        successful_calculations += 1
                        
                    except Exception as e:
                        print(f"Error processing margin result for {position_info['symbol']}: {e}")
                        margin_updates.append({
                            'range': f"K{row_num}",
                            'values': [["Error"]]
                        })
                        error_calculations += 1
                
                # Add a small delay between batches to avoid rate limiting
                if batch_end < total_positions:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error in batch margin calculation for batch {batch_start//batch_size + 1}: {e}")
                # Mark all positions in this batch as error
                for position_info in batch_mapping:
                    row_num = position_info['row_idx'] + 1
                    margin_updates.append({
                        'range': f"K{row_num}",
                        'values': [["Error"]]
                    })
                    error_calculations += len(batch_mapping)
        
        # Batch update all margin calculations
        if margin_updates:
            try:
                ws_positions.batch_update(margin_updates)
                print(f"Updated {len(margin_updates)} position margins: {successful_calculations} successful, {error_calculations} errors")
            except Exception as e:
                print(f"Error updating margins: {e}")
        
        print(f"[{get_indian_time_log()}] Position margin calculation completed", flush=True)
        return "Margins calculated"
        
    except Exception as e:
        print(f"calculate_position_margins error: {e}", flush=True)
        return f"Error: {e}"

def update_info_with_margins(kite, sheet_id):
    """
    Fetch account margins and write flattened key/value pairs into Info sheet
    starting from row 10 (overwrite existing cells in that range, do not clear sheet).
    """
    try:
        print(f"[{get_indian_time_log()}] Updating Info sheet with account margins...", flush=True)
        # Fetch all segments margins
        try:
            margins_data = kite.margins()
        except Exception as e:
            print(f"Margins fetch error: {e}")
            return "Margins fetch error"

        # Exclude commodity segment entirely if present (case-insensitive)
        try:
            if isinstance(margins_data, dict):
                margins_data = {k: v for k, v in margins_data.items() if str(k).lower() != 'commodity'}
        except Exception:
            pass

        # Flatten the nested dict into key/value rows
        rows_to_write = []
        rows_to_write.append(["margins_last_updated", get_indian_timestamp()])

        def walk(prefix, obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    # Skip commodity keys at any level (first path segment)
                    first_segment = (prefix.split('.')[0] if prefix else str(k)).lower()
                    if first_segment == 'commodity' or str(k).lower() == 'commodity':
                        continue
                    walk(f"{prefix}.{k}" if prefix else str(k), v)
            else:
                # Convert lists or primitives to string
                value_str = ", ".join(map(str, obj)) if isinstance(obj, list) else obj
                rows_to_write.append([prefix, value_str])

        # margins_data typically has segments like 'equity', 'commodity'
        walk("", margins_data)

        # Prepare gspread client
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        # Ensure Info sheet exists
        try:
            ws_info = spreadsheet.worksheet('Info')
        except Exception:
            ws_info = spreadsheet.add_worksheet(title='Info', rows='100', cols='26')

        # Write starting from row 10
        start_row = 10
        end_row = start_row + len(rows_to_write) - 1
        ws_info.update(range_name=f"A{start_row}:B{end_row}", values=rows_to_write)

        print(f"[{get_indian_time_log()}] Info margins update done ({len(rows_to_write)} rows).", flush=True)
        return f"Info margins updated: {len(rows_to_write)} rows"
    except Exception as e:
        print(f"update_info_with_margins error: {e}", flush=True)
        return f"Error: {e}"


def cancel_marked_open_orders(kite, sheet_id):
    """
    Cancel orders marked for cancellation in column M ("Yes") where:
    - Column H contains "OPEN PENDING"
    - Column G has an order_id
    - Column AA is empty (not already processed)

    Immediately writes cancellation intent/status into column AA to avoid re-processing.
    """
    try:
        print(f"[{get_indian_time_log()}] Processing cancellations for marked open orders...", flush=True)
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet('Actions')

        rows = sheet.get_all_values()
        if len(rows) <= 1:
            print("No data rows found for cancellations.", flush=True)
            return "No data rows"

        updates = []
        cancelled = 0
        skipped = 0
        errors = 0

        for idx in range(1, len(rows)):
            row = rows[idx]
            # Safely access columns
            order_id = (row[6] or "").strip() if len(row) > 6 else ""  # G
            order_status = (row[7] or "").strip().upper() if len(row) > 7 else ""  # H
            cancel_flag = (row[12] or "").strip().upper() if len(row) > 12 else ""  # M
            cancel_status_aa = (row[26] or "").strip() if len(row) > 26 else ""  # AA

            # Only process when M == Yes, H contains OPEN PENDING, G has order_id, and AA empty
            if cancel_flag == "YES" and "OPEN PENDING" in order_status and order_id and not cancel_status_aa:
                try:
                    kite.cancel_order(kite.VARIETY_REGULAR, order_id)
                    row_num = idx + 1
                    updates.append({
                        'range': f"AA{row_num}",
                        'values': [["CANCEL_REQUESTED"]]
                    })
                    cancelled += 1
                except Exception as e:
                    print(f"Cancel error for order {order_id}: {e}")
                    row_num = idx + 1
                    updates.append({
                        'range': f"AA{row_num}",
                        'values': [["CANCEL_ERROR"]]
                    })
                    errors += 1
                time.sleep(0.25)
            else:
                skipped += 1

        if updates:
            try:
                sheet.batch_update(updates)
                print(f"Applied {len(updates)} cancellation status updates in batch.")
            except Exception as e:
                print(f"Batch cancellation update error: {e}")

        result = f"cancelled={cancelled}, errors={errors}, skipped={skipped}"
        print(f"Cancellation processing done: {result}", flush=True)
        return result
    except Exception as e:
        print(f"cancel_marked_open_orders error: {e}", flush=True)
        return f"Error: {e}"

def place_order(symbol, direction, quantity, product=None):
    # Automatically detect exchange based on symbol
    if sum(1 for char in symbol if char.isdigit()) >= 2:
        # Check if it's CDS (Currency Derivatives) first
        if any(currency in symbol.upper() for currency in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR', 'INR']):
            exchange = "CDS"  # Currency derivatives
            # concise logs: no per-symbol exchange print
        else:
            exchange = "NFO"  # Other derivatives (options/futures)
            # concise logs: no per-symbol exchange print
    else:
        exchange = "NSE"  # No numbers = equity shares
        # concise logs: no per-symbol exchange print
    
    exchanges = {"NSE": kite.EXCHANGE_NSE, "NFO": kite.EXCHANGE_NFO, "CDS": kite.EXCHANGE_CDS}
    directions = {"BUY": kite.TRANSACTION_TYPE_BUY, "SELL": kite.TRANSACTION_TYPE_SELL}
    products = {"CNC": kite.PRODUCT_CNC, "MIS": kite.PRODUCT_MIS, "NRML": kite.PRODUCT_NRML}
    
    # Set default product based on exchange if not specified
    if product is None:
        if exchange == "NSE":
            product = "CNC"  # Cash and Carry for equity shares
        elif exchange in ["NFO", "CDS"]:
            product = "NRML"  # Normal margin for derivatives
        # concise logs: no per-symbol exchange print
    
    # Always get the best price from quotes for LIMIT orders
    try:
        # Get quote for the symbol
        quote_symbol = f"{exchange}:{symbol}"
        quotes = kite.quote(quote_symbol)
        
        if direction == "BUY":
            # For BUY order, use best bid price (what buyers are willing to pay)
            best_price = quotes[quote_symbol]['depth']['buy'][0]['price']
            # concise logs: no per-symbol exchange print
        else:  # SELL
            # For SELL order, use best ask price (what sellers are asking)
            best_price = quotes[quote_symbol]['depth']['sell'][0]['price']
            # concise logs: no per-symbol exchange print
        
    except Exception as e:
        print(f"Error getting quote for price: {e}")
        # Always return a tuple to avoid unpacking errors upstream
        return None, None
    
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchanges[exchange],
            tradingsymbol=symbol,
            transaction_type=directions[direction],
            quantity=quantity,
            product=products[product],
            order_type=kite.ORDER_TYPE_LIMIT,  # Always LIMIT
            price=best_price,  # Use the fetched price
            validity=kite.VALIDITY_DAY
        )
        # concise logs: no per-symbol exchange print
        return order_id, best_price
    except Exception as e:
        print(f"Order error: {e}")
        return None, None



def process_place_orders_with_kite(kite, sheet_id, threshold_minutes=600):
    """
    Read orders from Google Sheet 'Actions' and process rows without action_status.
    First places all pending orders, then checks status of placed orders.
    This version accepts kite and sheet_id as parameters for Cloud Functions.
    """
    try:
        print(f"[{get_indian_time_log()}] Starting order processing cycle...", flush=True)
        # STEP 0: Update Info sheet with margins snapshot
        print(f"[{get_indian_time_log()}] Step 0: Updating Info with margins...", flush=True)
        update_info_with_margins(kite, sheet_id)
        
        # STEP 1: Place all pending orders
        print(f"[{get_indian_time_log()}] Step 1: Placing pending orders...", flush=True)
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet('Actions')

        rows = sheet.get_all_values()
        
        placed_count = 0
        skipped_count = 0
        invalid_count = 0
        placements_updates = []
        
        if len(rows) <= 1:
            print("No data rows found (only header).", flush=True)
        else:
            # rows[0] is header; start from index 1
            for idx in range(1, len(rows)):
                row = rows[idx]
                # Safely access columns with defaults
                symbol = row[0].strip() if len(row) > 0 else ""
                direction = (row[1] or "").strip().upper() if len(row) > 1 else ""
                quantity_str = (row[2] or "").strip() if len(row) > 2 else ""
                action_status = (row[3] or "").strip().upper() if len(row) > 3 else ""

                # Skip empty or already processed rows
                if not symbol or not direction or not quantity_str:
                    invalid_count += 1
                    continue
                if action_status == "ORDER_PLACED":
                    skipped_count += 1
                    continue

                try:
                    quantity = int(float(quantity_str))
                except Exception:
                    print(f"Invalid quantity at row {idx+1}: '{quantity_str}'")
                    continue

                # Place the order using the provided kite object
                print(f"Placing order for row {idx+1}: {symbol} {direction} {quantity}", flush=True)
                order_id, limit_price = place_order_with_kite(kite, symbol, direction, quantity)

                # On success, accumulate action_status and timestamp update
                if order_id:
                    row_num = idx + 1  # 1-based row in sheet
                    timestamp = get_indian_timestamp()
                    placements_updates.append({
                        'range': f"D{row_num}:G{row_num}",
                        'values': [["Order_Placed", timestamp, limit_price, order_id]]
                    })
                    placed_count += 1
                
                # Add a small delay between processing rows
                time.sleep(0.5)
        
        # Perform one batch update for all placements
        if placements_updates:
            try:
                sheet.batch_update(placements_updates)
                print(f"Applied {len(placements_updates)} placement updates in batch.")
            except Exception as e:
                print(f"Batch placements update error: {e}")

        total_rows = len(rows) - 1
        place_result = f"total={total_rows}, placed={placed_count}, skipped={skipped_count}, invalid={invalid_count}"
        print(f"Order placement cycle done: {place_result}", flush=True)
        
        # STEP 2: Cancel marked open-pending orders before status checks
        print(f"[{get_indian_time_log()}] Step 2: Cancelling marked open orders...", flush=True)
        cancel_result = cancel_marked_open_orders(kite, sheet_id)

        # STEP 3: Check status of all placed orders
        print(f"[{get_indian_time_log()}] Step 3: Checking order statuses...", flush=True)
        status_result = update_order_statuses(kite, sheet_id, threshold_minutes)
        
        # STEP 4: Update portfolio snapshots
        print(f"[{get_indian_time_log()}] Step 4: Updating portfolio data...", flush=True)
        portfolio_result = update_portfolio_data(kite, sheet_id)

        # Combine results
        final_result = f"Placement: {place_result} | Cancel: {cancel_result} | Status: {status_result} | Portfolio: {portfolio_result}"
        print(f"Complete cycle done: {final_result}", flush=True)
        
        return final_result
        
    except Exception as e:
        print(f"process_place_orders_with_kite error: {e}", flush=True)
        return f"Error: {e}"

def process_place_orders():
    """
    Read orders from Google Sheet 'Actions' and process rows without action_status.
    First places all pending orders, then checks status of placed orders.
    Columns:
      A: symbol, B: direction (BUY/SELL), C: quantity, D: action_status, E: timestamp, F: limit_price, G: order_id, H: order_status, I: status_timestamp
    Starts from row 2 (row 1 is header). If D == 'Order_Placed', skip.
    """
    try:
        print(f"[{get_indian_time_log()}] Starting order processing cycle...", flush=True)
        # STEP 0: Update Info sheet with margins snapshot
        print(f"[{get_indian_time_log()}] Step 0: Updating Info with margins...", flush=True)
        update_info_with_margins(kite, SHEET_ID)
        
        # STEP 1: Place all pending orders
        print(f"[{get_indian_time_log()}] Step 1: Placing pending orders...", flush=True)
        creds = Credentials.from_service_account_file('service_account.json', scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet('Actions')

        rows = sheet.get_all_values()
        placed_count = 0
        skipped_count = 0
        invalid_count = 0
        placements_updates = []
        if len(rows) <= 1:
            print("No data rows found (only header).", flush=True)
        else:
            # rows[0] is header; start from index 1
            for idx in range(1, len(rows)):
                row = rows[idx]
                # Safely access columns with defaults
                symbol = row[0].strip() if len(row) > 0 else ""
                direction = (row[1] or "").strip().upper() if len(row) > 1 else ""
                quantity_str = (row[2] or "").strip() if len(row) > 2 else ""
                action_status = (row[3] or "").strip().upper() if len(row) > 3 else ""

                # Skip empty or already processed rows
                if not symbol or not direction or not quantity_str:
                    invalid_count += 1
                    continue
                if action_status == "ORDER_PLACED":
                    skipped_count += 1
                    continue

                try:
                    quantity = int(float(quantity_str))
                except Exception:
                    print(f"Invalid quantity at row {idx+1}: '{quantity_str}'")
                    continue

                # Place the order
                print(f"Placing order for row {idx+1}: {symbol} {direction} {quantity}", flush=True)
                order_id, limit_price = place_order(symbol, direction, quantity)

                # On success, accumulate action_status and timestamp
                if order_id:
                    row_num = idx + 1  # 1-based row in sheet
                    timestamp = get_indian_timestamp()
                    placements_updates.append({
                        'range': f"D{row_num}:G{row_num}",
                        'values': [["Order_Placed", timestamp, limit_price, order_id]]
                    })
                    placed_count += 1
                
                # Add a small delay between processing rows
                time.sleep(0.5)
        
        # Perform one batch update for all placements
        if placements_updates:
            try:
                sheet.batch_update(placements_updates)
                print(f"Applied {len(placements_updates)} placement updates in batch.")
            except Exception as e:
                print(f"Batch placements update error: {e}")

        total_rows = len(rows) - 1
        place_result = f"total={total_rows}, placed={placed_count}, skipped={skipped_count}, invalid={invalid_count}"
        print(f"Order placement cycle done: {place_result}", flush=True)
        
        # STEP 2: Cancel marked open-pending orders before status checks
        print(f"[{get_indian_time_log()}] Step 2: Cancelling marked open orders...", flush=True)
        cancel_result = cancel_marked_open_orders(kite, SHEET_ID)

        # STEP 3: Check status of all placed orders
        print(f"[{get_indian_time_log()}] Step 3: Checking order statuses...", flush=True)
        status_result = update_order_statuses(kite, SHEET_ID, threshold_minutes)
        
        # STEP 4: Update portfolio snapshots
        print(f"[{get_indian_time_log()}] Step 4: Updating portfolio data...", flush=True)
        portfolio_result = update_portfolio_data(kite, SHEET_ID)

        # Combine results
        final_result = f"Placement: {place_result} | Cancel: {cancel_result} | Status: {status_result} | Portfolio: {portfolio_result}"
        print(f"Complete cycle done: {final_result}", flush=True)
        
    except Exception as e:
        print(f"process_place_orders error: {e}", flush=True)


if __name__ == "__main__":
    print("Starting Actions run (single execution)...", flush=True)
    process_place_orders()

# Google Cloud Functions HTTP entry point
def hello_http(request):
    """HTTP trigger: Sheet creds + Kite via Oxylabs. Oxylabs keys come from .env only."""
    global api_key, access_token
    try:
        print(f"[{get_indian_time_log()}] Starting Cloud Function execution...")
        load_dotenv()
        
        # Use module-level SHEET_ID loaded from .env
        if not SHEET_ID:
            return ("SHEET_ID is not set in .env.", 500, {"Content-Type": "text/plain"})
        
        # Get credentials from Google Sheet (update globals so lazy _tick_map() matches this request's kite)
        api_key, api_secret, access_token, threshold_minutes = get_credentials_from_sheet()
        
        if not api_key or not api_secret:
            return ("Failed to load API credentials.", 500, {"Content-Type": "text/plain"})
        
        # Kite: same Oxylabs dict as local run — all api.kite.trade HTTPS uses this tunnel
        try:
            cf_proxies = get_oxylabs_proxies()
        except RuntimeError as e:
            return (str(e), 500, {"Content-Type": "text/plain"})
        kite = KiteConnect(api_key=api_key, proxies=cf_proxies, timeout=60)
        
        # Set access token and verify session (separate proxy/network vs auth errors)
        if access_token:
            try:
                kite.set_access_token(access_token)
                kite.profile()
                print(f"[{get_indian_time_log()}] Using access token from Google Sheet.")
            except requests.exceptions.RequestException as e:
                msg = (
                    f"Zerodha API unreachable via Oxylabs proxy (check OXYLABS_* credentials, "
                    f"host/port, firewall, or proxy status). Details: {e!s}"
                )
                print(f"[{get_indian_time_log()}] {msg}", flush=True)
                return (msg, 503, {"Content-Type": "text/plain"})
            except TokenException as e:
                print(f"[{get_indian_time_log()}] Token error: {e}", flush=True)
                return (
                    f"Access token invalid or expired (refresh B3 in Sheet). Kite: {e!s}",
                    401,
                    {"Content-Type": "text/plain"},
                )
            except KiteException as e:
                print(f"[{get_indian_time_log()}] Kite API error on profile: {e}", flush=True)
                return (f"Kite API error: [{e.code}] {e!s}", 502, {"Content-Type": "text/plain"})
        else:
            return ("No access token available.", 500, {"Content-Type": "text/plain"})
        
        # Now process orders with the initialized kite object (places orders + checks status)
        print(f"[{get_indian_time_log()}] Starting order processing and status checking...")
        result = process_place_orders_with_kite(kite, SHEET_ID, threshold_minutes)
        print(f"[{get_indian_time_log()}] Cloud Function cycle completed: {result}")
        
        # Return detailed response with Indian timestamp
        response_time = get_indian_timestamp()
        return (f"[{response_time}] Processed Actions and checked statuses. {result}", 200, {"Content-Type": "text/plain"})
        
    except Exception as e:
        error_time = get_indian_timestamp()
        print(f"[{error_time}] Cloud Function error: {e}")
        return (f"[{error_time}] Error while processing: {e}", 500, {"Content-Type": "text/plain"})
