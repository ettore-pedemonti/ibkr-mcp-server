"""IBKR MCP Server — ib_async based, propagates errors honestly."""
import os
import sys
import copy
import asyncio
import math
from collections import deque
from datetime import datetime, timedelta, timezone, time
from importlib import metadata
from typing import Any, Optional, Literal
from zoneinfo import ZoneInfo
import numpy as np
import yfinance as yf
from fastmcp import FastMCP
from scipy.optimize import brentq
from scipy.stats import norm
from ib_async import (
    IB,
    Bag,
    ComboLeg,
    ScannerSubscription,
    Stock,
    Option,
    LimitOrder,
    MarketOrder,
    StopOrder,
    Trade,
    ExecutionFilter,
)


def _env_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value in {None, ""}:
        return default
    try:
        return float(raw_value)
    except ValueError:
        return default

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "10"))
MCP_SERVER_VERSION = "1.0"
SERVER_RUNNING_SINCE = datetime.now(timezone.utc)
RECENT_ERRORS_MAX = 200
OPTION_GENERIC_TICKS = "100,101,104,106,165,221,225,233,236,258,293,294,295,318,375,411"
RISK_MAX_ORDER_NOTIONAL = _env_float("IBKR_MAX_ORDER_NOTIONAL", 5000.0)
RISK_MAX_PRICE_DISTANCE_PCT = _env_float("IBKR_MAX_PRICE_DISTANCE_PCT", 0.10)
RISK_MAX_CONCENTRATION_PCT = _env_float("IBKR_MAX_CONCENTRATION_PCT", 0.20)

_ib: Optional[IB] = None
_ib_error_handler_target_id: Optional[int] = None
_recent_errors: deque[dict[str, Any]] = deque(maxlen=RECENT_ERRORS_MAX)
_active_subscriptions: dict[str, dict[str, Any]] = {}
_data_farms = {
    "market_data": "INACTIVE",
    "hmds": "INACTIVE",
    "secdef": "INACTIVE",
}
_KNOWN_SECTOR_BY_SYMBOL = {
    "ENI": "Energy",
    "ISP": "Financials",
    "NVDA": "Technology",
    "PLTR": "Technology",
    "SLV": "Materials",
}
_EXCHANGE_HOURS = {
    "SMART": {"timezone": "America/New_York", "open_local": "09:30", "close_local": "16:00", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "NYSE": {"timezone": "America/New_York", "open_local": "09:30", "close_local": "16:00", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "NASDAQ": {"timezone": "America/New_York", "open_local": "09:30", "close_local": "16:00", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "ARCA": {"timezone": "America/New_York", "open_local": "09:30", "close_local": "16:00", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "BVME": {"timezone": "Europe/Rome", "open_local": "09:00", "close_local": "17:30", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "LSE": {"timezone": "Europe/London", "open_local": "08:00", "close_local": "16:30", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "XETRA": {"timezone": "Europe/Berlin", "open_local": "09:00", "close_local": "17:30", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
    "TSE": {"timezone": "Asia/Tokyo", "open_local": "09:00", "close_local": "15:00", "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
}
FUNDAMENTALS_SOURCE_NOTE = "yfinance data - not from Reuters/Refinitiv. Activate IBKR Reuters subscription for institutional-grade fundamentals."
EU_FUNDAMENTALS_SOURCE_NOTE = "yfinance - verify before trading decision"
_SUPPORTED_FUNDAMENTALS_REPORT_TYPES = {
    "ReportsFinSummary",
    "ReportsOwnership",
    "ReportsFinStatements",
    "RESC",
    "CalendarReport",
}
_YF_EXCHANGE_SUFFIX = {
    "BVME": ".MI",
    "XETRA": ".DE",
    "LSE": ".L",
}
_KNOWN_EU_YF_TICKERS = {
    ("ENI", "BVME", "EUR"): "ENI.MI",
    ("ENI", "SMART", "EUR"): "ENI.MI",
    ("ISP", "BVME", "EUR"): "ISP.MI",
    ("ISP", "SMART", "EUR"): "ISP.MI",
}
_KNOWN_CRYPTO_SYMBOLS = {
    "ADA",
    "AVAX",
    "BCH",
    "BNB",
    "BTC",
    "DOGE",
    "DOT",
    "ETH",
    "LINK",
    "LTC",
    "MATIC",
    "ONDO",
    "SOL",
    "USDC",
    "USDT",
    "XRP",
}
_KNOWN_CRYPTO_EXCHANGES = {
    "BINANCE",
    "COINBASE",
    "CRYPTO",
    "GEMINI",
    "KRAKEN",
    "PAXOS",
}
_SUPPORTED_SCANNER_CODES = {
    "TOP_PERC_GAIN",
    "TOP_PERC_LOSE",
    "MOST_ACTIVE",
    "HOT_BY_VOLUME",
}
_SCANNER_LOCATION_BY_EXCHANGE = {
    "SMART": {"instrument": "STK", "locationCode": "STK.US.MAJOR"},
    "NASDAQ": {"instrument": "STK", "locationCode": "STK.US.MAJOR"},
    "NYSE": {"instrument": "STK", "locationCode": "STK.US.MAJOR"},
    "ARCA": {"instrument": "STK", "locationCode": "STK.US.MAJOR"},
}
_SCANNER_MARKET_NAMES_BY_EXCHANGE = {
    "NASDAQ": {"NASDAQ", "NMS", "SCM", "NGM"},
    "NYSE": {"NYSE", "NYS"},
    "ARCA": {"ARCA", "ARCX"},
}


def _log_mcp(tool_name: str, info: str) -> None:
    print(f"[MCP] {tool_name}: {info}", file=sys.stderr)


def _serialize_trade_log(trade: Trade) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for entry in getattr(trade, "log", []) or []:
        item = {
            "time": str(getattr(entry, "time", "")),
            "status": getattr(entry, "status", ""),
            "errorCode": getattr(entry, "errorCode", None),
            "message": getattr(entry, "message", ""),
        }
        serialized.append(item)
    return serialized


def _serialize_trade_status(trade: Trade) -> dict[str, Any]:
    return {
        "orderId": trade.order.orderId,
        "permId": getattr(trade.order, "permId", None),
        "symbol": getattr(trade.contract, "symbol", None),
        "secType": getattr(trade.contract, "secType", None),
        "action": getattr(trade.order, "action", None),
        "quantity": getattr(trade.order, "totalQuantity", None),
        "orderType": getattr(trade.order, "orderType", None),
        "lmtPrice": getattr(trade.order, "lmtPrice", None),
        "auxPrice": getattr(trade.order, "auxPrice", None),
        "status": getattr(trade.orderStatus, "status", None),
        "filled": getattr(trade.orderStatus, "filled", None),
        "remaining": getattr(trade.orderStatus, "remaining", None),
        "avgFillPrice": getattr(trade.orderStatus, "avgFillPrice", None),
        "whyHeld": getattr(trade.orderStatus, "whyHeld", None),
        "log": _serialize_trade_log(trade),
    }


def _safe_package_version(package_name: str) -> Optional[str]:
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        return None


def _subscription_key(contract: Any) -> str:
    return "|".join(
        [
            str(getattr(contract, "conId", "")),
            str(getattr(contract, "secType", "")),
            str(getattr(contract, "symbol", "")),
            str(getattr(contract, "exchange", "")),
            str(getattr(contract, "currency", "")),
            str(getattr(contract, "localSymbol", "")),
        ]
    )


def _track_market_data_subscription(contract: Any) -> None:
    _active_subscriptions[_subscription_key(contract)] = {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "secType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None) or getattr(contract, "primaryExchange", None),
        "currency": getattr(contract, "currency", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


def _untrack_market_data_subscription(contract: Any) -> None:
    _active_subscriptions.pop(_subscription_key(contract), None)


def _update_data_farms(error_code: Any, message: Any) -> None:
    code = error_code if isinstance(error_code, int) else None
    text = str(message or "").lower()

    if code == 2104 or "market data farm connection is ok" in text:
        _data_farms["market_data"] = "OK"
    elif code in {2103, 2108} or "market data farm connection is inactive" in text:
        _data_farms["market_data"] = "INACTIVE"

    if code == 2106 or "hmds data farm connection is ok" in text or "historical data farm connection is ok" in text:
        _data_farms["hmds"] = "OK"
    elif code in {2105, 2107} or "hmds data farm connection is inactive" in text:
        _data_farms["hmds"] = "INACTIVE"

    if code == 2158 or "sec-def data farm connection is ok" in text:
        _data_farms["secdef"] = "OK"
    elif code == 2157 or "sec-def data farm connection is inactive" in text:
        _data_farms["secdef"] = "INACTIVE"


def _record_ib_error(req_id: Any, error_code: Any, error_string: Any, *args: Any) -> None:
    entry = {
        "time": datetime.now(timezone.utc).isoformat(),
        "code": error_code,
        "message": str(error_string or ""),
        "reqId": req_id,
    }
    _recent_errors.append(entry)
    _update_data_farms(error_code, error_string)


def _ensure_ib_event_handlers(ib: IB) -> None:
    global _ib_error_handler_target_id
    if _ib_error_handler_target_id == id(ib):
        return
    ib.errorEvent += _record_ib_error
    _ib_error_handler_target_id = id(ib)


def _get_managed_accounts(ib: IB) -> list[str]:
    managed_accounts = getattr(ib, "managedAccounts", None)
    if callable(managed_accounts):
        try:
            value = managed_accounts()
            if value is not None:
                return list(value)
        except TypeError:
            pass
    if isinstance(managed_accounts, (list, tuple, set)):
        return list(managed_accounts)
    wrapper_accounts = getattr(getattr(ib, "wrapper", None), "accounts", None)
    if isinstance(wrapper_accounts, (list, tuple, set)):
        return list(wrapper_accounts)
    return []


def _get_ib_server_version(ib: IB) -> Optional[str]:
    client = getattr(ib, "client", None)
    if client is None:
        return None
    server_version = getattr(client, "serverVersion", None)
    if callable(server_version):
        try:
            server_version = server_version()
        except TypeError:
            server_version = None
    return str(server_version) if server_version is not None else None


def _get_uptime_seconds() -> float:
    return round((datetime.now(timezone.utc) - SERVER_RUNNING_SINCE).total_seconds(), 3)


def _normalize_side(side: Any) -> str:
    normalized = str(side or "").upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    return normalized


def _normalize_exchange(exchange: Any) -> str:
    normalized = str(exchange or "").upper()
    if not normalized:
        raise ValueError("exchange is required")
    return normalized


def _exchange_schedule(exchange: Any) -> Optional[dict[str, Any]]:
    return _EXCHANGE_HOURS.get(_normalize_exchange(exchange))


def _parse_clock(value: str) -> time:
    hours_text, minutes_text = value.split(":", 1)
    return time(hour=int(hours_text), minute=int(minutes_text))


def _combine_local_datetime(day: datetime.date, clock_value: str, timezone_name: str) -> datetime:
    parsed_time = _parse_clock(clock_value)
    return datetime(day.year, day.month, day.day, parsed_time.hour, parsed_time.minute, tzinfo=ZoneInfo(timezone_name))


def _next_trading_day(start_day: datetime.date, trading_days: set[str]) -> datetime.date:
    cursor = start_day
    while cursor.strftime("%a") not in trading_days:
        cursor += timedelta(days=1)
    return cursor


def _build_exchange_hours_view(exchange: Any, now_utc: Optional[datetime] = None) -> dict[str, Any]:
    normalized_exchange = _normalize_exchange(exchange)
    schedule = _exchange_schedule(normalized_exchange)
    if schedule is None:
        raise ValueError(f"unsupported exchange: {normalized_exchange}")

    timezone_name = schedule["timezone"]
    trading_days = list(schedule["trading_days"])
    trading_days_set = set(trading_days)
    local_now = (now_utc or datetime.now(timezone.utc)).astimezone(ZoneInfo(timezone_name))
    local_day = local_now.date()
    open_dt = _combine_local_datetime(local_day, schedule["open_local"], timezone_name)
    close_dt = _combine_local_datetime(local_day, schedule["close_local"], timezone_name)
    is_trading_day = local_now.strftime("%a") in trading_days_set
    is_open_now = bool(is_trading_day and open_dt <= local_now < close_dt)

    if is_open_now:
        next_close = close_dt
        next_open_day = _next_trading_day(local_day + timedelta(days=1), trading_days_set)
        next_open = _combine_local_datetime(next_open_day, schedule["open_local"], timezone_name)
    elif is_trading_day and local_now < open_dt:
        next_open = open_dt
        next_close = close_dt
    else:
        next_open_day = _next_trading_day(local_day + timedelta(days=1), trading_days_set)
        next_open = _combine_local_datetime(next_open_day, schedule["open_local"], timezone_name)
        next_close = _combine_local_datetime(next_open_day, schedule["close_local"], timezone_name)

    return {
        "exchange": normalized_exchange,
        "timezone": timezone_name,
        "open_local": schedule["open_local"],
        "close_local": schedule["close_local"],
        "is_open_now": is_open_now,
        "next_open": next_open.isoformat(),
        "next_close": next_close.isoformat(),
        "trading_days": trading_days,
    }


def _normalize_symbol(symbol: Any) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol is required")
    return normalized


def _normalize_query_text(value: Any, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _is_probable_crypto_request(symbol: Any, exchange: Any) -> bool:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_exchange = str(exchange or "").strip().upper()
    if normalized_exchange in _KNOWN_CRYPTO_EXCHANGES:
        return True
    if normalized_symbol in _KNOWN_CRYPTO_SYMBOLS:
        return True
    return normalized_symbol.endswith("-USD") or normalized_symbol.endswith("-USDT")


def _resolve_yfinance_symbol(symbol: Any, exchange: Any, currency: Any) -> tuple[str, bool]:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_exchange = _normalize_exchange(exchange)
    normalized_currency = str(currency or "").strip().upper()

    explicit_match = _KNOWN_EU_YF_TICKERS.get((normalized_symbol, normalized_exchange, normalized_currency))
    if explicit_match:
        return explicit_match, True

    suffix = _YF_EXCHANGE_SUFFIX.get(normalized_exchange)
    if suffix:
        return f"{normalized_symbol}{suffix}", True
    return normalized_symbol, False


def _fundamentals_source_note(is_european_equity: bool) -> str:
    return EU_FUNDAMENTALS_SOURCE_NOTE if is_european_equity else FUNDAMENTALS_SOURCE_NOTE


def _serialize_yf_scalar(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes, dict, list, tuple, set)):
        try:
            return _serialize_yf_scalar(value.item())
        except Exception:
            pass
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            pass
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (list, tuple, set)):
        return [_serialize_yf_scalar(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize_yf_scalar(item) for key, item in value.items()}
    number = _normalize_number(value)
    if number is not None:
        return number
    text = str(value).strip()
    if text.lower() in {"", "nan", "nat", "none"}:
        return None
    return text


def _serialize_yf_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            serialized = _serialize_yf_date(item)
            if serialized is not None:
                return serialized
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            pass
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            pass
    number = _normalize_number(value)
    if number is not None and number > 100000:
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            pass
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "nat", "none"}:
        return None
    if " " in text:
        text = text.split(" ", 1)[0]
    return text


def _flatten_yf_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        raw_mapping = value
    elif hasattr(value, "to_dict"):
        try:
            raw_mapping = value.to_dict()
        except Exception:
            return {}
    else:
        return {}

    flattened: dict[str, Any] = {}
    for key, item in raw_mapping.items():
        normalized_key = str(key).strip().lower().replace(" ", "_")
        if isinstance(item, dict):
            chosen_value = None
            for nested in item.values():
                serialized = _serialize_yf_scalar(nested)
                if serialized not in (None, "", [], {}):
                    chosen_value = serialized
                    break
            flattened[normalized_key] = chosen_value
        else:
            flattened[normalized_key] = _serialize_yf_scalar(item)
    return flattened


def _serialize_statement_frame(frame: Any) -> Optional[dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        columns = list(frame.columns)
    except Exception:
        return None
    if not columns:
        return None
    latest_column = columns[0]
    try:
        latest_series = frame[latest_column]
    except Exception:
        return None

    values: dict[str, Any] = {}
    if hasattr(latest_series, "items"):
        for label, cell in latest_series.items():
            serialized = _serialize_yf_scalar(cell)
            if serialized is not None:
                values[str(label)] = serialized

    return {
        "period": _serialize_yf_date(latest_column) or str(latest_column),
        "values": values,
    }


def _fetch_yfinance_snapshot_sync(yf_symbol: str) -> dict[str, Any]:
    log: list[str] = []
    ticker = yf.Ticker(yf_symbol)

    try:
        info = dict(ticker.info or {})
    except Exception as exc:
        info = {}
        log.append(f"yfinance info fetch failed: {exc}")

    try:
        calendar = ticker.calendar
    except Exception as exc:
        calendar = None
        log.append(f"yfinance calendar fetch failed: {exc}")

    try:
        financials = ticker.financials
    except Exception as exc:
        financials = None
        log.append(f"yfinance financials fetch failed: {exc}")

    try:
        balance_sheet = ticker.balance_sheet
    except Exception as exc:
        balance_sheet = None
        log.append(f"yfinance balance_sheet fetch failed: {exc}")

    try:
        cashflow = ticker.cashflow
    except Exception as exc:
        cashflow = None
        log.append(f"yfinance cashflow fetch failed: {exc}")

    return {
        "info": info,
        "calendar": _flatten_yf_mapping(calendar),
        "financials_latest": _serialize_statement_frame(financials),
        "balance_sheet_latest": _serialize_statement_frame(balance_sheet),
        "cashflow_latest": _serialize_statement_frame(cashflow),
        "log": log,
    }


def _extract_yf_earnings_data(snapshot: dict[str, Any], log: list[str]) -> dict[str, Any]:
    info = snapshot.get("info", {}) or {}
    calendar = snapshot.get("calendar", {}) or {}
    next_earnings_date = _serialize_yf_date(
        calendar.get("earnings_date")
        or info.get("earningsDate")
        or info.get("earningsTimestampStart")
    )
    consensus_eps = _normalize_number(calendar.get("earnings_average") or calendar.get("eps_average"))
    consensus_revenue = _normalize_number(calendar.get("revenue_average") or calendar.get("sales_average"))
    earnings_time = None

    if next_earnings_date is not None:
        try:
            parsed_earnings_date = datetime.fromisoformat(next_earnings_date).date()
            if parsed_earnings_date <= datetime.now(timezone.utc).date():
                log.append("yfinance calendar earnings date is not in the future")
                next_earnings_date = None
        except ValueError:
            pass

    if next_earnings_date is None:
        log.append("missing next earnings date from yfinance calendar")
    if consensus_eps is None:
        log.append("missing consensus EPS from yfinance calendar")
    if consensus_revenue is None:
        log.append("missing consensus revenue from yfinance calendar")
    if earnings_time is None:
        log.append("missing earnings time from yfinance calendar")

    return {
        "next_earnings_date": next_earnings_date,
        "earnings_time": earnings_time,
        "consensus_eps": _round_number(consensus_eps),
        "consensus_revenue": _round_number(consensus_revenue),
    }


def _extract_yf_dividend_data(snapshot: dict[str, Any], log: list[str]) -> dict[str, Any]:
    info = snapshot.get("info", {}) or {}
    calendar = snapshot.get("calendar", {}) or {}

    annual_dividend = _normalize_number(info.get("dividendRate") or info.get("lastDividendValue"))
    yield_pct = _normalize_number(info.get("dividendYield"))
    if yield_pct is not None and yield_pct <= 1:
        yield_pct *= 100.0
    ex_date_next = _serialize_yf_date(info.get("exDividendDate") or calendar.get("ex_dividend_date") or calendar.get("ex_date"))
    pay_date_next = _serialize_yf_date(info.get("dividendDate") or calendar.get("dividend_date") or calendar.get("pay_date"))
    payout_ratio = _normalize_number(info.get("payoutRatio"))

    if annual_dividend is None:
        log.append("missing annual dividend from yfinance info")
    if yield_pct is None:
        log.append("missing dividend yield from yfinance info")
    if ex_date_next is None:
        log.append("missing next ex-dividend date from yfinance info")
    if pay_date_next is None:
        log.append("missing next dividend pay date from yfinance info")
    if payout_ratio is None:
        log.append("missing payout ratio from yfinance info")

    return {
        "annual_dividend": _round_number(annual_dividend),
        "yield_pct": _round_number(yield_pct),
        "ex_date_next": ex_date_next,
        "pay_date_next": pay_date_next,
        "payout_ratio": _round_number(payout_ratio),
    }


def _build_yfinance_response(symbol: str, data: dict[str, Any], log: list[str], source_note: str) -> dict[str, Any]:
    return {
        "symbol": _normalize_symbol(symbol),
        "source": "yfinance",
        "as_of": datetime.now(timezone.utc).date().isoformat(),
        "data": data,
        "source_note": source_note,
        "log": log,
    }


def _last_error() -> Optional[dict[str, Any]]:
    return _recent_errors[-1] if _recent_errors else None


def _round_number(value: Any, digits: int = 6) -> Optional[float]:
    number = _normalize_number(value)
    if number is None:
        return None
    return round(number, digits)


def _normalize_option_right(right: str) -> str:
    normalized = str(right or "").upper()
    if normalized not in {"C", "P"}:
        raise ValueError("right must be 'C' or 'P'")
    return normalized


def _require_positive(name: str, value: Any) -> float:
    number = _normalize_number(value)
    if number is None or number <= 0:
        raise ValueError(f"{name} must be > 0")
    return number


def _bs_metrics_raw(
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    volatility: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: str = "C",
) -> dict[str, float]:
    spot = _require_positive("spot", spot)
    strike = _require_positive("strike", strike)
    time_to_expiry_years = _require_positive("time_to_expiry_years", time_to_expiry_years)
    volatility = _require_positive("volatility", volatility)
    risk_free_rate = _normalize_number(risk_free_rate) or 0.0
    dividend_yield = _normalize_number(dividend_yield) or 0.0
    right = _normalize_option_right(right)

    sqrt_t = float(np.sqrt(time_to_expiry_years))
    d1 = (
        float(np.log(spot / strike))
        + (risk_free_rate - dividend_yield + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / (volatility * sqrt_t)
    d2 = d1 - volatility * sqrt_t
    discount_q = float(np.exp(-dividend_yield * time_to_expiry_years))
    discount_r = float(np.exp(-risk_free_rate * time_to_expiry_years))
    pdf_d1 = float(norm.pdf(d1))

    if right == "C":
        price = spot * discount_q * float(norm.cdf(d1)) - strike * discount_r * float(norm.cdf(d2))
        delta = discount_q * float(norm.cdf(d1))
        theta = (
            -(spot * discount_q * pdf_d1 * volatility) / (2 * sqrt_t)
            - risk_free_rate * strike * discount_r * float(norm.cdf(d2))
            + dividend_yield * spot * discount_q * float(norm.cdf(d1))
        )
        rho = strike * time_to_expiry_years * discount_r * float(norm.cdf(d2))
    else:
        price = strike * discount_r * float(norm.cdf(-d2)) - spot * discount_q * float(norm.cdf(-d1))
        delta = -discount_q * float(norm.cdf(-d1))
        theta = (
            -(spot * discount_q * pdf_d1 * volatility) / (2 * sqrt_t)
            + risk_free_rate * strike * discount_r * float(norm.cdf(-d2))
            - dividend_yield * spot * discount_q * float(norm.cdf(-d1))
        )
        rho = -strike * time_to_expiry_years * discount_r * float(norm.cdf(-d2))

    gamma = (discount_q * pdf_d1) / (spot * volatility * sqrt_t)
    vega = spot * discount_q * pdf_d1 * sqrt_t
    return {
        "price": price,
        "d1": d1,
        "d2": d2,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "rho": rho,
    }


def _serialize_bs_metrics(metrics: dict[str, float]) -> dict[str, float]:
    return {key: round(value, 6) for key, value in metrics.items()}


def _solve_implied_volatility(
    option_price: float,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: str = "C",
) -> float:
    option_price = _require_positive("option_price", option_price)
    spot = _require_positive("spot", spot)
    strike = _require_positive("strike", strike)
    time_to_expiry_years = _require_positive("time_to_expiry_years", time_to_expiry_years)
    risk_free_rate = _normalize_number(risk_free_rate) or 0.0
    dividend_yield = _normalize_number(dividend_yield) or 0.0
    right = _normalize_option_right(right)

    def objective(volatility_guess: float) -> float:
        return (
            _bs_metrics_raw(
                spot,
                strike,
                time_to_expiry_years,
                volatility_guess,
                risk_free_rate,
                dividend_yield,
                right,
            )["price"]
            - option_price
        )

    lower = 1e-6
    upper = 5.0
    f_lower = objective(lower)
    f_upper = objective(upper)
    while f_lower * f_upper > 0 and upper < 20.0:
        upper *= 2.0
        f_upper = objective(upper)
    if f_lower * f_upper > 0:
        raise ValueError("no convergence")
    return float(brentq(objective, lower, upper, maxiter=200))


def _normalize_leg(side: str, right: str, strike: Any, quantity: Any) -> dict[str, Any]:
    normalized_side = str(side or "").upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError("leg side must be BUY or SELL")
    normalized_right = _normalize_option_right(right)
    normalized_strike = _require_positive("leg strike", strike)
    normalized_quantity = _normalize_number(quantity)
    if normalized_quantity is None or normalized_quantity <= 0 or not float(normalized_quantity).is_integer():
        raise ValueError("leg quantity must be a positive integer")
    return {
        "side": normalized_side,
        "right": normalized_right,
        "strike": normalized_strike,
        "quantity": int(normalized_quantity),
        "sign": 1 if normalized_side == "BUY" else -1,
    }


def _option_intrinsic_value(underlying_price: float, strike: float, right: str) -> float:
    if right == "C":
        return max(underlying_price - strike, 0.0)
    return max(strike - underlying_price, 0.0)


def _expiry_pnl_for_legs(underlying_price: float, legs: list[dict[str, Any]], net_debit_credit: float) -> float:
    intrinsic_value = 0.0
    for leg in legs:
        intrinsic_value += leg["sign"] * leg["quantity"] * _option_intrinsic_value(underlying_price, leg["strike"], leg["right"])
    return intrinsic_value - net_debit_credit


def _format_bound(value: float, infinite: bool) -> float | str:
    if infinite:
        return "infinite"
    return round(value, 6)


def _position_matches(
    row: dict[str, Any],
    symbol: str,
    sec_type: str,
    currency: str,
    exchange: str,
) -> bool:
    row_symbol = str(row.get("symbol") or "").upper()
    row_sec_type = str(row.get("secType") or "").upper()
    row_currency = str(row.get("currency") or "").upper()
    row_exchange = str(row.get("exchange") or "").upper()
    target_exchange = _normalize_exchange(exchange)
    if row_symbol != str(symbol or "").upper():
        return False
    if row_sec_type != str(sec_type or "").upper():
        return False
    if currency and row_currency != str(currency).upper():
        return False
    if target_exchange != "SMART" and row_exchange and row_exchange != target_exchange:
        return False
    return True


def _reference_price_from_ticker(ticker: Any) -> tuple[Optional[float], Optional[str]]:
    bid = _normalize_number(getattr(ticker, "bid", None))
    ask = _normalize_number(getattr(ticker, "ask", None))
    last = _normalize_number(getattr(ticker, "last", None))
    close = _normalize_number(getattr(ticker, "close", None))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0, "mid"
    if last is not None:
        return last, "last"
    if close is not None:
        return close, "close"
    if bid is not None:
        return bid, "bid"
    if ask is not None:
        return ask, "ask"
    return None, None


def _normalize_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _normalize_pnl_number(value: Any) -> Optional[float]:
    number = _normalize_number(value)
    if number is None:
        return None
    # IB can use max-float sentinels for unavailable PnL fields.
    if abs(number) >= sys.float_info.max / 10:
        return None
    return number


def _position_multiplier(contract: Any) -> float:
    multiplier = _normalize_number(getattr(contract, "multiplier", None))
    return multiplier if multiplier not in {None, 0} else 1.0


def _serialize_strategy_position(position: Any, pnl_row: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    contract = getattr(position, "contract", None)
    row = dict(pnl_row or {})
    row.update(
        {
            "account": getattr(position, "account", None),
            "symbol": getattr(contract, "symbol", None),
            "secType": getattr(contract, "secType", None),
            "exchange": getattr(contract, "exchange", None) or getattr(contract, "primaryExchange", None),
            "primaryExchange": getattr(contract, "primaryExchange", None),
            "currency": getattr(contract, "currency", None),
            "conId": getattr(contract, "conId", None),
            "localSymbol": getattr(contract, "localSymbol", None),
            "position": _round_number(getattr(position, "position", None)),
            "avgCost": _round_number(getattr(position, "avgCost", None)),
            "right": getattr(contract, "right", None) or None,
            "strike": _round_number(getattr(contract, "strike", None)),
            "expiry": getattr(contract, "lastTradeDateOrContractMonth", None) or None,
            "multiplier": _round_number(_position_multiplier(contract)),
        }
    )
    return row


def _strategy_position_notional(position_row: dict[str, Any]) -> float:
    market_value = _normalize_number(position_row.get("marketValue"))
    if market_value is not None:
        return round(abs(market_value), 6)

    quantity = abs(_normalize_number(position_row.get("position")) or 0.0)
    if quantity == 0:
        return 0.0

    sec_type = str(position_row.get("secType") or "").upper()
    multiplier = _normalize_number(position_row.get("multiplier")) or 1.0
    if sec_type == "OPT":
        strike = _normalize_number(position_row.get("strike"))
        if strike is not None:
            return round(abs(strike * multiplier * quantity), 6)

    avg_cost = _normalize_number(position_row.get("avgCost"))
    if avg_cost is not None:
        return round(abs(avg_cost * quantity), 6)
    return 0.0


def _append_strategy_position(
    strategies: dict[str, dict[str, Any]],
    strategy_name: str,
    position_row: dict[str, Any],
) -> None:
    bucket = strategies[strategy_name]
    bucket["positions"].append(position_row)
    bucket["notional"] = round(bucket["notional"] + _strategy_position_notional(position_row), 6)


def _scanner_profile_for_exchange(exchange: Any) -> dict[str, str]:
    normalized_exchange = _normalize_exchange(exchange)
    profile = _SCANNER_LOCATION_BY_EXCHANGE.get(normalized_exchange)
    if profile is None:
        supported = ", ".join(sorted(_SCANNER_LOCATION_BY_EXCHANGE))
        raise ValueError(f"unsupported scanner exchange: {normalized_exchange}; supported: {supported}")
    return profile


def _scanner_scan_value(scan_row: Any) -> Any:
    for field_name in ("distance", "projection", "benchmark", "legsStr"):
        value = getattr(scan_row, field_name, None)
        if value not in {None, ""}:
            return value
    return None


def _scanner_result_matches_exchange(scan_row: Any, requested_exchange: Any) -> bool:
    normalized_exchange = _normalize_exchange(requested_exchange)
    if normalized_exchange == "SMART":
        return True

    contract_details = getattr(scan_row, "contractDetails", None)
    contract = getattr(contract_details, "contract", None)
    primary_exchange = str(getattr(contract, "primaryExchange", "") or "").upper()
    contract_exchange = str(getattr(contract, "exchange", "") or "").upper()
    if normalized_exchange in {primary_exchange, contract_exchange}:
        return True

    market_name = str(getattr(contract_details, "marketName", "") or "").upper()
    return market_name in _SCANNER_MARKET_NAMES_BY_EXCHANGE.get(normalized_exchange, set())


def _sorted_strategy_rows(strategies: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    order = [
        "long_stocks",
        "short_stocks",
        "covered_calls",
        "cash_secured_puts",
        "long_calls",
        "long_puts",
        "spreads",
        "other",
    ]
    rows: list[dict[str, Any]] = []
    for name in order:
        bucket = strategies[name]
        bucket["positions"].sort(
            key=lambda row: (
                str(row.get("symbol") or ""),
                str(row.get("secType") or ""),
                str(row.get("right") or ""),
                _normalize_number(row.get("strike")) or 0.0,
            )
        )
        rows.append(bucket)
    return rows


def _to_iso_string(value: Any) -> str:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    return str(value)


def _serialize_bar(bar: Any) -> dict[str, Any]:
    return {
        "time": _to_iso_string(getattr(bar, "date", None) or getattr(bar, "time", None)),
        "open": _normalize_number(getattr(bar, "open", None)),
        "high": _normalize_number(getattr(bar, "high", None)),
        "low": _normalize_number(getattr(bar, "low", None)),
        "close": _normalize_number(getattr(bar, "close", None)),
        "volume": _normalize_number(getattr(bar, "volume", None)),
    }


def _ticker_has_price(ticker: Any) -> bool:
    return any(
        _normalize_number(getattr(ticker, field, None)) is not None
        for field in ("bid", "ask", "last", "close")
    )


def _ticker_has_greeks(ticker: Any) -> bool:
    greeks = getattr(ticker, "modelGreeks", None) or getattr(ticker, "lastGreeks", None)
    if greeks is None:
        return False
    return any(
        _normalize_number(getattr(greeks, field, None)) is not None
        for field in ("optPrice", "impliedVol", "delta", "gamma", "theta", "vega", "undPrice")
    )


def _recent_errors_since(start_index: int) -> list[dict[str, Any]]:
    return list(_recent_errors)[start_index:]


def _find_market_data_permission_error(errors: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    for entry in reversed(errors):
        code = entry.get("code")
        message = str(entry.get("message", "")).lower()
        if code in {354, 10089, 10090, 10167, 10168, 10358}:
            return entry
        if "not subscribed" in message or "subscription manager" in message:
            return entry
        if "additional subscription" in message or "delayed market data is available" in message:
            return entry
        if "fundamentals data is not allowed" in message:
            return entry
        if "market data" in message and "permission" in message:
            return entry
    return None


async def _request_market_data_ticker(
    ib: IB,
    contract: Any,
    generic_tick_list: str = "",
    wait_seconds: float = 3.0,
    ready_predicate=None,
) -> tuple[Any, list[dict[str, Any]]]:
    errors_before = len(_recent_errors)
    _track_market_data_subscription(contract)
    ticker = ib.reqMktData(contract, generic_tick_list, False, False)
    try:
        attempts = max(1, int(wait_seconds / 0.25))
        for _ in range(attempts):
            if ready_predicate is not None and ready_predicate(ticker):
                break
            await asyncio.sleep(0.25)
        return ticker, _recent_errors_since(errors_before)
    finally:
        ib.cancelMktData(contract)
        _untrack_market_data_subscription(contract)


_market_data_type_lock = asyncio.Lock()


async def _request_regulatory_snapshot(
    ib: IB,
    contract: Any,
    retries: int = 3,
) -> tuple[Any, list[dict[str, Any]]]:
    """Prezzo real-time on-demand via regulatory snapshot NBBO (entitlement snapshot,
    ~0.01$/chiamata). Usato SOLO dal price gate / precheck. Ritenta sull'Internal
    server error 2147483647 transitorio; in caso di fallimento totale ritorna
    ticker=None e il chiamante blocca l'ordine (fail-safe, nessun fallback su delayed)."""
    errors_before = len(_recent_errors)
    ticker = None
    last_exception = None
    async with _market_data_type_lock:
        try:
            ib.reqMarketDataType(1)
            for attempt in range(1, retries + 1):
                try:
                    tickers = await asyncio.wait_for(
                        ib.reqTickersAsync(contract, regulatorySnapshot=True),
                        timeout=20,
                    )
                    if tickers:
                        ticker = tickers[0]
                        prices = [
                            _normalize_number(getattr(ticker, f, None))
                            for f in ("bid", "ask", "last", "close")
                        ]
                        if any(p and p > 0 for p in prices):
                            break
                except Exception as exc:
                    last_exception = repr(exc)
                if attempt < retries:
                    await asyncio.sleep(1.5 * attempt)
            if ticker is None and last_exception is not None:
                print(
                    "[regulatory_snapshot] retry esauriti per %s: %s"
                    % (getattr(contract, "symbol", contract), last_exception),
                    file=sys.stderr,
                )
            return ticker, _recent_errors_since(errors_before)
        finally:
            ib.reqMarketDataType(MARKET_DATA_TYPE)


async def _fetch_historical_bars(
    ib: IB,
    contract: Any,
    duration: str,
    bar_size: str,
    what_to_show: str,
    end_date_time: str = "",
):
    return await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=end_date_time or "",
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=True,
        formatDate=1,
        keepUpToDate=False,
    )


async def _wait_for(predicate, timeout_seconds: float, interval_seconds: float = 0.25) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_seconds)
    return predicate()


def _build_contract(
    symbol: str,
    exchange: str,
    currency: str,
    sec_type: str,
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[str] = None,
):
    if sec_type == "STK":
        return Stock(symbol, exchange, currency), None
    if sec_type == "OPT":
        if not expiry or strike is None or not right:
            return None, {"error": "OPT requires expiry, strike, right"}
        return Option(symbol, expiry, strike, right, exchange, currency=currency), None
    return None, {"error": f"unsupported sec_type: {sec_type}"}


async def _qualify_contract(
    ib: IB,
    symbol: str,
    exchange: str,
    currency: str,
    sec_type: str,
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[str] = None,
):
    contract, error = _build_contract(symbol, exchange, currency, sec_type, expiry, strike, right)
    if error:
        return None, error
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified or qualified[0].conId == 0:
        return None, {"error": f"contract not resolved: {symbol} {sec_type} on {exchange}"}
    return qualified[0], None


def _resolve_account(ib: IB, requested_account: Optional[str], positions: Optional[list[Any]] = None) -> Optional[str]:
    if requested_account:
        return requested_account
    managed_accounts = _get_managed_accounts(ib)
    if managed_accounts:
        return managed_accounts[0]
    for position in positions or []:
        account = getattr(position, "account", None)
        if account:
            return str(account)
    return None


def _account_summary_number(summary_items: list[Any], tags: tuple[str, ...]) -> Optional[float]:
    for preferred_base in (True, False):
        for item in summary_items:
            if getattr(item, "tag", None) not in tags:
                continue
            currency = str(getattr(item, "currency", "") or "")
            if preferred_base and currency not in {"", "BASE"}:
                continue
            number = _normalize_number(getattr(item, "value", None))
            if number is not None:
                return number
    return None


def _extract_cash_balances(summary_items: list[Any]) -> dict[str, float]:
    cash: dict[str, float] = {}
    for tag in ("TotalCashValue", "CashBalance"):
        for item in summary_items:
            if getattr(item, "tag", None) != tag:
                continue
            currency = str(getattr(item, "currency", "") or "")
            number = _normalize_number(getattr(item, "value", None))
            if number is None or not currency:
                continue
            cash[currency] = round(number, 6)
        if cash:
            return cash
    return cash


def _infer_sector(symbol: Any, sec_type: Any) -> Optional[str]:
    if str(sec_type or "").upper() != "STK":
        return None
    normalized_symbol = str(symbol or "").upper()
    return _KNOWN_SECTOR_BY_SYMBOL.get(normalized_symbol)


def _serialize_position_with_pnl(position: Any, pnl_single: Any) -> dict[str, Any]:
    contract = getattr(position, "contract", None)
    position_size = _normalize_number(getattr(pnl_single, "position", None))
    if position_size is None:
        position_size = _normalize_number(getattr(position, "position", None))
    market_value = _normalize_number(getattr(pnl_single, "value", None))
    market_price = None
    if position_size not in {None, 0} and market_value is not None:
        market_price = market_value / position_size
    return {
        "account": getattr(position, "account", None),
        "symbol": getattr(contract, "symbol", None),
        "secType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None) or getattr(contract, "primaryExchange", None),
        "currency": getattr(contract, "currency", None),
        "conId": getattr(contract, "conId", None),
        "position": _round_number(position_size),
        "avgCost": _round_number(getattr(position, "avgCost", None)),
        "marketPrice": _round_number(market_price),
        "marketValue": _round_number(market_value),
        "unrealizedPnL": _round_number(_normalize_pnl_number(getattr(pnl_single, "unrealizedPnL", None))),
        "realizedPnL": _round_number(_normalize_pnl_number(getattr(pnl_single, "realizedPnL", None))),
        "dailyPnL": _round_number(_normalize_pnl_number(getattr(pnl_single, "dailyPnL", None))),
    }


async def _collect_pnl_per_position(
    ib: IB,
    requested_account: Optional[str] = None,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    await ib.reqPositionsAsync()
    all_positions = list(ib.positions())
    resolved_account = _resolve_account(ib, requested_account, all_positions)
    positions = [
        position
        for position in all_positions
        if resolved_account is None or getattr(position, "account", None) == resolved_account
    ]
    if not positions:
        return [], resolved_account

    subscriptions: list[tuple[Any, Any]] = []
    try:
        for position in positions:
            contract = getattr(position, "contract", None)
            con_id = getattr(contract, "conId", None)
            if not con_id:
                continue
            subscriptions.append((position, ib.reqPnLSingle(position.account, "", con_id)))

        if subscriptions:
            await _wait_for(
                lambda: all(
                    any(
                        _normalize_number(getattr(pnl_single, field, None)) is not None
                        for field in ("value", "dailyPnL", "unrealizedPnL", "realizedPnL")
                    )
                    for _, pnl_single in subscriptions
                ),
                timeout_seconds=3.0,
                interval_seconds=0.25,
            )

        rows = [_serialize_position_with_pnl(position, pnl_single) for position, pnl_single in subscriptions]
        rows.sort(key=lambda row: (str(row.get("symbol") or ""), str(row.get("secType") or "")))
        return rows, resolved_account
    finally:
        for position, _ in subscriptions:
            contract = getattr(position, "contract", None)
            con_id = getattr(contract, "conId", None)
            if not con_id:
                continue
            try:
                ib.cancelPnLSingle(position.account, "", con_id)
            except Exception:
                pass


async def _get_reference_price_for_precheck(
    ib: IB,
    symbol: str,
    exchange: str,
    currency: str,
    sec_type: str,
    account: Optional[str] = None,
    pnl_rows: Optional[list[dict[str, Any]]] = None,
) -> tuple[Optional[float], Optional[str], Optional[dict[str, Any]]]:
    contract, error = await _qualify_contract(ib, symbol, exchange, currency, sec_type)
    if error:
        return None, None, error

    ticker, recent_errors = await _request_regulatory_snapshot(ib, contract)
    market_price, market_source = _reference_price_from_ticker(ticker)
    if market_price is not None:
        return market_price, f"market_data_{market_source}", None

    rows = pnl_rows
    if rows is None:
        rows, _ = await _collect_pnl_per_position(ib, account)
    for row in rows:
        if _position_matches(row, symbol, sec_type, currency, exchange):
            position_price = _normalize_number(row.get("marketPrice"))
            if position_price is not None:
                return position_price, "portfolio_position_market_price", None

    try:
        bars = await _fetch_historical_bars(ib, contract, "5 D", "1 day", "TRADES")
    except Exception:
        bars = []
    if bars:
        historical_close = _normalize_number(getattr(bars[-1], "close", None))
        if historical_close is not None:
            return historical_close, "historical_daily_close", None

    return None, None, _find_market_data_permission_error(recent_errors)

ORDER_PRICE_TOLERANCE_PCT = float(os.environ.get("IBKR_ORDER_PRICE_TOLERANCE_PCT", "2.0"))
MARKET_DATA_TYPE = int(os.environ.get("IBKR_MARKET_DATA_TYPE", "3"))


async def _precheck_live_price(ib, contract, side, order_type, limit_price):
    ticker, recent_errors = await _request_regulatory_snapshot(ib, contract)
    perm_err = _find_market_data_permission_error(recent_errors)
    bid = _normalize_number(getattr(ticker, "bid", None))
    ask = _normalize_number(getattr(ticker, "ask", None))
    last = _normalize_number(getattr(ticker, "last", None))
    close = _normalize_number(getattr(ticker, "close", None))
    if bid and ask and bid > 0 and ask > 0:
        ref = (bid + ask) / 2
    elif last and last > 0:
        ref = last
    elif close and close > 0:
        ref = close
    else:
        ref = None
    quote = {"bid": bid, "ask": ask, "last": last, "close": close}
    if ref is None:
        return {
            "error": "BLOCKED: no live price available, refusing to place order",
            "quote": quote,
            "permission_error": perm_err,
        }
    if order_type == "LMT" and limit_price is not None:
        lp = _normalize_number(limit_price)
        if lp is not None and lp > 0:
            dev = abs(lp - ref) / ref
            if dev > ORDER_PRICE_TOLERANCE_PCT / 100:
                return {
                    "error": "BLOCKED: limit price too far from live price",
                    "limit_price": lp,
                    "reference_price": round(ref, 4),
                    "deviation_pct": round(dev * 100, 2),
                    "tolerance_pct": ORDER_PRICE_TOLERANCE_PCT,
                    "quote": quote,
                }
    return None


async def get_ib() -> IB:
    global _ib
    if _ib is None:
        _ib = IB()
        _ensure_ib_event_handlers(_ib)
    else:
        _ensure_ib_event_handlers(_ib)
    if not _ib.isConnected():
        await _ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=15)
    _ib.reqMarketDataType(MARKET_DATA_TYPE)
    return _ib

mcp = FastMCP("ibkr-mcp-server")

@mcp.tool()
async def place_order(
    symbol: str,
    side: Literal["BUY", "SELL"],
    quantity: float,
    order_type: Literal["LMT", "MKT", "STP"] = "LMT",
    limit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    exchange: str = "SMART",
    currency: str = "USD",
    sec_type: Literal["STK", "OPT"] = "STK",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[Literal["C", "P"]] = None,
    tif: Literal["DAY", "GTC", "IOC"] = "DAY",
    outside_rth: bool = False,
    wait_seconds: int = 5,
) -> dict:
    """Place a stock or option order. Returns full status and log including all IB error codes."""
    ib = await get_ib()
    if sec_type == "STK":
        contract = Stock(symbol, exchange, currency)
    else:
        if not (expiry and strike and right):
            return {"error": "OPT requires expiry, strike, right"}
        contract = Option(symbol, expiry, strike, right, exchange, currency=currency)
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified or qualified[0].conId == 0:
        return {"error": f"contract not resolved: {symbol} {sec_type} on {exchange}"}
    gate = await _precheck_live_price(ib, contract, side, order_type, limit_price)
    if gate is not None:
        return gate
    if order_type == "LMT":
        if limit_price is None:
            return {"error": "LMT requires limit_price"}
        order = LimitOrder(side, quantity, limit_price)
    elif order_type == "MKT":
        order = MarketOrder(side, quantity)
    elif order_type == "STP":
        if stop_price is None:
            return {"error": "STP requires stop_price"}
        order = StopOrder(side, quantity, stop_price)
    order.tif = tif
    order.outsideRth = outside_rth
    trade: Trade = ib.placeOrder(contract, order)
    for _ in range(wait_seconds * 2):
        await asyncio.sleep(0.5)
        if trade.orderStatus.status == "Filled":
            break
    return {
        "orderId": trade.order.orderId,
        "status": trade.orderStatus.status,
        "filled": trade.orderStatus.filled,
        "remaining": trade.orderStatus.remaining,
        "avgFillPrice": trade.orderStatus.avgFillPrice,
        "whyHeld": trade.orderStatus.whyHeld,
        "log": [
            {"time": str(e.time), "status": e.status, "errorCode": e.errorCode, "message": e.message}
            for e in trade.log
        ],
    }

@mcp.tool()
async def cancel_order(order_id: int) -> dict:
    ib = await get_ib()
    for trade in ib.openTrades():
        if trade.order.orderId == order_id:
            ib.cancelOrder(trade.order)
            return {"cancelled": True, "orderId": order_id}
    return {"cancelled": False, "error": f"orderId {order_id} not open"}

@mcp.tool()
async def get_positions() -> list:
    ib = await get_ib()
    await ib.reqPositionsAsync()
    return [
        {
            "account": p.account,
            "symbol": p.contract.symbol,
            "secType": p.contract.secType,
            "exchange": p.contract.exchange or p.contract.primaryExchange,
            "currency": p.contract.currency,
            "position": p.position,
            "avgCost": p.avgCost,
        }
        for p in ib.positions()
    ]

@mcp.tool()
async def get_live_orders(all_clients: bool = True) -> list:
    """Returns open orders. If all_clients=True, includes orders from other clientIds."""
    ib = await get_ib()
    if all_clients:
        trades = await ib.reqAllOpenOrdersAsync()
    else:
        trades = await ib.reqOpenOrdersAsync()
    return [
        {
            "orderId": t.order.orderId,
            "symbol": t.contract.symbol,
            "secType": t.contract.secType,
            "action": t.order.action,
            "quantity": t.order.totalQuantity,
            "orderType": t.order.orderType,
            "lmtPrice": t.order.lmtPrice,
            "tif": t.order.tif,
            "status": t.orderStatus.status,
            "filled": t.orderStatus.filled,
            "remaining": t.orderStatus.remaining,
            "whyHeld": t.orderStatus.whyHeld,
        }
        for t in trades
    ]

@mcp.tool()
async def get_account_summary() -> dict:
    ib = await get_ib()
    summary = await ib.accountSummaryAsync()
    return {
        item.tag + (f":{item.currency}" if item.currency else ""): item.value
        for item in summary
    }

@mcp.tool()
async def get_market_data(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    sec_type: Literal["STK", "OPT"] = "STK",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[Literal["C", "P"]] = None,
) -> dict:
    try:
        ib = await get_ib()
        contract, error = await _qualify_contract(
            ib,
            symbol,
            exchange,
            currency,
            sec_type,
            expiry,
            strike,
            right,
        )
        if error:
            return error

        ticker, recent_errors = await _request_market_data_ticker(
            ib,
            contract,
            wait_seconds=3.0,
            ready_predicate=_ticker_has_price,
        )
        permission_error = _find_market_data_permission_error(recent_errors)
        bid = _normalize_number(getattr(ticker, "bid", None))
        ask = _normalize_number(getattr(ticker, "ask", None))
        last = _normalize_number(getattr(ticker, "last", None))
        close = _normalize_number(getattr(ticker, "close", None))
        mdt = getattr(ticker, "marketDataType", None)
        has_usable_price = (
            (bid is not None and bid > 0 and ask is not None and ask > 0)
            or (last is not None and last > 0)
            or (close is not None and close > 0)
        )
        if not has_usable_price:
            if permission_error is not None:
                return {
                    "error": "no market data permission for symbol/exchange",
                    "details": permission_error,
                }
            return {"error": "no price available for symbol/exchange"}

        return {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "last": last,
            "close": close,
            "high": _normalize_number(getattr(ticker, "high", None)),
            "low": _normalize_number(getattr(ticker, "low", None)),
            "volume": _normalize_number(getattr(ticker, "volume", None)),
            "market_data_type": mdt,
            "delayed": mdt in (3, 4),
        }
    except Exception as exc:
        return {"error": str(exc)}

@mcp.tool()
async def get_option_chain(symbol: str, exchange: str = "SMART", currency: str = "USD") -> list:
    ib = await get_ib()
    underlying = Stock(symbol, exchange, currency)
    await ib.qualifyContractsAsync(underlying)
    chains = await ib.reqSecDefOptParamsAsync(
        underlyingSymbol=underlying.symbol,
        futFopExchange="",
        underlyingSecType=underlying.secType,
        underlyingConId=underlying.conId,
    )
    return [
        {
            "exchange": c.exchange,
            "tradingClass": c.tradingClass,
            "expirations": sorted(list(c.expirations)),
            "strikes": sorted(list(c.strikes)),
        }
        for c in chains
    ]


@mcp.tool()
async def get_historical_bars(
    symbol: str,
    duration: str = "1 D",
    bar_size: str = "1 hour",
    what_to_show: str = "TRADES",
    exchange: str = "SMART",
    currency: str = "USD",
    sec_type: Literal["STK", "OPT"] = "STK",
    end_date_time: str = "",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[Literal["C", "P"]] = None,
):
    tool_name = "get_historical_bars"
    _log_mcp(tool_name, f"request symbol={symbol} duration={duration} bar_size={bar_size}")
    try:
        ib = await get_ib()
        contract, error = await _qualify_contract(
            ib,
            symbol,
            exchange,
            currency,
            sec_type,
            expiry,
            strike,
            right,
        )
        if error:
            return error
        bars = await _fetch_historical_bars(ib, contract, duration, bar_size, what_to_show, end_date_time)
        return [_serialize_bar(bar) for bar in bars]
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_52w_context(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict:
    tool_name = "get_52w_context"
    _log_mcp(tool_name, f"request symbol={symbol} exchange={exchange}")
    try:
        ib = await get_ib()
        contract, error = await _qualify_contract(ib, symbol, exchange, currency, "STK")
        if error:
            return error

        bars = await _fetch_historical_bars(ib, contract, "1 Y", "1 day", "TRADES")
        serialized_bars = [_serialize_bar(bar) for bar in bars]
        valid_bars = [bar for bar in serialized_bars if bar["high"] is not None and bar["low"] is not None and bar["close"] is not None]
        if not valid_bars:
            return {"error": f"no historical bars returned for {symbol}"}

        latest_bar = valid_bars[-1]
        current = latest_bar["close"]
        high_52w = max(bar["high"] for bar in valid_bars)
        low_52w = min(bar["low"] for bar in valid_bars)
        as_of = latest_bar["time"]

        ticker, _ = await _request_market_data_ticker(
            ib,
            contract,
            wait_seconds=3.0,
            ready_predicate=_ticker_has_price,
        )
        for field in ("last", "close", "bid", "ask"):
            quote_value = _normalize_number(getattr(ticker, field, None))
            if quote_value is not None:
                current = quote_value
                as_of = datetime.now(timezone.utc).date().isoformat()
                break

        return {
            "current": current,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "pct_from_high": round(((current - high_52w) / high_52w) * 100, 4) if high_52w else None,
            "pct_from_low": round(((current - low_52w) / low_52w) * 100, 4) if low_52w else None,
            "as_of": as_of,
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_market_data_with_greeks(
    symbol: str,
    expiry: str,
    strike: float,
    right: Literal["C", "P"],
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict:
    tool_name = "get_market_data_with_greeks"
    _log_mcp(tool_name, f"request symbol={symbol} expiry={expiry} strike={strike} right={right}")
    if strike <= 0:
        return {"error": "strike must be > 0"}

    try:
        ib = await get_ib()
        contract, error = await _qualify_contract(
            ib,
            symbol,
            exchange,
            currency,
            "OPT",
            expiry,
            strike,
            right,
        )
        if error:
            return error

        ticker, recent_errors = await _request_market_data_ticker(
            ib,
            contract,
            generic_tick_list=OPTION_GENERIC_TICKS,
            wait_seconds=5.0,
            ready_predicate=_ticker_has_greeks,
        )

        permission_error = _find_market_data_permission_error(recent_errors)
        if permission_error is not None:
            return {
                "error": "no market data permission for symbol/exchange",
                "details": permission_error,
            }

        greeks = getattr(ticker, "modelGreeks", None) or getattr(ticker, "lastGreeks", None)
        if greeks is None:
            return {"error": "option greeks not available within timeout"}

        return {
            "bid": _normalize_number(getattr(ticker, "bid", None)),
            "ask": _normalize_number(getattr(ticker, "ask", None)),
            "last": _normalize_number(getattr(ticker, "last", None)),
            "modelPrice": _normalize_number(getattr(greeks, "optPrice", None)),
            "iv": _normalize_number(getattr(greeks, "impliedVol", None)),
            "delta": _normalize_number(getattr(greeks, "delta", None)),
            "gamma": _normalize_number(getattr(greeks, "gamma", None)),
            "theta": _normalize_number(getattr(greeks, "theta", None)),
            "vega": _normalize_number(getattr(greeks, "vega", None)),
            "undPrice": _normalize_number(getattr(greeks, "undPrice", None)),
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def bs_price(
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    volatility: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> dict:
    tool_name = "bs_price"
    _log_mcp(tool_name, f"request spot={spot} strike={strike} t={time_to_expiry_years} vol={volatility}")
    try:
        metrics = _bs_metrics_raw(
            spot,
            strike,
            time_to_expiry_years,
            volatility,
            risk_free_rate,
            dividend_yield,
            right,
        )
        return _serialize_bs_metrics(metrics)
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def implied_volatility(
    option_price: float,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> dict:
    tool_name = "implied_volatility"
    _log_mcp(tool_name, f"request option_price={option_price} spot={spot} strike={strike}")
    try:
        iv = _solve_implied_volatility(
            option_price,
            spot,
            strike,
            time_to_expiry_years,
            risk_free_rate,
            dividend_yield,
            right,
        )
        return {"iv": round(iv, 6)}
    except ValueError as exc:
        if str(exc) == "no convergence":
            return {"error": "no convergence"}
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def greeks_from_price(
    option_price: float,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> dict:
    tool_name = "greeks_from_price"
    _log_mcp(tool_name, f"request option_price={option_price} spot={spot} strike={strike}")
    try:
        iv = _solve_implied_volatility(
            option_price,
            spot,
            strike,
            time_to_expiry_years,
            risk_free_rate,
            dividend_yield,
            right,
        )
        metrics = _serialize_bs_metrics(
            _bs_metrics_raw(
                spot,
                strike,
                time_to_expiry_years,
                iv,
                risk_free_rate,
                dividend_yield,
                right,
            )
        )
        metrics["iv"] = round(iv, 6)
        return metrics
    except ValueError as exc:
        if str(exc) == "no convergence":
            return {"error": "no convergence"}
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def prob_itm(
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    volatility: float,
    risk_free_rate: float = 0.05,
    dividend_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> dict:
    tool_name = "prob_itm"
    _log_mcp(tool_name, f"request spot={spot} strike={strike} t={time_to_expiry_years} vol={volatility}")
    try:
        metrics = _bs_metrics_raw(
            spot,
            strike,
            time_to_expiry_years,
            volatility,
            risk_free_rate,
            dividend_yield,
            right,
        )
        normalized_right = _normalize_option_right(right)
        probability = float(norm.cdf(metrics["d2"])) if normalized_right == "C" else float(norm.cdf(-metrics["d2"]))
        return {"prob_itm": round(probability, 6)}
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def expected_move(
    spot: float,
    volatility: float,
    time_to_expiry_years: float,
) -> dict:
    tool_name = "expected_move"
    _log_mcp(tool_name, f"request spot={spot} vol={volatility} t={time_to_expiry_years}")
    try:
        spot = _require_positive("spot", spot)
        volatility = _require_positive("volatility", volatility)
        time_to_expiry_years = _require_positive("time_to_expiry_years", time_to_expiry_years)
        move = float(spot * volatility * np.sqrt(time_to_expiry_years))
        return {
            "expected_move": round(move, 6),
            "expected_up": round(move, 6),
            "expected_down": round(-move, 6),
            "expected_high": round(spot + move, 6),
            "expected_low": round(max(0.0, spot - move), 6),
        }
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def evaluate_multi_leg(
    legs: list[dict[str, Any]],
    spot: float,
    time_to_expiry_years: float,
    volatility: float,
    risk_free_rate: float = 0.05,
) -> dict:
    tool_name = "evaluate_multi_leg"
    _log_mcp(tool_name, f"request legs={len(legs) if isinstance(legs, list) else 'invalid'} spot={spot}")
    try:
        if not isinstance(legs, list) or not legs:
            return {"error": "legs must be a non-empty list"}

        spot = _require_positive("spot", spot)
        time_to_expiry_years = _require_positive("time_to_expiry_years", time_to_expiry_years)
        volatility = _require_positive("volatility", volatility)
        risk_free_rate = _normalize_number(risk_free_rate) or 0.0

        normalized_legs = [
            _normalize_leg(leg.get("side"), leg.get("right"), leg.get("strike"), leg.get("quantity"))
            for leg in legs
        ]

        premiums = []
        aggregate = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}
        for leg in normalized_legs:
            metrics = _bs_metrics_raw(
                spot,
                leg["strike"],
                time_to_expiry_years,
                volatility,
                risk_free_rate,
                0.0,
                leg["right"],
            )
            signed_quantity = leg["sign"] * leg["quantity"]
            premiums.append(signed_quantity * metrics["price"])
            for greek in aggregate:
                aggregate[greek] += signed_quantity * metrics[greek]

        net_debit_credit = float(np.sum(np.array(premiums, dtype=float)))
        strike_points = np.unique(np.array([0.0] + [leg["strike"] for leg in normalized_legs], dtype=float))
        max_strike = float(np.max(strike_points)) if strike_points.size else 0.0
        upper_probe = float(max(max_strike * 2.0, spot * 2.0, max_strike + 100.0, 100.0))
        candidate_points = sorted(float(point) for point in np.append(strike_points, upper_probe))
        candidate_payoffs = [
            _expiry_pnl_for_legs(point, normalized_legs, net_debit_credit)
            for point in candidate_points
        ]

        breakevens: list[float] = []
        for left, right_point in zip(candidate_points[:-1], candidate_points[1:]):
            left_value = _expiry_pnl_for_legs(left, normalized_legs, net_debit_credit)
            right_value = _expiry_pnl_for_legs(right_point, normalized_legs, net_debit_credit)
            if abs(left_value) < 1e-9:
                root = round(left, 6)
                if root not in breakevens:
                    breakevens.append(root)
            if abs(right_value) < 1e-9:
                root = round(right_point, 6)
                if root not in breakevens:
                    breakevens.append(root)
            if left_value * right_value < 0:
                root = left - left_value * (right_point - left) / (right_value - left_value)
                rounded_root = round(root, 6)
                if rounded_root not in breakevens:
                    breakevens.append(rounded_root)

        tail_left = max_strike
        tail_left_value = _expiry_pnl_for_legs(tail_left, normalized_legs, net_debit_credit)
        tail_right = upper_probe * 2.0 + 1.0
        tail_right_value = _expiry_pnl_for_legs(tail_right, normalized_legs, net_debit_credit)
        tail_slope = (tail_right_value - tail_left_value) / (tail_right - tail_left if tail_right != tail_left else 1.0)
        if abs(tail_slope) > 1e-9:
            tail_root = tail_left - (tail_left_value / tail_slope)
            rounded_tail_root = round(tail_root, 6)
            if tail_root >= tail_left and rounded_tail_root not in breakevens:
                breakevens.append(rounded_tail_root)

        max_profit_infinite = tail_slope > 1e-9
        max_loss_infinite = tail_slope < -1e-9
        max_profit_value = max(candidate_payoffs)
        min_pnl_value = min(candidate_payoffs)

        return {
            "net_debit_credit": round(net_debit_credit, 6),
            "max_profit": _format_bound(max_profit_value, max_profit_infinite),
            "max_loss": _format_bound(abs(min_pnl_value), max_loss_infinite),
            "breakevens": sorted(breakevens),
            "greeks_aggregate": {key: round(value, 6) for key, value in aggregate.items()},
        }
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def pmcc_evaluator(
    spot: float,
    long_strike: float,
    long_expiry_days: float,
    short_strike: float,
    short_expiry_days: float,
    volatility: float,
    risk_free_rate: float = 0.05,
) -> dict:
    tool_name = "pmcc_evaluator"
    _log_mcp(tool_name, f"request spot={spot} long_strike={long_strike} short_strike={short_strike}")
    try:
        spot = _require_positive("spot", spot)
        long_strike = _require_positive("long_strike", long_strike)
        short_strike = _require_positive("short_strike", short_strike)
        long_expiry_days = _require_positive("long_expiry_days", long_expiry_days)
        short_expiry_days = _require_positive("short_expiry_days", short_expiry_days)
        volatility = _require_positive("volatility", volatility)
        risk_free_rate = _normalize_number(risk_free_rate) or 0.0
        if long_expiry_days <= short_expiry_days:
            return {"error": "long_expiry_days must be greater than short_expiry_days"}

        long_metrics = _bs_metrics_raw(spot, long_strike, long_expiry_days / 365.0, volatility, risk_free_rate, 0.0, "C")
        short_metrics = _bs_metrics_raw(spot, short_strike, short_expiry_days / 365.0, volatility, risk_free_rate, 0.0, "C")
        net_debit = long_metrics["price"] - short_metrics["price"]
        stock_equivalent_basis = long_strike + net_debit
        max_gain_if_assigned = short_strike - stock_equivalent_basis
        roi_pct = (max_gain_if_assigned / net_debit * 100.0) if net_debit > 0 else None

        return {
            "long_call_price": round(long_metrics["price"], 6),
            "short_call_credit": round(short_metrics["price"], 6),
            "net_debit": round(net_debit, 6),
            "cost_basis": round(stock_equivalent_basis, 6),
            "breakeven": round(stock_equivalent_basis, 6),
            "max_gain_if_assigned": round(max_gain_if_assigned, 6),
            "roi_pct": round(roi_pct, 6) if roi_pct is not None else None,
            "long_delta": round(long_metrics["delta"], 6),
            "short_delta": round(short_metrics["delta"], 6),
        }
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def roll_analyzer(
    current_strike: float,
    current_expiry_days: float,
    current_right: Literal["C", "P"],
    new_strike: float,
    new_expiry_days: float,
    spot: float,
    volatility: float,
    risk_free_rate: float = 0.05,
) -> dict:
    tool_name = "roll_analyzer"
    _log_mcp(tool_name, f"request current_strike={current_strike} new_strike={new_strike}")
    try:
        spot = _require_positive("spot", spot)
        current_strike = _require_positive("current_strike", current_strike)
        new_strike = _require_positive("new_strike", new_strike)
        current_expiry_days = _require_positive("current_expiry_days", current_expiry_days)
        new_expiry_days = _require_positive("new_expiry_days", new_expiry_days)
        volatility = _require_positive("volatility", volatility)
        risk_free_rate = _normalize_number(risk_free_rate) or 0.0
        right = _normalize_option_right(current_right)

        current_metrics = _bs_metrics_raw(spot, current_strike, current_expiry_days / 365.0, volatility, risk_free_rate, 0.0, right)
        new_metrics = _bs_metrics_raw(spot, new_strike, new_expiry_days / 365.0, volatility, risk_free_rate, 0.0, right)

        roll_cost = new_metrics["price"] - current_metrics["price"]
        delta_change = new_metrics["delta"] - current_metrics["delta"]
        theta_change = new_metrics["theta"] - current_metrics["theta"]
        vega_change = new_metrics["vega"] - current_metrics["vega"]
        new_breakeven = new_strike + new_metrics["price"] if right == "C" else new_strike - new_metrics["price"]

        cost_threshold = max(current_metrics["price"] * 0.25, 0.05 * spot)
        directional_improvement = new_metrics["delta"] >= current_metrics["delta"] if right == "C" else abs(new_metrics["delta"]) >= abs(current_metrics["delta"])
        theta_improvement = new_metrics["theta"] >= current_metrics["theta"]
        cost_ok = roll_cost <= cost_threshold
        recommended = cost_ok and (directional_improvement or theta_improvement)

        reasons = []
        if roll_cost <= 0:
            reasons.append("roll is a net credit or flat")
        elif cost_ok:
            reasons.append("roll cost is modest")
        else:
            reasons.append("roll cost is large")
        if directional_improvement:
            reasons.append("directional exposure improves or is preserved")
        if theta_improvement:
            reasons.append("theta profile improves")
        if not directional_improvement and not theta_improvement:
            reasons.append("greek profile does not improve")

        return {
            "roll_cost": round(roll_cost, 6),
            "delta_change": round(delta_change, 6),
            "theta_change": round(theta_change, 6),
            "vega_change": round(vega_change, 6),
            "new_breakeven": round(new_breakeven, 6),
            "recommended": recommended,
            "reason": "; ".join(reasons),
        }
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def get_fundamentals(
    symbol: str,
    report_type: str = "ReportsFinSummary",
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict[str, Any]:
    tool_name = "get_fundamentals"
    _log_mcp(tool_name, f"request symbol={symbol} report_type={report_type} exchange={exchange}")

    try:
        if _is_probable_crypto_request(symbol, exchange):
            return {"error": "use CoinGecko for crypto fundamentals"}

        yf_symbol, is_european_equity = _resolve_yfinance_symbol(symbol, exchange, currency)
        snapshot = await asyncio.to_thread(_fetch_yfinance_snapshot_sync, yf_symbol)
        log = list(snapshot.get("log", []))
        if report_type not in _SUPPORTED_FUNDAMENTALS_REPORT_TYPES:
            log.append(f"unsupported report_type '{report_type}' for yfinance-backed fundamentals; returning normalized snapshot")
        elif report_type != "ReportsFinSummary":
            log.append(f"report_type '{report_type}' is advisory only under yfinance-backed fundamentals")

        info = snapshot.get("info", {}) or {}
        earnings_data = _extract_yf_earnings_data(snapshot, log)
        dividend_data = _extract_yf_dividend_data(snapshot, log)

        if not info and snapshot.get("financials_latest") is None and snapshot.get("balance_sheet_latest") is None and snapshot.get("cashflow_latest") is None:
            return {"error": f"no yfinance data for symbol {yf_symbol}"}

        trailing_pe = _normalize_number(info.get("trailingPE"))
        trailing_eps = _normalize_number(info.get("trailingEps"))
        total_revenue = _normalize_number(info.get("totalRevenue"))
        market_cap = _normalize_number(info.get("marketCap"))
        sector = info.get("sector")

        if sector in {None, ""}:
            log.append("missing sector from yfinance info")
        if market_cap is None:
            log.append("missing market cap from yfinance info")
        if trailing_pe is None:
            log.append("missing trailing PE from yfinance info")
        if trailing_eps is None:
            log.append("missing trailing EPS from yfinance info")
        if total_revenue is None:
            log.append("missing total revenue from yfinance info")

        data = {
            "report_type_requested": report_type,
            "yfinance_symbol": yf_symbol,
            "company_name": info.get("longName") or info.get("shortName"),
            "sector": sector,
            "industry": info.get("industry"),
            "market_cap": market_cap,
            "trailing_pe": _round_number(trailing_pe),
            "forward_pe": _round_number(_normalize_number(info.get("forwardPE"))),
            "eps_ttm": _round_number(trailing_eps),
            "eps_forward": _round_number(_normalize_number(info.get("forwardEps"))),
            "revenue_ttm": total_revenue,
            "financials_latest": snapshot.get("financials_latest"),
            "balance_sheet_latest": snapshot.get("balance_sheet_latest"),
            "cashflow_latest": snapshot.get("cashflow_latest"),
            "earnings": earnings_data,
            "dividends": dividend_data,
        }
        return _build_yfinance_response(symbol, data, log, _fundamentals_source_note(is_european_equity))
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_earnings_date(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict[str, Any]:
    tool_name = "get_earnings_date"
    _log_mcp(tool_name, f"request symbol={symbol} exchange={exchange}")

    try:
        if _is_probable_crypto_request(symbol, exchange):
            return {"error": "use CoinGecko for crypto fundamentals"}

        yf_symbol, is_european_equity = _resolve_yfinance_symbol(symbol, exchange, currency)
        snapshot = await asyncio.to_thread(_fetch_yfinance_snapshot_sync, yf_symbol)
        log = list(snapshot.get("log", []))
        earnings_data = _extract_yf_earnings_data(snapshot, log)
        earnings_data["yfinance_symbol"] = yf_symbol

        if earnings_data.get("next_earnings_date") is None and not snapshot.get("info"):
            return {"error": f"no yfinance data for symbol {yf_symbol}"}

        return _build_yfinance_response(symbol, earnings_data, log, _fundamentals_source_note(is_european_equity))
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_dividend_calendar(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict[str, Any]:
    tool_name = "get_dividend_calendar"
    _log_mcp(tool_name, f"request symbol={symbol} exchange={exchange}")

    try:
        if _is_probable_crypto_request(symbol, exchange):
            return {"error": "use CoinGecko for crypto fundamentals"}

        yf_symbol, is_european_equity = _resolve_yfinance_symbol(symbol, exchange, currency)
        snapshot = await asyncio.to_thread(_fetch_yfinance_snapshot_sync, yf_symbol)
        log = list(snapshot.get("log", []))
        dividend_data = _extract_yf_dividend_data(snapshot, log)
        dividend_data["yfinance_symbol"] = yf_symbol

        if all(dividend_data.get(field) is None for field in ("annual_dividend", "yield_pct", "ex_date_next", "pay_date_next", "payout_ratio")):
            if not snapshot.get("info"):
                return {"error": f"no yfinance data for symbol {yf_symbol}"}

        return _build_yfinance_response(symbol, dividend_data, log, _fundamentals_source_note(is_european_equity))
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def place_combo_order(
    legs: list[dict[str, Any]],
    total_quantity: float,
    order_type: Literal["LMT", "MKT"] = "LMT",
    limit_price: Optional[float] = None,
    exchange: str = "SMART",
    currency: str = "USD",
    action: Literal["BUY", "SELL"] = "BUY",
    tif: Literal["DAY", "GTC", "IOC"] = "DAY",
) -> dict[str, Any]:
    tool_name = "place_combo_order"
    _log_mcp(tool_name, f"request legs={len(legs) if isinstance(legs, list) else 'invalid'} qty={total_quantity}")

    try:
        normalized_exchange = _normalize_exchange(exchange)
        normalized_action = _normalize_side(action)
        total_quantity = _require_positive("total_quantity", total_quantity)
        normalized_order_type = str(order_type or "").upper()
        if normalized_order_type not in {"LMT", "MKT"}:
            return {"error": "order_type must be LMT or MKT"}
        if normalized_order_type == "LMT" and (limit_price is None or _normalize_number(limit_price) is None or float(limit_price) <= 0):
            return {"error": "LMT requires limit_price > 0"}
        if not isinstance(legs, list) or not legs:
            return {"error": "legs must be a non-empty list"}

        combo_legs = []
        for index, leg in enumerate(legs, start=1):
            con_id = int(leg.get("conId") or 0)
            ratio = int(leg.get("ratio") or 0)
            leg_action = _normalize_side(leg.get("action"))
            leg_exchange = _normalize_exchange(leg.get("exchange") or normalized_exchange)
            if con_id <= 0:
                return {"error": f"leg {index} conId must be > 0"}
            if ratio <= 0:
                return {"error": f"leg {index} ratio must be > 0"}
            combo_leg = ComboLeg()
            combo_leg.conId = con_id
            combo_leg.ratio = ratio
            combo_leg.action = leg_action
            combo_leg.exchange = leg_exchange
            combo_legs.append(combo_leg)

        bag = Bag()
        bag.exchange = normalized_exchange
        bag.currency = currency
        bag.comboLegs = combo_legs

        if normalized_order_type == "LMT":
            order = LimitOrder(normalized_action, total_quantity, float(limit_price))
        else:
            order = MarketOrder(normalized_action, total_quantity)
        order.tif = tif

        ib = await get_ib()
        trade = ib.placeOrder(bag, order)
        await _wait_for(
            lambda: bool(getattr(trade.orderStatus, "status", None)),
            timeout_seconds=3.0,
            interval_seconds=0.25,
        )
        return _serialize_trade_status(trade)
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_exchange_hours(exchange: str) -> dict[str, Any]:
    tool_name = "get_exchange_hours"
    _log_mcp(tool_name, f"request exchange={exchange}")
    try:
        return _build_exchange_hours_view(exchange)
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
async def precheck_order(
    symbol: str,
    side: Literal["BUY", "SELL"],
    quantity: float,
    limit_price: float,
    exchange: str = "SMART",
    currency: str = "USD",
    sec_type: Literal["STK", "OPT"] = "STK",
) -> dict[str, Any]:
    tool_name = "precheck_order"
    _log_mcp(tool_name, f"request symbol={symbol} side={side} quantity={quantity} exchange={exchange}")

    warnings: list[str] = []
    blockers: list[str] = []
    checks_run: list[str] = []

    try:
        normalized_side = _normalize_side(side)
        normalized_exchange = _normalize_exchange(exchange)
        normalized_sec_type = str(sec_type or "").upper()
        quantity = _require_positive("quantity", quantity)
        limit_price = _require_positive("limit_price", limit_price)
        notional = quantity * limit_price

        if normalized_sec_type != "STK":
            blockers.append("precheck_order currently supports STK only")
            return {
                "ok": False,
                "warnings": warnings,
                "blockers": blockers,
                "checks_run": checks_run,
                "notional": round(notional, 6),
            }

        hours = _build_exchange_hours_view(normalized_exchange)
        checks_run.append("exchange_hours")
        if not hours["is_open_now"]:
            blockers.append(f"exchange {normalized_exchange} is currently closed")

        checks_run.append("notional_limit")
        if notional > RISK_MAX_ORDER_NOTIONAL:
            blockers.append(
                f"order notional {round(notional, 6)} exceeds max {round(RISK_MAX_ORDER_NOTIONAL, 6)}"
            )

        ib = await get_ib()
        pnl_rows, resolved_account = await _collect_pnl_per_position(ib)
        summary_items = list(await ib.accountSummaryAsync())

        reference_price, reference_source, reference_error = await _get_reference_price_for_precheck(
            ib,
            symbol,
            normalized_exchange,
            currency,
            normalized_sec_type,
            account=resolved_account,
            pnl_rows=pnl_rows,
        )
        checks_run.append(f"reference_price:{reference_source or 'unavailable'}")
        if reference_price is None:
            blockers.append("unable to determine reference price for distance check")
            if reference_error:
                warnings.append(str(reference_error))
        else:
            price_distance = abs(limit_price - reference_price) / reference_price
            if price_distance > RISK_MAX_PRICE_DISTANCE_PCT:
                blockers.append(
                    f"limit price {round(limit_price, 6)} is {round(price_distance * 100, 3)}% away from reference price {round(reference_price, 6)}"
                )

        buying_power = _account_summary_number(summary_items, ("BuyingPower", "AvailableFunds"))
        checks_run.append("available_cash")
        if buying_power is None:
            warnings.append("buying power unavailable from account summary")
        elif notional > buying_power:
            blockers.append(
                f"order notional {round(notional, 6)} exceeds buying power {round(buying_power, 6)}"
            )

        net_liquidation = _account_summary_number(summary_items, ("NetLiquidation",))
        checks_run.append("concentration")
        if net_liquidation is None or net_liquidation <= 0:
            warnings.append("net liquidation unavailable for concentration check")
            concentration_after_pct = None
        else:
            current_symbol_value = 0.0
            for row in pnl_rows:
                if _position_matches(row, symbol, normalized_sec_type, currency, normalized_exchange):
                    current_symbol_value += _normalize_number(row.get("marketValue")) or 0.0
            signed_notional = notional if normalized_side == "BUY" else -notional
            concentration_after_pct = abs(current_symbol_value + signed_notional) / net_liquidation
            if concentration_after_pct > RISK_MAX_CONCENTRATION_PCT:
                warnings.append(
                    f"post-trade concentration would be {round(concentration_after_pct * 100, 3)}% of net liquidation"
                )

        return {
            "ok": len(blockers) == 0,
            "warnings": warnings,
            "blockers": blockers,
            "checks_run": checks_run,
            "reference_price": _round_number(reference_price),
            "reference_price_source": reference_source,
            "notional": round(notional, 6),
            "buying_power": _round_number(buying_power),
            "concentration_after_pct": _round_number(concentration_after_pct if net_liquidation else None),
            "recommendation": "can place" if not blockers else "do not place",
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_pnl_per_position(account: Optional[str] = None) -> list[dict[str, Any]] | dict[str, Any]:
    tool_name = "get_pnl_per_position"
    _log_mcp(tool_name, f"request account={account or 'default'}")
    try:
        ib = await get_ib()
        rows, resolved_account = await _collect_pnl_per_position(ib, account)
        _log_mcp(tool_name, f"resolved_account={resolved_account} positions={len(rows)}")
        return rows
    except Exception as exc:
        _log_mcp(tool_name, f"error account={account or 'default'}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def resolve_symbol(query: str) -> list[dict[str, Any]] | dict[str, Any]:
    tool_name = "resolve_symbol"
    try:
        normalized_query = _normalize_query_text(query, "query")
        _log_mcp(tool_name, f"request query={normalized_query}")
        ib = await get_ib()
        before_errors = len(_recent_errors)
        matches = list(await ib.reqMatchingSymbolsAsync(normalized_query))

        rows: list[dict[str, Any]] = []
        for match in matches:
            contract = getattr(match, "contract", None)
            symbol_value = getattr(contract, "symbol", None)
            con_id = getattr(contract, "conId", None)
            if not symbol_value or con_id in {None, 0}:
                continue
            rows.append(
                {
                    "symbol": symbol_value,
                    "secType": getattr(contract, "secType", None),
                    "primaryExchange": getattr(contract, "primaryExchange", None) or getattr(contract, "exchange", None),
                    "currency": getattr(contract, "currency", None),
                    "conId": con_id,
                    "longName": getattr(match, "longName", None),
                }
            )

        rows.sort(
            key=lambda row: (
                0 if str(row.get("symbol") or "").upper() == normalized_query.upper() else 1,
                str(row.get("primaryExchange") or ""),
                str(row.get("secType") or ""),
                str(row.get("longName") or ""),
            )
        )
        if rows:
            return rows

        recent_errors = _recent_errors_since(before_errors)
        if recent_errors:
            return {"error": "no matching symbols returned", "details": recent_errors[-1]}
        return []
    except Exception as exc:
        _log_mcp(tool_name, f"error query={query}: {exc}")
        recent_errors = list(_recent_errors)
        if recent_errors:
            return {"error": str(exc), "details": recent_errors[-1]}
        return {"error": str(exc)}


@mcp.tool()
async def classify_positions_by_strategy() -> dict[str, Any]:
    tool_name = "classify_positions_by_strategy"
    _log_mcp(tool_name, "request")

    strategy_names = [
        "long_stocks",
        "short_stocks",
        "covered_calls",
        "cash_secured_puts",
        "long_calls",
        "long_puts",
        "spreads",
        "other",
    ]

    try:
        ib = await get_ib()
        await ib.reqPositionsAsync()
        raw_positions = list(ib.positions())
        resolved_account = _resolve_account(ib, None, raw_positions)
        scoped_positions = [
            position
            for position in raw_positions
            if resolved_account is None or getattr(position, "account", None) == resolved_account
        ]

        pnl_rows, _ = await _collect_pnl_per_position(ib, resolved_account)
        pnl_by_con_id = {
            row.get("conId"): row
            for row in pnl_rows
            if row.get("conId") is not None
        }
        summary_items = list(await ib.accountSummaryAsync())
        cash_balances = _extract_cash_balances(summary_items)
        available_cash_by_currency = dict(cash_balances)

        positions_by_symbol: dict[str, list[dict[str, Any]]] = {}
        all_rows: list[dict[str, Any]] = []
        for position in scoped_positions:
            contract = getattr(position, "contract", None)
            row = _serialize_strategy_position(position, pnl_by_con_id.get(getattr(contract, "conId", None)))
            all_rows.append(row)
            positions_by_symbol.setdefault(str(row.get("symbol") or ""), []).append(row)

        strategies = {
            name: {"name": name, "positions": [], "notional": 0.0}
            for name in strategy_names
        }
        assigned_con_ids: set[Any] = set()

        for symbol_rows in positions_by_symbol.values():
            stock_rows = [
                row for row in symbol_rows
                if str(row.get("secType") or "").upper() == "STK"
            ]
            option_rows = [
                row for row in symbol_rows
                if str(row.get("secType") or "").upper() == "OPT"
            ]
            long_stock_rows = [row for row in stock_rows if (_normalize_number(row.get("position")) or 0.0) > 0]
            short_call_rows = [
                row for row in option_rows
                if (_normalize_number(row.get("position")) or 0.0) < 0 and str(row.get("right") or "").upper() == "C"
            ]

            if long_stock_rows and short_call_rows:
                for row in long_stock_rows + short_call_rows:
                    con_id = row.get("conId")
                    if con_id in assigned_con_ids:
                        continue
                    _append_strategy_position(strategies, "covered_calls", row)
                    assigned_con_ids.add(con_id)

        for symbol_rows in positions_by_symbol.values():
            remaining_option_rows = [
                row
                for row in symbol_rows
                if str(row.get("secType") or "").upper() == "OPT" and row.get("conId") not in assigned_con_ids
            ]
            if len(remaining_option_rows) < 2:
                continue

            has_long = any((_normalize_number(row.get("position")) or 0.0) > 0 for row in remaining_option_rows)
            has_short = any((_normalize_number(row.get("position")) or 0.0) < 0 for row in remaining_option_rows)
            if not (has_long and has_short):
                continue

            for row in remaining_option_rows:
                con_id = row.get("conId")
                if con_id in assigned_con_ids:
                    continue
                _append_strategy_position(strategies, "spreads", row)
                assigned_con_ids.add(con_id)

        short_put_rows = sorted(
            [
                row
                for row in all_rows
                if str(row.get("secType") or "").upper() == "OPT"
                and str(row.get("right") or "").upper() == "P"
                and (_normalize_number(row.get("position")) or 0.0) < 0
                and row.get("conId") not in assigned_con_ids
            ],
            key=lambda row: str(row.get("currency") or ""),
        )
        for row in short_put_rows:
            currency = str(row.get("currency") or "")
            strike = _normalize_number(row.get("strike"))
            multiplier = _normalize_number(row.get("multiplier")) or 100.0
            contracts = abs(_normalize_number(row.get("position")) or 0.0)
            secured_cash_required = None if strike is None else strike * multiplier * contracts
            available_cash = _normalize_number(available_cash_by_currency.get(currency))
            if secured_cash_required is None or available_cash is None or available_cash < secured_cash_required:
                continue

            available_cash_by_currency[currency] = round(available_cash - secured_cash_required, 6)
            _append_strategy_position(strategies, "cash_secured_puts", row)
            assigned_con_ids.add(row.get("conId"))

        for row in all_rows:
            con_id = row.get("conId")
            if con_id in assigned_con_ids:
                continue

            sec_type = str(row.get("secType") or "").upper()
            position_size = _normalize_number(row.get("position")) or 0.0
            right = str(row.get("right") or "").upper()

            if sec_type == "STK" and position_size > 0:
                _append_strategy_position(strategies, "long_stocks", row)
            elif sec_type == "STK" and position_size < 0:
                _append_strategy_position(strategies, "short_stocks", row)
            elif sec_type == "OPT" and position_size > 0 and right == "C":
                _append_strategy_position(strategies, "long_calls", row)
            elif sec_type == "OPT" and position_size > 0 and right == "P":
                _append_strategy_position(strategies, "long_puts", row)
            else:
                _append_strategy_position(strategies, "other", row)
            assigned_con_ids.add(con_id)

        return {
            "account": resolved_account,
            "cash_balances": cash_balances,
            "strategies": _sorted_strategy_rows(strategies),
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def screen_universe(criteria: dict[str, Any]) -> list[dict[str, Any]] | dict[str, Any]:
    """Screen equities via IB scanner codes. Supported scan_code values: HOT_BY_VOLUME, MOST_ACTIVE, TOP_PERC_GAIN, TOP_PERC_LOSE."""
    tool_name = "screen_universe"
    _log_mcp(tool_name, f"request criteria={criteria}")

    try:
        if not isinstance(criteria, dict):
            return {"error": "criteria must be a dict"}

        scan_code = str(criteria.get("scan_code") or "").upper()
        if scan_code not in _SUPPORTED_SCANNER_CODES:
            supported = ", ".join(sorted(_SUPPORTED_SCANNER_CODES))
            return {"error": f"unsupported scan_code: {scan_code or '<empty>'}; supported: {supported}"}

        exchange = criteria.get("exchange", "SMART")
        scanner_profile = _scanner_profile_for_exchange(exchange)
        limit = int(criteria.get("limit", 50) or 50)
        limit = max(1, min(limit, 50))

        subscription = ScannerSubscription(
            numberOfRows=limit,
            instrument=str(criteria.get("instrument") or scanner_profile["instrument"]),
            locationCode=str(criteria.get("location_code") or scanner_profile["locationCode"]),
            scanCode=scan_code,
        )

        above_price = _normalize_number(criteria.get("above_price"))
        below_price = _normalize_number(criteria.get("below_price"))
        above_volume = criteria.get("above_volume")
        market_cap_above = _normalize_number(criteria.get("market_cap_above"))
        market_cap_below = _normalize_number(criteria.get("market_cap_below"))
        stock_type_filter = criteria.get("stock_type_filter")

        if above_price is not None:
            subscription.abovePrice = above_price
        if below_price is not None:
            subscription.belowPrice = below_price
        if above_volume not in {None, ""}:
            subscription.aboveVolume = int(above_volume)
        if market_cap_above is not None:
            subscription.marketCapAbove = market_cap_above
        if market_cap_below is not None:
            subscription.marketCapBelow = market_cap_below
        if stock_type_filter not in {None, ""}:
            subscription.stockTypeFilter = str(stock_type_filter)

        ib = await get_ib()
        before_errors = len(_recent_errors)
        scan_rows = list(await ib.reqScannerDataAsync(subscription))

        rows: list[dict[str, Any]] = []
        for scan_row in scan_rows[:limit]:
            contract_details = getattr(scan_row, "contractDetails", None)
            contract = getattr(contract_details, "contract", None)
            if contract is None or not _scanner_result_matches_exchange(scan_row, exchange):
                continue

            row_exchange = getattr(contract, "primaryExchange", None) or getattr(contract, "exchange", None)
            if not row_exchange or str(row_exchange).upper() == "SMART":
                row_exchange = _normalize_exchange(exchange)
            rows.append(
                {
                    "symbol": getattr(contract, "symbol", None),
                    "exchange": row_exchange,
                    "rank": getattr(scan_row, "rank", None),
                    "scan_value": _scanner_scan_value(scan_row),
                }
            )

        if rows:
            return rows

        recent_errors = _recent_errors_since(before_errors)
        if recent_errors:
            return {"error": "scanner returned no rows", "details": recent_errors[-1]}
        return []
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        recent_errors = list(_recent_errors)
        if recent_errors:
            return {"error": str(exc), "details": recent_errors[-1]}
        return {"error": str(exc)}


@mcp.tool()
async def get_portfolio_snapshot() -> dict[str, Any]:
    tool_name = "get_portfolio_snapshot"
    _log_mcp(tool_name, "request")
    try:
        ib = await get_ib()
        summary_items = list(await ib.accountSummaryAsync())
        positions, resolved_account = await _collect_pnl_per_position(ib)

        exposure_by_currency: dict[str, float] = {}
        exposure_by_sector: dict[str, float] = {}
        exposure_by_asset_class: dict[str, float] = {}
        total_unrealized_pnl = 0.0
        total_realized_pnl_today = 0.0

        for position in positions:
            market_value = _normalize_number(position.get("marketValue"))
            if market_value is not None:
                currency = str(position.get("currency") or "")
                sec_type = str(position.get("secType") or "UNKNOWN")
                if currency:
                    exposure_by_currency[currency] = exposure_by_currency.get(currency, 0.0) + market_value
                exposure_by_asset_class[sec_type] = exposure_by_asset_class.get(sec_type, 0.0) + market_value
                sector = _infer_sector(position.get("symbol"), sec_type)
                if sector:
                    exposure_by_sector[sector] = exposure_by_sector.get(sector, 0.0) + market_value

            unrealized_pnl = _normalize_number(position.get("unrealizedPnL"))
            realized_pnl = _normalize_number(position.get("realizedPnL"))
            if unrealized_pnl is not None:
                total_unrealized_pnl += unrealized_pnl
            if realized_pnl is not None:
                total_realized_pnl_today += realized_pnl

        return {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "account": resolved_account,
            "cash": _extract_cash_balances(summary_items),
            "net_liquidation": _round_number(_account_summary_number(summary_items, ("NetLiquidation",))),
            "buying_power": _round_number(_account_summary_number(summary_items, ("BuyingPower", "AvailableFunds"))),
            "margin_used": _round_number(
                _account_summary_number(summary_items, ("MaintMarginReq", "FullMaintMarginReq", "InitMarginReq"))
            ),
            "positions": positions,
            "exposure_by_currency": {key: round(value, 6) for key, value in exposure_by_currency.items()},
            "exposure_by_sector": {key: round(value, 6) for key, value in exposure_by_sector.items()},
            "exposure_by_asset_class": {key: round(value, 6) for key, value in exposure_by_asset_class.items()},
            "total_unrealized_pnl": round(total_unrealized_pnl, 6),
            "total_realized_pnl_today": round(total_realized_pnl_today, 6),
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_daily_pnl_history(account: Optional[str] = None, days_back: int = 30) -> dict[str, Any]:
    tool_name = "get_daily_pnl_history"
    _log_mcp(tool_name, f"request account={account or 'default'} days_back={days_back}")
    try:
        if days_back <= 0:
            return {"error": "days_back must be > 0"}
        ib = await get_ib()
        resolved_account = _resolve_account(ib, account)
        return {
            "error": "not available via ib_async; use IB Client Portal",
            "account": resolved_account,
            "days_back": days_back,
            "history": [],
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error account={account or 'default'}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def modify_order(
    order_id: int,
    new_limit_price: Optional[float] = None,
    new_quantity: Optional[float] = None,
    new_stop_price: Optional[float] = None,
) -> dict:
    tool_name = "modify_order"
    _log_mcp(tool_name, f"request order_id={order_id}")
    if order_id <= 0:
        return {"error": "order_id must be > 0"}
    if new_limit_price is None and new_quantity is None and new_stop_price is None:
        return {"error": "at least one modification field is required"}
    if new_quantity is not None and new_quantity <= 0:
        return {"error": "new_quantity must be > 0"}
    if new_limit_price is not None and new_limit_price <= 0:
        return {"error": "new_limit_price must be > 0"}
    if new_stop_price is not None and new_stop_price <= 0:
        return {"error": "new_stop_price must be > 0"}

    try:
        ib = await get_ib()
        await ib.reqOpenOrdersAsync()
        open_trades = list(ib.openTrades())
        trade = next((item for item in open_trades if item.order.orderId == order_id), None)
        if trade is None:
            return {"error": f"orderId {order_id} not found", "checked_open": len(open_trades)}

        modified_order = copy.deepcopy(trade.order)
        modified_order.orderId = trade.order.orderId
        modified_fields: dict[str, Any] = {}
        order_type = str(getattr(modified_order, "orderType", "")).upper()

        if new_limit_price is not None:
            if order_type not in {"LMT", "STP LMT"}:
                return {"error": f"orderId {order_id} is {order_type}; new_limit_price not applicable"}
            modified_order.lmtPrice = new_limit_price
            modified_fields["lmtPrice"] = new_limit_price
        if new_quantity is not None:
            modified_order.totalQuantity = new_quantity
            modified_fields["totalQuantity"] = new_quantity
        if new_stop_price is not None:
            if "STP" not in order_type:
                return {"error": f"orderId {order_id} is {order_type}; new_stop_price not applicable"}
            modified_order.auxPrice = new_stop_price
            modified_fields["auxPrice"] = new_stop_price

        previous_status = getattr(trade.orderStatus, "status", None)
        previous_log_len = len(getattr(trade, "log", []) or [])
        resubmitted_trade = ib.placeOrder(trade.contract, modified_order)
        await _wait_for(
            lambda: (
                len(getattr(resubmitted_trade, "log", []) or []) > previous_log_len
                or getattr(resubmitted_trade.orderStatus, "status", None) != previous_status
            ),
            timeout_seconds=3,
        )

        _log_mcp(tool_name, f"modified order_id={order_id} fields={modified_fields}")
        return {
            "orderId": resubmitted_trade.order.orderId,
            "status": resubmitted_trade.orderStatus.status,
            "modified_fields": modified_fields,
            "filled": resubmitted_trade.orderStatus.filled,
            "remaining": resubmitted_trade.orderStatus.remaining,
            "avgFillPrice": resubmitted_trade.orderStatus.avgFillPrice,
            "whyHeld": resubmitted_trade.orderStatus.whyHeld,
            "log": _serialize_trade_log(resubmitted_trade),
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error order_id={order_id}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_order_status(order_id: int) -> dict:
    tool_name = "get_order_status"
    _log_mcp(tool_name, f"request order_id={order_id}")
    if order_id <= 0:
        return {"error": "order_id must be > 0"}

    try:
        ib = await get_ib()
        await ib.reqOpenOrdersAsync()
        open_trades = list(ib.openTrades())
        all_trades = list(ib.trades())
        trade = next((item for item in open_trades if item.order.orderId == order_id), None)
        if trade is None:
            trade = next((item for item in all_trades if item.order.orderId == order_id), None)
        if trade is None:
            return {
                "error": f"orderId {order_id} not found",
                "checked_open": len(open_trades),
                "checked_all": len(all_trades),
            }

        return _serialize_trade_status(trade)
    except Exception as exc:
        _log_mcp(tool_name, f"error order_id={order_id}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def get_executions(symbol: Optional[str] = None, days_back: int = 1) -> list:
    tool_name = "get_executions"
    _log_mcp(tool_name, f"request symbol={symbol or '*'} days_back={days_back}")
    if days_back < 0:
        return {"error": "days_back must be >= 0"}

    try:
        ib = await get_ib()
        execution_filter = ExecutionFilter()
        if days_back > 0:
            cutoff = datetime.now().astimezone() - timedelta(days=days_back)
            execution_filter.time = cutoff.strftime("%Y%m%d %H:%M:%S")

        fills = await ib.reqExecutionsAsync(execution_filter)
        normalized_symbol = symbol.upper() if symbol else None
        executions = []
        for fill in fills:
            contract = getattr(fill, "contract", None)
            execution = getattr(fill, "execution", None)
            commission_report = getattr(fill, "commissionReport", None)
            if execution is None or contract is None:
                continue
            if normalized_symbol and getattr(contract, "symbol", "").upper() != normalized_symbol:
                continue
            executions.append(
                {
                    "execId": getattr(execution, "execId", None),
                    "time": str(getattr(execution, "time", "")),
                    "symbol": getattr(contract, "symbol", None),
                    "side": getattr(execution, "side", None),
                    "shares": getattr(execution, "shares", None),
                    "price": getattr(execution, "price", None),
                    "orderId": getattr(execution, "orderId", None),
                    "permId": getattr(execution, "permId", None),
                    "commission": getattr(commission_report, "commission", None),
                }
            )

        return executions
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def cancel_all_open_orders(
    symbol: Optional[str] = None,
    side: Optional[Literal["BUY", "SELL"]] = None,
) -> dict:
    tool_name = "cancel_all_open_orders"
    _log_mcp(tool_name, f"request symbol={symbol or '*'} side={side or '*'}")

    try:
        ib = await get_ib()
        await ib.reqOpenOrdersAsync()
        open_trades = list(ib.openTrades())
        normalized_symbol = symbol.upper() if symbol else None
        normalized_side = side.upper() if side else None
        matching_trades = []

        for trade in open_trades:
            if normalized_symbol and getattr(trade.contract, "symbol", "").upper() != normalized_symbol:
                continue
            if normalized_side and getattr(trade.order, "action", "").upper() != normalized_side:
                continue
            matching_trades.append(trade)

        order_ids = []
        errors = []
        for trade in matching_trades:
            try:
                ib.cancelOrder(trade.order)
                order_ids.append(trade.order.orderId)
            except Exception as exc:
                errors.append(f"orderId {trade.order.orderId}: {exc}")

        if order_ids:
            await _wait_for(
                lambda: not any(trade.order.orderId in order_ids for trade in ib.openTrades()),
                timeout_seconds=2,
            )

        await ib.reqOpenOrdersAsync()
        remaining_ids = {
            trade.order.orderId
            for trade in ib.openTrades()
            if trade.order.orderId in order_ids
        }
        for remaining_id in sorted(remaining_ids):
            errors.append(f"orderId {remaining_id} still open after cancel request")

        cancelled_count = len(order_ids) - len(remaining_ids)
        _log_mcp(tool_name, f"cancelled={cancelled_count} requested={len(order_ids)}")
        return {
            "cancelled": cancelled_count,
            "orderIds": order_ids,
            "errors": errors,
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def place_bracket_order(
    symbol: str,
    side: Literal["BUY", "SELL"],
    quantity: float,
    entry_price: float,
    take_profit_price: float,
    stop_loss_price: float,
    exchange: str = "SMART",
    currency: str = "USD",
    sec_type: Literal["STK", "OPT"] = "STK",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[Literal["C", "P"]] = None,
) -> dict:
    tool_name = "place_bracket_order"
    _log_mcp(tool_name, f"request symbol={symbol} side={side} qty={quantity}")
    if quantity <= 0:
        return {"error": "quantity must be > 0"}
    if entry_price <= 0 or take_profit_price <= 0 or stop_loss_price <= 0:
        return {"error": "entry_price, take_profit_price and stop_loss_price must be > 0"}

    try:
        ib = await get_ib()
        contract, error = await _qualify_contract(
            ib,
            symbol,
            exchange,
            currency,
            sec_type,
            expiry,
            strike,
            right,
        )
        if error:
            return error

        bracket = ib.bracketOrder(
            action=side,
            quantity=quantity,
            limitPrice=entry_price,
            takeProfitPrice=take_profit_price,
            stopLossPrice=stop_loss_price,
        )

        placed_trades = []
        for order in bracket:
            placed_trades.append(ib.placeOrder(contract, order))

        await _wait_for(
            lambda: all(getattr(trade.order, "orderId", 0) for trade in placed_trades),
            timeout_seconds=3,
        )
        parent_trade = placed_trades[0]
        await _wait_for(
            lambda: (
                getattr(parent_trade.orderStatus, "status", None) not in {"PendingSubmit", "ApiPending"}
                or len(getattr(parent_trade, "log", []) or []) > 0
            ),
            timeout_seconds=3,
        )

        statuses = [_serialize_trade_status(trade) for trade in placed_trades]
        return {
            "symbol": symbol,
            "parentOrderId": placed_trades[0].order.orderId if len(placed_trades) > 0 else None,
            "takeProfitOrderId": placed_trades[1].order.orderId if len(placed_trades) > 1 else None,
            "stopLossOrderId": placed_trades[2].order.orderId if len(placed_trades) > 2 else None,
            "orderIds": [trade.order.orderId for trade in placed_trades],
            "statuses": statuses,
            "log": _serialize_trade_log(parent_trade),
        }
    except Exception as exc:
        _log_mcp(tool_name, f"error symbol={symbol}: {exc}")
        return {"error": str(exc)}


@mcp.tool()
async def healthcheck() -> dict:
    tool_name = "healthcheck"
    _log_mcp(tool_name, "request")
    result = {
        "connected": False,
        "ib_server_version": None,
        "client_id": IB_CLIENT_ID,
        "managed_accounts": [],
        "active_subscriptions": list(_active_subscriptions.values()),
        "open_orders_count": 0,
        "positions_count": 0,
        "data_farms": dict(_data_farms),
        "uptime_seconds": _get_uptime_seconds(),
        "last_error": _last_error(),
    }

    try:
        ib = await get_ib()
        result["connected"] = ib.isConnected()
        result["ib_server_version"] = _get_ib_server_version(ib)
        result["managed_accounts"] = _get_managed_accounts(ib)
        open_orders = await ib.reqOpenOrdersAsync()
        await ib.reqPositionsAsync()
        result["open_orders_count"] = len(open_orders) if open_orders is not None else len(ib.openTrades())
        result["positions_count"] = len(ib.positions())
        result["active_subscriptions"] = list(_active_subscriptions.values())
        result["data_farms"] = dict(_data_farms)
        result["last_error"] = _last_error()
        result["uptime_seconds"] = _get_uptime_seconds()
        return result
    except Exception as exc:
        _log_mcp(tool_name, f"error: {exc}")
        result["error"] = str(exc)
        return result


@mcp.tool()
async def get_recent_errors(limit: int = 20):
    tool_name = "get_recent_errors"
    _log_mcp(tool_name, f"request limit={limit}")
    if limit <= 0:
        return {"error": "limit must be > 0"}
    return list(_recent_errors)[-limit:]


@mcp.tool()
async def get_server_info() -> dict:
    tool_name = "get_server_info"
    _log_mcp(tool_name, "request")
    return {
        "mcp_server_version": MCP_SERVER_VERSION,
        "ib_async_version": _safe_package_version("ib_async"),
        "fastmcp_version": _safe_package_version("fastmcp"),
        "python_version": sys.version.split()[0],
        "running_since": SERVER_RUNNING_SINCE.isoformat(),
        "host": IB_HOST,
        "port": IB_PORT,
        "client_id": IB_CLIENT_ID,
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")
