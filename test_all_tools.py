from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable


ROOT = Path(__file__).resolve().parent
SERVER_PATH = ROOT / "server.py"
CUSTOM_TOOL_COUNT = 38
TOTAL_TOOL_COUNT = 42
MUTATING_TOOLS = [
    "place_order",
    "cancel_order",
    "modify_order",
    "cancel_all_open_orders",
    "place_bracket_order",
    "place_combo_order",
]

if "TEST_IB_CLIENT_ID" in os.environ:
    os.environ["IB_CLIENT_ID"] = os.environ["TEST_IB_CLIENT_ID"]
else:
    os.environ.setdefault("IB_CLIENT_ID", "99")

Validator = Callable[[Any], tuple[bool, str]]
AsyncCall = Callable[[Any], Awaitable[Any]]


@dataclass(frozen=True)
class Case:
    name: str
    description: str
    call: AsyncCall
    validator: Validator


def _load_server_module() -> Any:
    spec = importlib.util.spec_from_file_location("ibkr_mcp_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load server module from {SERVER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _preview(value: Any, limit: int = 320) -> str:
    try:
        text = json.dumps(value, default=str, ensure_ascii=True)
    except TypeError:
        text = repr(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _validate_dict_keys(result: Any, required: tuple[str, ...]) -> tuple[bool, str]:
    if not isinstance(result, dict):
        return False, f"expected dict, got {type(result).__name__}"
    missing = [key for key in required if key not in result]
    if missing:
        return False, f"missing keys: {', '.join(missing)}"
    return True, "dict shape ok"


def _validate_nonempty_dict(result: Any) -> tuple[bool, str]:
    if not isinstance(result, dict):
        return False, f"expected dict, got {type(result).__name__}"
    if not result:
        return False, "dict is empty"
    return True, f"dict keys={len(result)}"


def _validate_list(
    result: Any,
    required_keys: tuple[str, ...] = (),
    *,
    allow_empty: bool,
) -> tuple[bool, str]:
    if not isinstance(result, list):
        return False, f"expected list, got {type(result).__name__}"
    if not result:
        return (True, "empty list accepted") if allow_empty else (False, "list is empty")
    first = result[0]
    if required_keys:
        if not isinstance(first, dict):
            return False, f"expected list[dict], got list[{type(first).__name__}]"
        missing = [key for key in required_keys if key not in first]
        if missing:
            return False, f"first row missing keys: {', '.join(missing)}"
    return True, f"rows={len(result)}"


def _validate_error_or_dict_keys(
    result: Any,
    required: tuple[str, ...],
    *,
    expected_error_fragment: str | None = None,
) -> tuple[bool, str]:
    if isinstance(result, dict) and "error" in result:
        if expected_error_fragment:
            haystack = _preview(result, limit=600)
            if expected_error_fragment not in haystack:
                return False, f"error did not contain expected fragment: {expected_error_fragment}"
        return True, f"honest error: {result.get('error')}"
    return _validate_dict_keys(result, required)


def _validate_error_or_list(
    result: Any,
    required_keys: tuple[str, ...],
    *,
    allow_empty: bool,
    expected_error_fragment: str | None = None,
) -> tuple[bool, str]:
    if isinstance(result, dict) and "error" in result:
        if expected_error_fragment:
            haystack = _preview(result, limit=600)
            if expected_error_fragment not in haystack:
                return False, f"error did not contain expected fragment: {expected_error_fragment}"
        return True, f"honest error: {result.get('error')}"
    return _validate_list(result, required_keys, allow_empty=allow_empty)


def _validate_numeric_key(result: Any, key: str) -> tuple[bool, str]:
    ok, detail = _validate_dict_keys(result, (key,))
    if not ok:
        return ok, detail
    if not isinstance(result[key], (int, float)):
        return False, f"key {key} is not numeric"
    return True, f"{key}={result[key]}"


def _validate_yfinance_payload(result: Any) -> tuple[bool, str]:
    ok, detail = _validate_dict_keys(result, ("symbol", "source", "as_of", "data", "source_note", "log"))
    if not ok:
        return ok, detail
    if result.get("source") != "yfinance":
        return False, f"unexpected source: {result.get('source')}"
    if not isinstance(result.get("data"), dict):
        return False, "data is not a dict"
    if not isinstance(result.get("log"), list):
        return False, "log is not a list"
    return True, "yfinance payload ok"


def _validate_strategy_payload(result: Any) -> tuple[bool, str]:
    ok, detail = _validate_dict_keys(result, ("account", "cash_balances", "strategies"))
    if not ok:
        return ok, detail
    strategies = result.get("strategies")
    if not isinstance(strategies, list):
        return False, "strategies is not a list"
    expected_names = {
        "long_stocks",
        "short_stocks",
        "covered_calls",
        "cash_secured_puts",
        "long_calls",
        "long_puts",
        "spreads",
        "other",
    }
    found_names = {row.get("name") for row in strategies if isinstance(row, dict)}
    missing = sorted(expected_names - found_names)
    if missing:
        return False, f"missing strategy buckets: {', '.join(missing)}"
    return True, f"strategy buckets={len(strategies)}"


def _validate_order_status_not_found(result: Any) -> tuple[bool, str]:
    if not isinstance(result, dict):
        return False, f"expected dict, got {type(result).__name__}"
    error = str(result.get("error") or "")
    if "not found" not in error.lower():
        return False, f"unexpected order status response: {error or _preview(result)}"
    return True, error


def _validate_daily_pnl_history(result: Any) -> tuple[bool, str]:
    ok, detail = _validate_dict_keys(result, ("error", "history"))
    if not ok:
        return ok, detail
    if "not available via ib_async" not in str(result.get("error")):
        return False, f"unexpected fallback: {result.get('error')}"
    if not isinstance(result.get("history"), list):
        return False, "history is not a list"
    return True, "fallback payload ok"


def _build_cases(module: Any) -> list[Case]:
    return [
        Case("get_positions", "Current IB positions", lambda server: server.get_positions(), lambda result: _validate_list(result, ("symbol", "secType", "position"), allow_empty=True)),
        Case("get_live_orders", "Open orders for current client", lambda server: server.get_live_orders(all_clients=False), lambda result: _validate_list(result, ("orderId", "symbol", "status"), allow_empty=True)),
        Case("get_account_summary", "Account summary snapshot", lambda server: server.get_account_summary(), _validate_nonempty_dict),
        Case("get_market_data", "Quote or honest permission error", lambda server: server.get_market_data("ENI", exchange="BVME", currency="EUR"), lambda result: _validate_error_or_dict_keys(result, ("symbol", "bid", "ask", "last", "close"), expected_error_fragment="no market data permission")),
        Case("get_option_chain", "Option chain discovery", lambda server: server.get_option_chain("NVDA"), lambda result: _validate_list(result, ("exchange", "tradingClass", "expirations", "strikes"), allow_empty=False)),
        Case("get_historical_bars", "Historical hourly bars", lambda server: server.get_historical_bars("NVDA", duration="5 D", bar_size="1 hour"), lambda result: _validate_list(result, ("time", "open", "high", "low", "close", "volume"), allow_empty=False)),
        Case("get_52w_context", "52-week high/low context", lambda server: server.get_52w_context("NVDA"), lambda result: _validate_dict_keys(result, ("current", "high_52w", "low_52w", "pct_from_high", "pct_from_low", "as_of"))),
        Case("get_market_data_with_greeks", "Option greeks or honest permission error", lambda server: server.get_market_data_with_greeks("NVDA", "20261218", 250, "C"), lambda result: _validate_error_or_dict_keys(result, ("bid", "ask", "last", "iv", "delta", "gamma", "theta", "vega"), expected_error_fragment="no market data permission")),
        Case("bs_price", "Black-Scholes pricing", lambda server: server.bs_price(100, 100, 0.25, 0.20, 0.05, 0.0, "C"), lambda result: _validate_dict_keys(result, ("price", "d1", "d2", "delta", "gamma", "theta", "vega", "rho"))),
        Case("implied_volatility", "IV solver", lambda server: server.implied_volatility(4.61, 100, 100, 0.25, 0.05, 0.0, "C"), lambda result: _validate_numeric_key(result, "iv")),
        Case("greeks_from_price", "Greeks from market price", lambda server: server.greeks_from_price(4.61, 100, 100, 0.25, 0.05, 0.0, "C"), lambda result: _validate_dict_keys(result, ("price", "iv", "delta", "gamma", "theta", "vega", "rho"))),
        Case("prob_itm", "Probability ITM", lambda server: server.prob_itm(100, 100, 0.25, 0.20, 0.05, 0.0, "C"), lambda result: _validate_numeric_key(result, "prob_itm")),
        Case("expected_move", "One-sigma move", lambda server: server.expected_move(100, 0.20, 0.25), lambda result: _validate_dict_keys(result, ("expected_move", "expected_up", "expected_down", "expected_high", "expected_low"))),
        Case("evaluate_multi_leg", "Multi-leg payoff analysis", lambda server: server.evaluate_multi_leg([{"side": "BUY", "right": "C", "strike": 95, "quantity": 1}, {"side": "SELL", "right": "C", "strike": 105, "quantity": 1}], 100, 30 / 365, 0.20, 0.05), lambda result: _validate_dict_keys(result, ("net_debit_credit", "max_profit", "max_loss", "breakevens", "greeks_aggregate"))),
        Case("pmcc_evaluator", "PMCC evaluator", lambda server: server.pmcc_evaluator(100, 80, 180, 105, 30, 0.25, 0.05), lambda result: _validate_dict_keys(result, ("net_debit", "cost_basis", "breakeven", "max_gain_if_assigned", "roi_pct"))),
        Case("roll_analyzer", "Option roll analyzer", lambda server: server.roll_analyzer(100, 30, "C", 105, 60, 100, 0.20, 0.05), lambda result: _validate_dict_keys(result, ("roll_cost", "delta_change", "theta_change", "vega_change", "new_breakeven", "recommended", "reason"))),
        Case("get_fundamentals", "yfinance fundamentals snapshot", lambda server: server.get_fundamentals("NVDA"), _validate_yfinance_payload),
        Case("get_earnings_date", "yfinance earnings calendar", lambda server: server.get_earnings_date("NVDA"), _validate_yfinance_payload),
        Case("get_dividend_calendar", "yfinance dividend calendar", lambda server: server.get_dividend_calendar("ENI", exchange="BVME", currency="EUR"), _validate_yfinance_payload),
        Case("get_exchange_hours", "Exchange schedule", lambda server: server.get_exchange_hours("BVME"), lambda result: _validate_dict_keys(result, ("exchange", "timezone", "open_local", "close_local", "is_open_now", "next_open", "next_close", "trading_days"))),
        Case("precheck_order", "Risk-only order precheck", lambda server: server.precheck_order("ENI", "BUY", 1, 100.0, "BVME", "EUR"), lambda result: _validate_dict_keys(result, ("ok", "warnings", "blockers", "checks_run", "notional", "recommendation"))),
        Case("get_pnl_per_position", "PnL per position", lambda server: server.get_pnl_per_position(), lambda result: _validate_list(result, ("symbol", "secType", "position", "marketValue"), allow_empty=True)),
        Case("resolve_symbol", "Symbol resolution", lambda server: server.resolve_symbol("ENI"), lambda result: _validate_list(result, ("symbol", "secType", "primaryExchange", "currency", "conId"), allow_empty=False)),
        Case("classify_positions_by_strategy", "Strategy classification", lambda server: server.classify_positions_by_strategy(), _validate_strategy_payload),
        Case("screen_universe", "IB scanner read-only screen", lambda server: server.screen_universe({"scan_code": "TOP_PERC_GAIN", "exchange": "NASDAQ"}), lambda result: _validate_list(result, ("symbol", "exchange", "rank", "scan_value"), allow_empty=False)),
        Case("get_portfolio_snapshot", "Portfolio snapshot", lambda server: server.get_portfolio_snapshot(), lambda result: _validate_dict_keys(result, ("as_of", "account", "cash", "net_liquidation", "buying_power", "positions", "exposure_by_currency", "exposure_by_sector", "exposure_by_asset_class", "total_unrealized_pnl", "total_realized_pnl_today"))),
        Case("get_daily_pnl_history", "Explicit ib_async fallback", lambda server: server.get_daily_pnl_history(days_back=30), _validate_daily_pnl_history),
        Case("get_order_status", "Safe negative lookup for non-existent order", lambda server: server.get_order_status(999999999), _validate_order_status_not_found),
        Case("get_executions", "Recent executions", lambda server: server.get_executions(symbol="ENI", days_back=1), lambda result: _validate_list(result, ("execId", "symbol", "orderId"), allow_empty=True)),
        Case("healthcheck", "Server health probe", lambda server: server.healthcheck(), lambda result: _validate_dict_keys(result, ("connected", "ib_server_version", "client_id", "managed_accounts", "active_subscriptions", "open_orders_count", "positions_count", "data_farms", "uptime_seconds", "last_error"))),
        Case("get_recent_errors", "Recent IB errors buffer", lambda server: server.get_recent_errors(10), lambda result: _validate_list(result, (), allow_empty=True)),
        Case("get_server_info", "Static server metadata", lambda server: server.get_server_info(), lambda result: _validate_dict_keys(result, ("mcp_server_version", "ib_async_version", "fastmcp_version", "python_version", "running_since", "host", "port", "client_id"))),
    ]


async def _run_case(module: Any, case: Case) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    try:
        result = await case.call(module)
        ok, detail = case.validator(result)
        return {
            "name": case.name,
            "description": case.description,
            "ok": ok,
            "detail": detail,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "result_preview": _preview(result),
        }
    except Exception as exc:
        return {
            "name": case.name,
            "description": case.description,
            "ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "result_preview": _preview({"exception": repr(exc)}),
        }


def _disconnect_module_ib(module: Any) -> None:
    ib = getattr(module, "_ib", None)
    if ib is None:
        return
    try:
        if ib.isConnected():
            ib.disconnect()
    except Exception:
        pass


async def _main() -> int:
    started_at = datetime.now(timezone.utc)
    try:
        module = _load_server_module()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "import_error": f"{type(exc).__name__}: {exc}",
                    "server_path": str(SERVER_PATH),
                },
                indent=2,
            )
        )
        return 1

    cases = _build_cases(module)
    results: list[dict[str, Any]] = []
    try:
        for case in cases:
            results.append(await _run_case(module, case))
    finally:
        _disconnect_module_ib(module)

    failed = [row for row in results if not row["ok"]]
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "server_path": str(SERVER_PATH),
        "ib_client_id": os.environ.get("IB_CLIENT_ID"),
        "custom_tools_documented": CUSTOM_TOOL_COUNT,
        "total_tools": TOTAL_TOOL_COUNT,
        "read_only_tests_run": len(results),
        "mutating_tools_skipped": MUTATING_TOOLS,
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "results": results,
    }
    print(json.dumps(summary, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))