# Hermes IBKR MCP

Custom IBKR FastMCP server for Hermes, built on `ib_async` and exposed over `stdio`.

## Current Surface

- Custom MCP tools implemented in `server.py`: `38`
- Total tools visible in Hermes: `42`
- The delta comes from `4` generic FastMCP prompt/resource surfaces in addition to the custom IBKR toolset.
- Read-only regression script: `test_all_tools.py`
- Default runtime target: `<path>/server.py`
- Default Python environment on Hermes: `<venv>/bin/python`

## Runtime

Start the server directly:

```bash
<venv>/bin/python <path>/server.py
```

Smoke test the server without leaving it running:

```bash
timeout 3 <venv>/bin/python <path>/server.py
```

Run the read-only validation suite:

```bash
TEST_IB_CLIENT_ID=99 <venv>/bin/python <path>/test_all_tools.py
```

## Dependencies

- `fastmcp`
- `ib_async`
- `numpy`
- `scipy`
- `yfinance`

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `IB_HOST` | `127.0.0.1` | IB Gateway or TWS host |
| `IB_PORT` | `4002` | Paper gateway port |
| `IB_CLIENT_ID` | `10` | Client ID used by the MCP server process |
| `IBKR_MAX_ORDER_NOTIONAL` | `5000.0` | Max notional used by `precheck_order` |
| `IBKR_MAX_PRICE_DISTANCE_PCT` | `0.10` | Max limit-price distance from reference price |
| `IBKR_MAX_CONCENTRATION_PCT` | `0.20` | Max post-trade concentration warning threshold |
| `TEST_IB_CLIENT_ID` | `99` | Client ID override used by `test_all_tools.py` before importing `server.py` |

## Common Response Shapes

Trade status payload:

```json
{
  "orderId": 123,
  "permId": 456,
  "symbol": "ENI",
  "secType": "STK",
  "action": "BUY",
  "quantity": 1,
  "orderType": "LMT",
  "lmtPrice": 23.5,
  "auxPrice": null,
  "status": "Submitted",
  "filled": 0,
  "remaining": 1,
  "avgFillPrice": 0.0,
  "whyHeld": "",
  "log": []
}
```

YFinance envelope:

```json
{
  "symbol": "NVDA",
  "source": "yfinance",
  "as_of": "2026-05-22",
  "data": {},
  "source_note": "yfinance data - not from Reuters/Refinitiv. Activate IBKR Reuters subscription for institutional-grade fundamentals.",
  "log": []
}
```

Honest IB permission error pattern:

```json
{
  "error": "no market data permission for symbol/exchange",
  "details": {
    "code": 354,
    "message": "Requested market data is not subscribed",
    "reqId": 37,
    "time": "2026-05-22T17:00:00+00:00"
  }
}
```

Strategy classification payload:

```json
{
  "account": "DU1234567",
  "cash_balances": {
    "EUR": 100000.00
  },
  "strategies": [
    {
      "name": "long_stocks",
      "positions": [],
      "notional": 0.0
    }
  ]
}
```

## Tool Catalog

### Trading And Order Lifecycle

| Tool | Mode | Inputs | Returns |
| --- | --- | --- | --- |
| `place_order` | mutating | `symbol, side, quantity, order_type="LMT", limit_price=None, stop_price=None, exchange="SMART", currency="USD", sec_type="STK", expiry=None, strike=None, right=None, tif="DAY", outside_rth=False, wait_seconds=5` | trade status dict with IB log |
| `cancel_order` | mutating | `order_id` | `{"cancelled": bool, "orderId": int}` or error |
| `get_positions` | read-only | none | `list[{account, symbol, secType, exchange, currency, position, avgCost}]` |
| `get_live_orders` | read-only | `all_clients=True` | `list[{orderId, symbol, secType, action, quantity, orderType, lmtPrice, tif, status, filled, remaining, whyHeld}]` |
| `get_account_summary` | read-only | none | `dict[tag[:currency] -> value]` |
| `modify_order` | mutating | `order_id, new_limit_price=None, new_quantity=None, new_stop_price=None` | trade status dict plus `modified_fields` |
| `get_order_status` | read-only | `order_id` | trade status dict or `not found` error |
| `get_executions` | read-only | `symbol=None, days_back=1` | `list[{execId, time, symbol, side, shares, price, orderId, permId, commission}]` |
| `cancel_all_open_orders` | mutating | `symbol=None, side=None` | `{"cancelled": int, "orderIds": [], "errors": []}` |
| `place_bracket_order` | mutating | `symbol, side, quantity, entry_price, take_profit_price, stop_loss_price, exchange="SMART", currency="USD", sec_type="STK", expiry=None, strike=None, right=None` | parent/take-profit/stop-loss ids plus per-leg statuses |
| `place_combo_order` | mutating | `legs, total_quantity, order_type="LMT", limit_price=None, exchange="SMART", currency="USD", action="BUY", tif="DAY"` | trade status dict |
| `precheck_order` | read-only | `symbol, side, quantity, limit_price, exchange="SMART", currency="USD", sec_type="STK"` | `{"ok", "warnings", "blockers", "checks_run", "reference_price", "notional", "buying_power", "concentration_after_pct", "recommendation"}` |
| `get_exchange_hours` | read-only | `exchange` | `{"exchange", "timezone", "open_local", "close_local", "is_open_now", "next_open", "next_close", "trading_days"}` |

### Market Data And Screening

| Tool | Mode | Inputs | Returns |
| --- | --- | --- | --- |
| `get_market_data` | read-only | `symbol, exchange="SMART", currency="USD", sec_type="STK", expiry=None, strike=None, right=None` | quote dict or honest permission error |
| `get_option_chain` | read-only | `symbol, exchange="SMART", currency="USD"` | `list[{exchange, tradingClass, expirations, strikes}]` |
| `get_historical_bars` | read-only | `symbol, duration="1 D", bar_size="1 hour", what_to_show="TRADES", exchange="SMART", currency="USD", sec_type="STK", end_date_time="", expiry=None, strike=None, right=None` | `list[{time, open, high, low, close, volume}]` |
| `get_52w_context` | read-only | `symbol, exchange="SMART", currency="USD"` | `{"current", "high_52w", "low_52w", "pct_from_high", "pct_from_low", "as_of"}` |
| `get_market_data_with_greeks` | read-only | `symbol, expiry, strike, right, exchange="SMART", currency="USD"` | quote/greeks dict or honest permission error |
| `resolve_symbol` | read-only | `query` | `list[{symbol, secType, primaryExchange, currency, conId, longName}]` |
| `screen_universe` | read-only | `criteria` dict with `scan_code`, `exchange`, optional `limit`, `instrument`, `location_code`, `above_price`, `below_price`, `above_volume`, `market_cap_above`, `market_cap_below`, `stock_type_filter` | `list[{symbol, exchange, rank, scan_value}]` |

### Analytics And Fundamentals

| Tool | Mode | Inputs | Returns |
| --- | --- | --- | --- |
| `bs_price` | read-only | `spot, strike, time_to_expiry_years, volatility, risk_free_rate=0.05, dividend_yield=0.0, right="C"` | `{"price", "d1", "d2", "delta", "gamma", "theta", "vega", "rho"}` |
| `implied_volatility` | read-only | `option_price, spot, strike, time_to_expiry_years, risk_free_rate=0.05, dividend_yield=0.0, right="C"` | `{"iv": float}` or `{"error": "no convergence"}` |
| `greeks_from_price` | read-only | `option_price, spot, strike, time_to_expiry_years, risk_free_rate=0.05, dividend_yield=0.0, right="C"` | implied vol plus Black-Scholes greeks |
| `prob_itm` | read-only | `spot, strike, time_to_expiry_years, volatility, risk_free_rate=0.05, dividend_yield=0.0, right="C"` | `{"prob_itm": float}` |
| `expected_move` | read-only | `spot, volatility, time_to_expiry_years` | `{"expected_move", "expected_up", "expected_down", "expected_high", "expected_low"}` |
| `evaluate_multi_leg` | read-only | `legs, spot, time_to_expiry_years, volatility, risk_free_rate=0.05` | `{"net_debit_credit", "max_profit", "max_loss", "breakevens", "greeks_aggregate"}` |
| `pmcc_evaluator` | read-only | `spot, long_strike, long_expiry_days, short_strike, short_expiry_days, volatility, risk_free_rate=0.05` | PMCC debit/breakeven/ROI summary |
| `roll_analyzer` | read-only | `current_strike, current_expiry_days, current_right, new_strike, new_expiry_days, spot, volatility, risk_free_rate=0.05` | roll cost, greek changes, recommendation |
| `get_fundamentals` | read-only | `symbol, report_type="ReportsFinSummary", exchange="SMART", currency="USD"` | YFinance envelope with fundamentals snapshot |
| `get_earnings_date` | read-only | `symbol, exchange="SMART", currency="USD"` | YFinance envelope with earnings calendar fields |
| `get_dividend_calendar` | read-only | `symbol, exchange="SMART", currency="USD"` | YFinance envelope with dividend fields |

### Portfolio And Observability

| Tool | Mode | Inputs | Returns |
| --- | --- | --- | --- |
| `get_pnl_per_position` | read-only | `account=None` | `list[{account, symbol, secType, exchange, currency, conId, position, avgCost, marketPrice, marketValue, unrealizedPnL, realizedPnL, dailyPnL}]` |
| `classify_positions_by_strategy` | read-only | none | strategy classification payload |
| `get_portfolio_snapshot` | read-only | none | portfolio snapshot with cash, net liquidation, buying power, positions, exposures, total PnL |
| `get_daily_pnl_history` | read-only | `account=None, days_back=30` | explicit fallback `{"error": "not available via ib_async; use IB Client Portal", "history": []}` |
| `healthcheck` | read-only | none | `{"connected", "ib_server_version", "client_id", "managed_accounts", "active_subscriptions", "open_orders_count", "positions_count", "data_farms", "uptime_seconds", "last_error"}` |
| `get_recent_errors` | read-only | `limit=20` | `list[{time, code, message, reqId}]` |
| `get_server_info` | read-only | none | `{"mcp_server_version", "ib_async_version", "fastmcp_version", "python_version", "running_since", "host", "port", "client_id"}` |

## Hermes Usage Examples

```text
healthcheck()
```

```text
resolve_symbol("ENI")
```

```text
get_market_data("NVDA")
```

```text
get_fundamentals("NVDA")
```

```text
precheck_order("ENI", "BUY", 1, 23.50, "BVME", "EUR")
```

```text
screen_universe({"scan_code": "TOP_PERC_GAIN", "exchange": "NASDAQ"})
```

## Known Limitations

- The paper account does not have every market-data entitlement. Quote and greeks tools intentionally return explicit permission errors instead of fabricated values.
- On the current paper account, ENI/BVME real-time data is not entitled. `precheck_order` falls back to held-position `marketPrice` when available.
- Fundamentals use `yfinance`, not Reuters/Refinitiv. EU equity responses always include a stricter warning note. Crypto fundamentals always return `{"error": "use CoinGecko for crypto fundamentals"}`.
- `screen_universe` uses IB scanners only. On this account, direct `STK.US.NASDAQ` scanner location failed with IB `162`, so the implementation queries `STK.US.MAJOR` and filters NASDAQ rows via `contractDetails.marketName`.
- `get_daily_pnl_history` is intentionally a documented fallback because the required endpoint is not exposed via this `ib_async` surface.
- `test_all_tools.py` intentionally skips all mutating tools. It exercises only read-only paths and safe negative lookups.

## Read-Only Test Suite

`test_all_tools.py` imports `server.py` directly, overrides `IB_CLIENT_ID` to `TEST_IB_CLIENT_ID` or `99`, and executes `32` safe checks across the read-only surface.

What it does:

- Verifies structural payloads for all read-only tools.
- Accepts honest limitation branches where the tool is designed to return them, for example market-data permission errors and the explicit daily-PnL fallback.
- Returns JSON with per-tool pass/fail, a compact result preview, and a non-zero exit code if any read-only check fails.

What it does not do:

- It does not place, cancel, or modify orders.
- It does not call any combo or bracket submission path.
- It does not rely on Hermes being open; it imports the module directly and talks to IB through a separate client id.