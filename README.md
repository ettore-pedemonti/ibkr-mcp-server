# IBKR MCP Server

An MCP server that gives AI agents 38 tools to query and trade through Interactive Brokers, built on [`ib_async`](https://github.com/ib-api-reloaded/ib_async) and [FastMCP](https://gofastmcp.com).

> [!WARNING]
> This project is designed and tested **only with IBKR PAPER accounts**.
> Six tools can **place, modify or cancel real orders** (`place_order`, `place_bracket_order`, `place_combo_order`, `modify_order`, `cancel_order`, `cancel_all_open_orders`).
> Use at your own risk. Nothing here is financial advice, and the authors accept no liability for any losses.

## Features

**Orders (10)**
- `place_order`, `place_bracket_order`, `place_combo_order` — submit stock, option, bracket and combo orders
- `modify_order`, `cancel_order`, `cancel_all_open_orders` — change or cancel orders
- `precheck_order` — risk-only dry check (hours, notional, price distance, buying power, concentration)
- `get_live_orders`, `get_order_status`, `get_executions` — order and fill tracking

**Account and data (16)**
- `get_positions`, `get_account_summary`, `get_portfolio_snapshot`, `get_pnl_per_position`, `get_daily_pnl_history`, `classify_positions_by_strategy`
- `get_market_data`, `get_historical_bars`, `get_52w_context`, `get_option_chain`
- `get_fundamentals`, `get_earnings_date`, `get_dividend_calendar` (via yfinance)
- `get_exchange_hours`, `resolve_symbol`, `screen_universe`

**Options analytics (9)**
- `get_market_data_with_greeks`, `bs_price`, `implied_volatility`, `greeks_from_price`
- `prob_itm`, `expected_move`
- `evaluate_multi_leg`, `pmcc_evaluator`, `roll_analyzer`

**Diagnostics (3)**
- `healthcheck`, `get_recent_errors`, `get_server_info`

## Safety by design

- **Live-price gate on `place_order`**: the order is blocked if no live price is available, or if a limit price deviates from the mid/last/close reference by more than `IBKR_ORDER_PRICE_TOLERANCE_PCT` (default `2.0`%).
- **`precheck_order` risk limits** (configurable, evaluated when the tool is called):
  - `IBKR_MAX_ORDER_NOTIONAL` — max order notional, default `5000.0` (blocker)
  - `IBKR_MAX_PRICE_DISTANCE_PCT` — max limit-price distance from reference, default `0.10` (blocker)
  - `IBKR_MAX_CONCENTRATION_PCT` — max post-trade position share of net liquidation, default `0.20` (warning)
  - It also blocks when the exchange is closed or the order exceeds buying power.
- **Paper port by default**: the server connects to `127.0.0.1:4002`.

> [!NOTE]
> The `IBKR_MAX_*` limits are enforced by `precheck_order`, not inside the order-placing tools. The agent (or your prompt/policy) must call `precheck_order` before placing orders.

## Quick start

**Requirements:** Python 3.11+, and IB Gateway or TWS running in **paper** mode with the API enabled (default Gateway paper port `4002`).

```bash
git clone https://github.com/ettore-pedemonti/ibkr-mcp-server.git
cd ibkr-mcp-server
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python server.py   # speaks MCP over stdio
```

Example MCP client configuration (e.g. Claude Desktop):

```json
{
  "mcpServers": {
    "ibkr": {
      "command": "/path/to/ibkr-mcp-server/.venv/bin/python",
      "args": ["/path/to/ibkr-mcp-server/server.py"],
      "env": {
        "IB_HOST": "127.0.0.1",
        "IB_PORT": "4002",
        "IB_CLIENT_ID": "10"
      }
    }
  }
}
```

## Configuration

All settings are environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `IB_HOST` | `127.0.0.1` | IB Gateway / TWS host |
| `IB_PORT` | `4002` | API port (4002 = Gateway paper) |
| `IB_CLIENT_ID` | `10` | Client ID used by the server |
| `IBKR_MAX_ORDER_NOTIONAL` | `5000.0` | Max order notional in `precheck_order` |
| `IBKR_MAX_PRICE_DISTANCE_PCT` | `0.10` | Max limit-price distance from reference (fraction) in `precheck_order` |
| `IBKR_MAX_CONCENTRATION_PCT` | `0.20` | Concentration warning threshold (fraction of net liquidation) |
| `IBKR_ORDER_PRICE_TOLERANCE_PCT` | `2.0` | Max limit-price deviation (in %) enforced by the `place_order` price gate |
| `IBKR_MARKET_DATA_TYPE` | `3` | IB market data type (1 live, 2 frozen, 3 delayed, 4 delayed-frozen) |

## Testing

`test_all_tools.py` is a **read-only** regression script: it calls the read-only tools and skips the order-mutating ones. It requires a running IB Gateway/TWS in paper mode.

```bash
TEST_IB_CLIENT_ID=99 python test_all_tools.py
```

`TEST_IB_CLIENT_ID` (default `99`) sets the client ID used by the script, so it does not clash with a running server.

## Roadmap

- Enforce the `IBKR_MAX_*` risk limits inside the order-placing tools, not only in `precheck_order`
- Unit tests with a mocked `ib_async`
- GitHub Actions CI
- `ruff` linting and type hints

## License

[MIT](LICENSE)
