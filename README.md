# NOVA TRADER V5.0 ULTIMATE

A cloud-first, mobile-controlled, **PAPER + SHADOW ONLY** Solana trading research system.
LIVE transaction creation/signing/submission is intentionally hard-locked.

## Engines retained from earlier versions
- Solana Pump Hunter
- Micro Scalper
- Perpetual LONG / SHORT engine
- Funding + Open Interest intelligence
- Momentum / buy-pressure / volume acceleration scoring
- Multi-timeframe spot context (5m / 1h / 6h / 24h)
- Dynamic partial take-profit + trailing exits
- Stop-loss, break-even protection, time stop and signal-reversal exit
- Drawdown-aware Risk Engine
- Consecutive-loss cooldown / pause
- Adaptive Forward Optimizer
- Strategy health and bounded threshold adaptation
- Portfolio Brain
- Regime detection: RISK_ON / RISK_OFF / CHOP / HIGH_VOL / WARMUP
- Total / strategy / LONG / SHORT exposure caps
- Correlation Guard
- Execution Reality Engine
- Fee / spread / slippage / impact / latency simulation
- Execution Quality Gate
- Equity curve / expectancy / payoff / profit factor / drawdown
- Market snapshot recorder
- Replay backtest
- Walk-forward out-of-sample validation
- Monte Carlo risk test

## New in V5.0
### Solana On-chain Token Security
Uses read-only Solana JSON-RPC:
- Mint authority state
- Freeze authority state
- SPL / Token-2022 program-owner check
- Largest token-account concentration (Top 1 / Top 5 / Top 10)
- Market-age / liquidity / liquidity-to-market-cap risk modifiers
- Cached scans to reduce RPC load
- Shadow Mode can require a valid security scan and reject hard-risk conditions

Important: largest-account concentration can include LP, treasury or burn accounts. This layer is a risk screen, not a full smart-contract audit.

### Shadow Mode
- Uses live market observations while keeping all execution hypothetical
- Requires simulated execution
- Perp Shadow entries require the primary perp market-data source
- Spot Shadow entries can require an on-chain security scan
- Continuous Shadow observation time is tracked for the readiness gate
- `LIVE` mode is rejected by the API

### Shadow Venue Router / Signal Quality
- Data-source quality
- Data freshness
- Estimated execution quality
- Token/market security quality
- Portfolio weight
- Composite opportunity quality
- Route-quality gate

This is a read-only/simulated venue-selection layer. It does not send exchange transactions.

### Live-Readiness Gate
Checks:
- Closed trade sample size
- Profit factor
- Positive expectancy
- Max drawdown
- Execution-simulator state
- Observed execution costs
- Market-data health
- Security-scan coverage
- Continuous Shadow observation
- Walk-forward robustness
- Monte Carlo downside risk

Even a 100/100 readiness result **does not unlock LIVE** and does not guarantee future profit.

### System Watchdog + Audit Trail
- Data freshness
- Engine heartbeat
- Perp-source status
- Error streak
- Position open/close system events
- Decision logs for opened and blocked trades
- Reason, signal score, composite quality, route quality and security score

## Modes
- `PAPER`: research/paper account
- `SHADOW`: stricter simulated production observation
- `LIVE`: hard-locked and returns HTTP 403

## Key API endpoints
- `/`
- `/health`
- `/api/dashboard`
- `/api/full-status`
- `/api/readiness`
- `/api/system`
- `/api/security`
- `/api/security/rescan/{mint}`
- `/api/decisions`
- `/api/execution`
- `/api/execution/estimate`
- `/api/portfolio`
- `/api/optimizer`
- `/api/research/status`
- `/api/research/run`
- `/api/mode/{PAPER|SHADOW}`
- `/api/control/{start|stop|kill|reset-paper}`
- `/api/settings`
- `/api/watch`
- `/api/watch/remove`

## Deployment
1. Replace `app.py` in the `nova-trader-cloud` repository.
2. Keep the already-working dependency setup, or use this package's `requirements.txt`.
3. Deploy on Render.
4. Confirm `/` reports version `5.0.0`.
5. Replace `index.html` in `nova-trader-mobile`.
6. Open the GitHub Pages dashboard with `?v=50`.
7. Keep PAPER mode first. Move to SHADOW only after the backend is stable.

## Operational note
A free hosting plan may sleep or throttle. That is unsuitable for a genuine always-on production trading service. V5.0 is still a research/shadow system.
