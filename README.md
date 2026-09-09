# NOVA TRADER V4 CLOUD

Cloud paper-trading engine for Solana.

## What it does
- Runs a live market scan against DEX Screener.
- Pump Hunter + Scalper scoring.
- Position sizing by risk.
- Stop loss, partial take profits, dynamic trailing exit.
- Liquidity-drop and momentum exits.
- Daily loss guard, loss-streak pause, cooldowns.
- PostgreSQL persistence.
- PAPER ONLY. No wallet/private key/live execution.

## Render
`render.yaml` provisions:
- one Python web service
- one PostgreSQL database

During Blueprint setup set `NOVA_ADMIN_KEY` to a long private password.
Do NOT paste that password into GitHub or send it to anyone.

The included Render plan is Free for testing.
For actual always-on operation, upgrade the web service from Free to a paid compute plan.
