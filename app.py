import os, asyncio, math, time, random, json
from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx
import websockets

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, String, Float, Integer, Boolean, DateTime, Text, select, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

APP_VERSION = "6.6.0"
DEX = "https://api.dexscreener.com"
VELOCITY_DATA = "https://data.velocity.exchange"
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
LIVE_EXECUTION_LOCKED = True

PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY","").strip()
PUMPPORTAL_TRADE_STREAM_ENABLED = os.getenv("PUMPPORTAL_TRADE_STREAM_ENABLED","false").lower() in ("1","true","yes","on")
PUMPPORTAL_WS_BASE = "wss://pumpportal.fun/api/data"

# Legacy public-price fallback only. Primary perp intelligence in V4.2 comes from Velocity Data API.
PERP_UNIVERSE = {
    "So11111111111111111111111111111111111111112": {"market": "SOL-PERP", "symbol": "SOL"},
    "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh": {"market": "BTC-PERP", "symbol": "BTC"},
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs": {"market": "ETH-PERP", "symbol": "ETH"},
}
ADMIN_KEY = os.getenv("NOVA_ADMIN_KEY", "change-me-now")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./nova_trader.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase): pass

class KV(Base):
    __tablename__ = "kv"
    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[str] = mapped_column(Text)

class Position(Base):
    __tablename__ = "positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mint: Mapped[str] = mapped_column(String(100), index=True)
    symbol: Mapped[str] = mapped_column(String(40))
    name: Mapped[str] = mapped_column(String(120))
    strategy: Mapped[str] = mapped_column(String(20))
    entry_price: Mapped[float] = mapped_column(Float)
    last_price: Mapped[float] = mapped_column(Float)
    peak_price: Mapped[float] = mapped_column(Float)
    initial_notional: Mapped[float] = mapped_column(Float)
    remaining_cost: Mapped[float] = mapped_column(Float)
    locked_pnl: Mapped[float] = mapped_column(Float, default=0)
    tp1: Mapped[bool] = mapped_column(Boolean, default=False)
    tp2: Mapped[bool] = mapped_column(Boolean, default=False)
    tp3: Mapped[bool] = mapped_column(Boolean, default=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mint: Mapped[str] = mapped_column(String(100), index=True)
    symbol: Mapped[str] = mapped_column(String(40))
    strategy: Mapped[str] = mapped_column(String(20))
    pnl: Mapped[float] = mapped_column(Float)
    pnl_pct: Mapped[float] = mapped_column(Float)
    reason: Mapped[str] = mapped_column(String(80))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class EquityPoint(Base):
    __tablename__ = "equity_points"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    equity: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)
    drawdown_pct: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class PositionFeature(Base):
    __tablename__ = "position_features"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    strategy: Mapped[str] = mapped_column(String(30), index=True)
    entry_score: Mapped[float] = mapped_column(Float)
    long_score: Mapped[float] = mapped_column(Float, default=0)
    short_score: Mapped[float] = mapped_column(Float, default=0)
    pump_score: Mapped[float] = mapped_column(Float, default=0)
    scalp_score: Mapped[float] = mapped_column(Float, default=0)
    market_risk: Mapped[float] = mapped_column(Float, default=0)
    direction_edge: Mapped[float] = mapped_column(Float, default=0)
    funding_rate: Mapped[float] = mapped_column(Float, default=0)
    open_interest_usd: Mapped[float] = mapped_column(Float, default=0)
    volatility_regime: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class TradeFeature(Base):
    __tablename__ = "trade_features"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    strategy: Mapped[str] = mapped_column(String(30), index=True)
    entry_score: Mapped[float] = mapped_column(Float)
    market_risk: Mapped[float] = mapped_column(Float, default=0)
    direction_edge: Mapped[float] = mapped_column(Float, default=0)
    funding_rate: Mapped[float] = mapped_column(Float, default=0)
    open_interest_usd: Mapped[float] = mapped_column(Float, default=0)
    volatility_regime: Mapped[str] = mapped_column(String(20), default="UNKNOWN", index=True)
    pnl: Mapped[float] = mapped_column(Float)
    pnl_pct: Mapped[float] = mapped_column(Float)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mint: Mapped[str] = mapped_column(String(120), index=True)
    symbol: Mapped[str] = mapped_column(String(50), index=True)
    data_source: Mapped[str] = mapped_column(String(30), index=True)
    perp_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    price: Mapped[float] = mapped_column(Float)
    pump_score: Mapped[float] = mapped_column(Float, default=0)
    scalp_score: Mapped[float] = mapped_column(Float, default=0)
    long_score: Mapped[float] = mapped_column(Float, default=0)
    short_score: Mapped[float] = mapped_column(Float, default=0)
    market_risk: Mapped[float] = mapped_column(Float, default=0)
    direction: Mapped[str] = mapped_column(String(12), default="WAIT")
    direction_edge: Mapped[float] = mapped_column(Float, default=0)
    funding_rate: Mapped[float] = mapped_column(Float, default=0)
    open_interest_usd: Mapped[float] = mapped_column(Float, default=0)
    volatility_regime: Mapped[str] = mapped_column(String(20), default="UNKNOWN")
    liquidity: Mapped[float] = mapped_column(Float, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class ExecutionEvent(Base):
    __tablename__ = "execution_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    trade_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    symbol: Mapped[str] = mapped_column(String(50), index=True)
    strategy: Mapped[str] = mapped_column(String(30), index=True)
    phase: Mapped[str] = mapped_column(String(20), index=True)
    side: Mapped[str] = mapped_column(String(10))
    mid_price: Mapped[float] = mapped_column(Float)
    fill_price: Mapped[float] = mapped_column(Float)
    notional_usd: Mapped[float] = mapped_column(Float)
    fee_bps: Mapped[float] = mapped_column(Float, default=0)
    spread_bps: Mapped[float] = mapped_column(Float, default=0)
    slippage_bps: Mapped[float] = mapped_column(Float, default=0)
    impact_bps: Mapped[float] = mapped_column(Float, default=0)
    latency_bps: Mapped[float] = mapped_column(Float, default=0)
    adverse_bps: Mapped[float] = mapped_column(Float, default=0)
    all_in_bps: Mapped[float] = mapped_column(Float, default=0)
    latency_ms: Mapped[float] = mapped_column(Float, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class SystemEvent(Base):
    __tablename__ = "system_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    level: Mapped[str] = mapped_column(String(12), index=True)
    code: Mapped[str] = mapped_column(String(50), index=True)
    message: Mapped[str] = mapped_column(String(240))
    detail: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class DecisionLog(Base):
    __tablename__ = "decision_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), index=True)
    mint: Mapped[str] = mapped_column(String(120), index=True)
    strategy: Mapped[str] = mapped_column(String(30), index=True)
    operating_mode: Mapped[str] = mapped_column(String(12), index=True)
    outcome: Mapped[str] = mapped_column(String(20), index=True)
    reason: Mapped[str] = mapped_column(String(120))
    signal_score: Mapped[float] = mapped_column(Float, default=0)
    quality_score: Mapped[float] = mapped_column(Float, default=0)
    route_quality: Mapped[float] = mapped_column(Float, default=0)
    security_score: Mapped[float] = mapped_column(Float, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

Base.metadata.create_all(engine)

DEFAULTS = {
    "cash": os.getenv("PAPER_START_BALANCE", "5000"),
    "start_balance": os.getenv("PAPER_START_BALANCE", "5000"),
    "bot_enabled": "false",
    "killed": "false",
    "risk_pct": "0.75",
    "max_position_pct": "10",
    "max_positions": "4",
    "stop_loss_pct": "4",
    "daily_loss_limit_pct": "3",
    "min_pump_score": "72",
    "min_scalp_score": "74",
    "min_long_score": "72",
    "min_short_score": "72",
    "perp_leverage": "1.0",
    "min_direction_edge": "8",
    "min_perp_oi_usd": "100000",
    "max_abs_funding_rate": "0.005",
    "block_extreme_volatility": "true",
    "adaptive_enabled": "true",
    "optimizer_min_samples": "20",
    "max_adaptive_shift": "5",
    "strategy_pause_min_samples": "10",
    "strategy_pause_minutes": "45",
    "drawdown_soft_cut_pct": "3",
    "drawdown_hard_cut_pct": "7",
    "snapshot_interval_sec": "60",
    "snapshot_retention_days": "7",
    "replay_hold_minutes": "15",
    "replay_take_profit_pct": "8",
    "research_days": "7",
    "monte_carlo_runs": "1000",
    "portfolio_brain_enabled": "true",
    "max_total_exposure_pct": "35",
    "max_strategy_exposure_pct": "18",
    "max_direction_exposure_pct": "25",
    "correlation_threshold": "0.80",
    "correlation_lookback_points": "20",
    "min_portfolio_weight": "0.35",
    "execution_simulator_enabled": "true",
    "spot_fee_bps": "30",
    "perp_fee_bps": "10",
    "base_spread_bps": "8",
    "slippage_floor_bps": "2",
    "impact_coefficient_bps": "40",
    "simulated_latency_ms": "450",
    "max_execution_cost_bps": "120",
    "operating_mode": "PAPER",
    "shadow_started_at": "",
    "max_data_age_sec": "90",
    "min_token_security_score": "60",
    "security_scan_ttl_sec": "600",
    "security_scan_top_n": "5",
    "security_required_shadow": "true",
    "security_hard_block_shadow": "true",
    "min_route_quality": "55",
    "spot_max_hold_minutes": "120",
    "perp_max_hold_minutes": "240",
    "reversal_exit_edge": "15",
    "breakeven_trigger_pct": "6",
    "breakeven_exit_pct": "0.5",
    "profit_lock_enabled": "true",
    "capital_shield_enabled": "true",
    "scalp_max_loss_pct": "1.75",
    "pump_max_loss_pct": "2.75",
    "perp_max_loss_pct": "1.50",
    "scratch_after_minutes": "8",
    "scratch_loss_pct": "0.75",
    "scratch_min_peak_pct": "0.50",
    "global_equity_guard_pct": "2.50",
    "capital_shield_min_liquidity": "50000",
    "capital_shield_min_market_quality": "65",
    "max_spot_position_liquidity_pct": "0.75",
    "max_spot_abs_m5_pct": "25",

    "sniper_enabled": "true",
    "min_sniper_score": "78",
    "sniper_scan_interval_sec": "8",
    "sniper_min_buy_pressure": "60",
    "sniper_min_volume_accel": "52",
    "sniper_min_m5_pct": "0.30",
    "sniper_max_m5_pct": "12",
    "sniper_max_age_minutes": "360",
    "sniper_max_positions": "1",
    "sniper_risk_multiplier": "0.50",
    "sniper_max_loss_pct": "1.00",
    "sniper_scratch_minutes": "1.50",
    "sniper_scratch_loss_pct": "0.35",
    "sniper_max_hold_minutes": "5",

    "realtime_pulse_enabled": "true",
    "pulse_min_score": "76",
    "pulse_min_events_5s": "4",
    "pulse_min_buy_pressure_5s": "68",
    "pulse_min_unique_buyers_10s": "3",
    "pulse_max_m5_pct": "10",
    "pulse_cooldown_sec": "45",
    "pulse_eval_timeout_sec": "5",
    "realtime_exit_enabled": "true",
    "realtime_exit_min_interval_ms": "250",
    "realtime_exit_rest_fallback_ms": "750",
    "pulse_max_trade_subscriptions": "120",
    "pulse_subscription_ttl_sec": "120",

    "adaptive_pulse_enabled": "true",
    "pulse_diag_window_sec": "300",
    "pulse_adapt_interval_sec": "60",
    "pulse_adapt_min_observations": "40",
    "pulse_score_floor": "70",
    "pulse_events_floor": "3",
    "pulse_buy_pressure_floor": "62",
    "pulse_unique_buyers_floor": "2",
    "pulse_score_ceiling": "84",
    "pulse_buy_pressure_ceiling": "78",
    "pulse_unique_buyers_ceiling": "5",

    "pulse_recovery_score": "72",
    "pulse_recovery_events_5s": "3",
    "pulse_recovery_buy_pressure": "64",
    "pulse_recovery_unique_buyers": "2",
    "pulse_confirmation_required": "2",
    "pulse_confirmation_window_sec": "4",
    "pulse_confirmation_min_gap_ms": "300",

    "edge_governor_enabled": "true",
    "governor_min_samples": "6",
    "governor_recent_trades": "12",
    "governor_probation_risk_multiplier": "0.25",
    "governor_caution_risk_multiplier": "0.50",
    "governor_min_profit_factor": "1.05",
    "governor_pause_minutes": "90",
    "governor_loss_streak_limit": "2",
    "governor_loss_streak_pause_minutes": "30",
    "survival_daily_loss_pct": "1.50",
    "survival_combined_loss_pct": "1.50",
    "recovery_spot_liquidity_position_cap_pct": "0.50",
    "recovery_sniper_loss_cap_pct": "0.75",
    "recovery_scalp_loss_cap_pct": "1.25",
    "recovery_pump_loss_cap_pct": "1.75",
    "recovery_perp_loss_cap_pct": "1.25",

    "selective_entry_router_enabled": "true",
    "router_soft_market_quality_floor": "62",
    "router_soft_liquidity_floor": "35000",
    "router_min_signal_margin": "2",
    "router_min_buy_pressure": "55",
    "router_max_soft_m5_pct": "12",
    "router_soft_risk_multiplier": "0.50",
    "router_max_soft_entries_per_hour": "2",

    # V6.6 Launch Sniper — first-seconds PAPER engine.
    "launch_sniper_enabled": "true",
    "launch_max_active_watch": "24",
    "launch_watch_ttl_sec": "30",
    "launch_entry_max_age_sec": "18",
    "launch_min_age_sec": "0.35",
    "launch_min_score": "74",
    "launch_min_events_2s": "3",
    "launch_min_unique_buyers_2s": "2",
    "launch_min_buy_pressure_2s": "68",
    "launch_min_buy_sol_2s": "0.15",
    "launch_max_top_buyer_share_pct": "65",
    "launch_max_mcap_multiple": "1.85",
    "launch_min_mcap_multiple": "0.90",
    "launch_creator_window_sec": "600",
    "launch_creator_max_tokens": "3",
    "launch_duplicate_symbol_max": "2",
    "launch_confirmation_required": "2",
    "launch_confirmation_window_sec": "1.6",
    "launch_confirmation_gap_ms": "150",
    "launch_max_positions": "1",
    "launch_max_position_pct": "0.50",
    "launch_max_entries_per_hour": "4",
    "launch_raw_stop_pct": "1.00",
    "launch_max_net_loss_pct": "7.00",
    "launch_scratch_after_sec": "3.0",
    "launch_scratch_peak_pct": "1.0",
    "launch_scratch_loss_pct": "4.5",
    "launch_max_hold_sec": "22",
    "launch_flow_reversal_after_sec": "1.0",
    "launch_flow_reversal_pressure": "44",
    "launch_protocol_fee_bps": "125",
    "launch_interface_fee_bps": "50",
    "launch_slippage_floor_bps": "35",
    "launch_impact_coefficient_bps": "180",

    "daily_profit_target_pct": "10.0",
    "daily_profit_secure_buffer_pct": "0.25",
    "daily_de_risk_start_pct": "7.0",
    "daily_de_risk_multiplier": "0.50",
    "daily_target_lock_enabled": "true",
    "no_martingale": "true",
    "max_total_open_risk_pct": "3.0",

    "position_watch_interval_sec": "4",
    "stale_position_price_sec": "45",
    "readiness_min_trades": "100",
    "readiness_min_pf": "1.15",
    "readiness_min_shadow_hours": "24",
    "readiness_max_drawdown_pct": "10",
    "readiness_max_mc_below_start_pct": "35",
    "readiness_min_wf_robust": "1",
    "readiness_max_exec_bps": "80",
    "min_liquidity": "20000",
    "min_market_risk": "70",
    "min_spot_market_quality": "55",
    "execution_cost_pct": "0.50",
    "cooldown_minutes": "10",
    "max_consecutive_losses": "3",
    "loss_pause_minutes": "30",
    "watchlist": "",
}

def init_defaults():
    with SessionLocal() as s:
        for k,v in DEFAULTS.items():
            if not s.get(KV, k):
                s.add(KV(key=k, value=v))
        s.commit()
init_defaults()

def getv(key, default=None):
    with SessionLocal() as s:
        row = s.get(KV, key)
        return row.value if row else default

def setv(key, value):
    with SessionLocal() as s:
        row = s.get(KV, key)
        if row: row.value = str(value)
        else: s.add(KV(key=key, value=str(value)))
        s.commit()

def f(key): return float(getv(key, DEFAULTS.get(key, "0")))
def i(key): return int(float(getv(key, DEFAULTS.get(key, "0"))))
def b(key): return getv(key, "false").lower() == "true"

app = FastAPI(title="NOVA Trader Cloud", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

runtime = {
    "candidates": [],
    "last_refresh": None,
    "last_error": None,
    "prev_liq": {},
    "prev_oi": {},
    "price_history": {},
    "velocity_markets": {},
    "velocity_source_ok": False,
    "last_velocity_refresh": None,
    "last_equity_snapshot": 0,
    "last_market_snapshot": 0,
    "last_snapshot_cleanup": 0,
    "last_portfolio_block": None,
    "last_execution_block": None,
    "execution_blocks": 0,
    "token_security": {},
    "last_spot_refresh": None,
    "last_perp_refresh": None,
    "last_successful_loop": None,
    "last_position_watch": 0,
    "position_watcher_alive": False,
    "position_watch_error": None,
    "position_price_seen": {},

    "sniper_scanner_alive": False,
    "last_sniper_scan": 0,
    "sniper_scan_error": None,
    "sniper_entries": 0,
    "sniper_candidates": [],
    "daily_target_locked": False,
    "daily_target_lock_time": None,

    "pulse_stream_connected": False,
    "pulse_stream_mode": "OFF",
    "pulse_stream_error": None,
    "pulse_last_event": 0,
    "pulse_events_total": 0,
    "pulse_buffers": {},
    "pulse_hot": {},
    "pulse_evaluating": set(),
    "pulse_subscribed": set(),
    "pulse_entries": 0,
    "pulse_subscription_birth": {},
    "realtime_exit_refs": {},
    "realtime_exit_last_eval": {},
    "realtime_exit_checks": 0,
    "realtime_exit_direct_marks": 0,
    "realtime_exit_rest_checks": 0,
    "realtime_exit_last_event": 0,
    "realtime_exit_error": None,

    "pulse_diag_events": [],
    "pulse_diag_dedupe": {},
    "pulse_recent_best": {},
    "pulse_adaptive_shifts": {"score":0.0,"events":0,"pressure":0.0,"buyers":0},
    "pulse_adaptive_last": 0,
    "pulse_adaptive_actions": [],
    "pulse_last_perf_trade_id": 0,
    "pulse_confirmations": {},
    "governor_pauses": {},
    "governor_last_reason": {},
    "router_soft_entry_times": [],
    "router_last_entry": None,
    "router_soft_entries": 0,

    "launch_watch": {},
    "launch_marks": {},
    "launch_confirmations": {},
    "launch_evaluating": set(),
    "launch_creator_history": {},
    "launch_symbol_history": {},
    "launch_entries": 0,
    "launch_entry_times": [],
    "launch_new_tokens": 0,
    "launch_trades_seen": 0,
    "launch_diag": {},
    "launch_best": None,
    "launch_last_event": 0,
    "launch_last_entry": None,

    "engine_error_streak": 0,
    "last_decision_log": {},
    "last_event_log": {},
    "last_research_report": None,
    "cooldowns": {},
    "strategy_pauses": {},
    "pause_until": None,
    "loop_alive": False,
}

position_manage_lock = asyncio.Lock()
entry_lock = asyncio.Lock()

def auth(x_nova_key: Optional[str]):
    if not x_nova_key or x_nova_key != ADMIN_KEY:
        raise HTTPException(401, "Invalid NOVA admin key")

def nz(v, d=0.0):
    try: return float(v or d)
    except: return d


def operating_mode():
    mode=str(getv("operating_mode","PAPER")).upper()
    return mode if mode in ("PAPER","SHADOW") else "PAPER"

def iso_age_seconds(value):
    if not value:return 10**9
    try:
        dt=datetime.fromisoformat(value)
        if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
        return max(0,(datetime.now(timezone.utc)-dt).total_seconds())
    except:return 10**9

def record_event(level,code_name,message,detail=None,dedupe_sec=180):
    now=time.time()
    key=f"{level}:{code_name}:{message}"
    prev=runtime["last_event_log"].get(key,0)
    if now-prev<dedupe_sec:return
    runtime["last_event_log"][key]=now
    try:
        with SessionLocal() as s:
            s.add(SystemEvent(
                level=str(level)[:12],code=str(code_name)[:50],message=str(message)[:240],
                detail=json.dumps(detail or {},default=str)[:8000],
                created_at=datetime.now(timezone.utc)
            ));s.commit()
    except:pass

def log_decision(c,strategy,outcome,reason,signal_score=0,quality_score=0,route_quality=0,security_score=0):
    now=time.time()
    key=f"{c.get('mint')}:{strategy}:{outcome}:{reason}"
    if now-runtime["last_decision_log"].get(key,0)<120:return
    runtime["last_decision_log"][key]=now
    try:
        with SessionLocal() as s:
            s.add(DecisionLog(
                symbol=str(c.get("symbol") or "?"),mint=str(c.get("mint") or ""),
                strategy=strategy,operating_mode=operating_mode(),
                outcome=str(outcome)[:20],reason=str(reason)[:120],
                signal_score=nz(signal_score),quality_score=nz(quality_score),
                route_quality=nz(route_quality),security_score=nz(security_score),
                created_at=datetime.now(timezone.utc)
            ));s.commit()
    except:pass

async def solana_rpc(client,method,params):
    payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
    r=await client.post(SOLANA_RPC_URL,json=payload,timeout=20)
    r.raise_for_status()
    body=r.json()
    if body.get("error"):raise RuntimeError(str(body["error"]))
    return body.get("result")

async def scan_token_security(client,c,force=False):
    if c.get("perp_eligible"):
        return {"status":"N/A","score":100,"hard_block":False,"flags":[],"source":"PERP_MARKET"}
    mint=str(c.get("mint") or "")
    cached=runtime["token_security"].get(mint)
    ttl=i("security_scan_ttl_sec")
    if cached and not force and time.time()-cached.get("_ts",0)<ttl:
        return {k:v for k,v in cached.items() if k!="_ts"}
    try:
        account=await solana_rpc(client,"getAccountInfo",[
            mint,{"encoding":"jsonParsed","commitment":"confirmed"}
        ])
        value=(account or {}).get("value")
        if not value:raise RuntimeError("mint account not found")
        owner=str(value.get("owner") or "")
        data=value.get("data") or {}
        parsed=data.get("parsed") if isinstance(data,dict) else None
        info=(parsed or {}).get("info") or {}
        supply_raw=float(info.get("supply") or 0)
        mint_auth=info.get("mintAuthority")
        freeze_auth=info.get("freezeAuthority")

        largest=await solana_rpc(client,"getTokenLargestAccounts",[
            mint,{"commitment":"confirmed"}
        ])
        accts=(largest or {}).get("value") or []
        amounts=[]
        for x in accts:
            try:amounts.append(float(x.get("amount") or 0))
            except:pass
        top1=(amounts[0]/supply_raw*100) if supply_raw>0 and amounts else 0
        top5=(sum(amounts[:5])/supply_raw*100) if supply_raw>0 else 0
        top10=(sum(amounts[:10])/supply_raw*100) if supply_raw>0 else 0

        token_programs={
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        }
        score=100.0;flags=[];hard=False
        if owner not in token_programs:
            score-=30;flags.append("unknown mint program");hard=True
        if mint_auth:
            score-=18;flags.append("mint authority active")
        if freeze_auth:
            score-=24;flags.append("freeze authority active");hard=True
        if top1>=35:
            score-=28;flags.append(f"top1 concentration {top1:.1f}%")
        elif top1>=20:
            score-=18;flags.append(f"top1 concentration {top1:.1f}%")
        elif top1>=10:
            score-=8;flags.append(f"top1 concentration {top1:.1f}%")
        if top5>=75:
            score-=22;flags.append(f"top5 concentration {top5:.1f}%")
        elif top5>=55:
            score-=12;flags.append(f"top5 concentration {top5:.1f}%")
        age=nz(c.get("age_minutes"),999999)
        if age<15:score-=12;flags.append("very new market")
        elif age<60:score-=6;flags.append("new market")
        liq=nz(c.get("liquidity"))
        if liq<25000:score-=14;flags.append("thin liquidity")
        elif liq<50000:score-=6;flags.append("modest liquidity")
        mc=max(nz(c.get("market_cap")),1)
        ratio=liq/mc
        if ratio<0.01:score-=12;flags.append("low liquidity/market-cap ratio")
        elif ratio<0.025:score-=6;flags.append("modest liquidity/market-cap ratio")

        score=round(clamp(score,0,100),1)
        status="PASS" if score>=75 and not hard else "WARN" if score>=45 and not hard else "BLOCK"
        result={
            "status":status,"score":score,"hard_block":hard,"flags":flags,
            "mint_authority_active":bool(mint_auth),"freeze_authority_active":bool(freeze_auth),
            "top1_pct":round(top1,2),"top5_pct":round(top5,2),"top10_pct":round(top10,2),
            "program_owner":owner,"source":"SOLANA_RPC",
            "note":"Largest-account concentration is approximate and can include LP, treasury or burn accounts."
        }
        runtime["token_security"][mint]={**result,"_ts":time.time()}
        return result
    except Exception as e:
        result={
            "status":"UNKNOWN","score":None,"hard_block":False,
            "flags":[f"security scan unavailable: {str(e)[:120]}"],
            "source":"SOLANA_RPC","note":"No contract-security conclusion was made."
        }
        runtime["token_security"][mint]={**result,"_ts":time.time()}
        return result

def security_coverage():
    spots=[c for c in runtime.get("candidates",[]) if not c.get("perp_eligible")]
    top=spots[:max(1,i("security_scan_top_n"))]
    known=[c for c in top if (c.get("security") or {}).get("status") not in (None,"UNKNOWN")]
    return {
        "scanned_known":len(known),"target":len(top),
        "coverage_pct":(len(known)/len(top)*100) if top else 100
    }

def source_health():
    age=iso_age_seconds(runtime.get("last_successful_loop"))
    spot_age=iso_age_seconds(runtime.get("last_spot_refresh"))
    perp_age=iso_age_seconds(runtime.get("last_perp_refresh"))
    max_age=f("max_data_age_sec")
    reasons=[]
    status="OK"
    if not runtime.get("loop_alive"):
        status="CRITICAL";reasons.append("engine loop not alive")
    if age>max_age:
        status="CRITICAL";reasons.append("market data stale")
    elif runtime.get("engine_error_streak",0)>0:
        status="WARN";reasons.append("recent engine errors")
    if not runtime.get("velocity_source_ok"):
        if status=="OK":status="WARN"
        reasons.append("primary perp source unavailable; fallback may be active")
    return {
        "status":status,"reasons":reasons,
        "loop_alive":runtime.get("loop_alive",False),
        "last_loop_age_sec":round(age,1),
        "spot_age_sec":round(spot_age,1),
        "perp_age_sec":round(perp_age,1),
        "velocity_source_ok":runtime.get("velocity_source_ok",False),
        "engine_error_streak":runtime.get("engine_error_streak",0)
    }

def clamp(v,a,b):
    return max(a,min(b,v))

def pick_num(d,*keys,default=0.0):
    if not isinstance(d,dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            try:return float(d[k])
            except:pass
    return default

def normalize_funding_rate(v):
    """Best-effort conversion to decimal hourly funding; absurd values are ignored."""
    x=nz(v,0)
    ax=abs(x)
    if ax>10000:x=x/1e9
    elif ax>1:x=x/1e6
    if abs(x)>0.05:return 0.0
    return x

def volatility_features(symbol,price):
    hist=runtime["price_history"].setdefault(symbol,[])
    hist.append(float(price))
    if len(hist)>80:del hist[:-80]
    if len(hist)<5:
        return {"volatility_pct":0.0,"regime":"WARMUP","momentum_pct":0.0}
    returns=[]
    for a,bp in zip(hist[:-1],hist[1:]):
        if a>0:returns.append((bp/a-1)*100)
    if not returns:
        vol=0.0
    else:
        mean=sum(returns)/len(returns)
        vol=(sum((x-mean)**2 for x in returns)/len(returns))**0.5
    window=hist[-20:] if len(hist)>=20 else hist
    mom=(window[-1]/window[0]-1)*100 if window[0]>0 else 0
    if vol>=1.8:regime="EXTREME"
    elif vol>=0.8:regime="HIGH"
    elif vol>=0.25:regime="NORMAL"
    else:regime="LOW"
    return {"volatility_pct":round(vol,4),"regime":regime,"momentum_pct":round(mom,3)}

def parse_velocity_market(m):
    if not isinstance(m,dict):return None
    market_type=str(m.get("marketType") or m.get("type") or "").lower()
    symbol=str(m.get("symbol") or m.get("market") or m.get("name") or "").upper()
    if "perp" not in market_type and "-PERP" not in symbol:return None
    if not symbol:return None

    price=pick_num(m,"price","markPrice","oraclePrice","lastPrice")
    oi=m.get("openInterest") or {}
    if isinstance(oi,dict):
        long_oi=pick_num(oi,"long","longOpenInterest","baseAssetAmountLong")
        short_oi=abs(pick_num(oi,"short","shortOpenInterest","baseAssetAmountShort"))
    else:
        total=abs(nz(oi))
        long_oi=short_oi=total/2
    if price<=0:
        return None

    long_usd=abs(long_oi)*price
    short_usd=abs(short_oi)*price
    total_oi_usd=long_usd+short_usd

    funding=normalize_funding_rate(
        m.get("fundingRate") if m.get("fundingRate") is not None
        else m.get("lastFundingRate") if m.get("lastFundingRate") is not None
        else m.get("funding")
    )
    status=str(m.get("status") or "active").lower()
    return {
        "symbol":symbol,"price":price,"funding_rate":funding,
        "oi_long_usd":long_usd,"oi_short_usd":short_usd,
        "open_interest_usd":total_oi_usd,"status":status,
        "raw":m
    }

async def fetch_velocity_markets(client):
    """Primary V4.2 perp source. Gracefully returns [] if Velocity is unavailable."""
    paths=["/stats/markets"]
    last=None
    for path in paths:
        try:
            r=await client.get(VELOCITY_DATA+path,timeout=20)
            r.raise_for_status()
            body=r.json()
            rows=body.get("markets") if isinstance(body,dict) else body
            if rows is None and isinstance(body,dict):
                rows=body.get("data") or body.get("result") or []
            parsed=[]
            for row in rows or []:
                p=parse_velocity_market(row)
                if p and p["status"] not in ("delisted","settlement"):
                    parsed.append(p)
            if parsed:
                runtime["velocity_source_ok"]=True
                runtime["last_velocity_refresh"]=datetime.now(timezone.utc).isoformat()
                runtime["velocity_markets"]={x["symbol"]:x for x in parsed}
                return parsed
        except Exception as e:
            last=e
    runtime["velocity_source_ok"]=False
    if last:runtime["last_error"]=f"velocity: {last}"
    return []

def perp_intelligence(v):
    symbol=v["symbol"]
    price=v["price"]
    vf=volatility_features(symbol,price)

    total=max(v["open_interest_usd"],1)
    long_share=clamp(v["oi_long_usd"]/total,0,1)
    short_share=clamp(v["oi_short_usd"]/total,0,1)
    funding=v["funding_rate"]

    prev=runtime["prev_oi"].get(symbol,total)
    oi_change=(total/max(prev,1)-1)*100
    runtime["prev_oi"][symbol]=total

    momentum=vf["momentum_pct"]
    mom_score=clamp(50+momentum*8,0,100)
    inv_mom=100-mom_score

    # Positive funding means longs pay shorts; negative funding means shorts pay longs.
    funding_bps=funding*10000
    funding_long_bias=clamp(50-funding_bps*4,20,80)
    funding_short_bias=clamp(50+funding_bps*4,20,80)

    oi_growth_score=clamp(50+oi_change*4,0,100)
    quality=clamp(35+18*math.log10(max(total,1)/100000),20,100)

    long_score=round(
        mom_score*.37 + funding_long_bias*.16 + (short_share*100)*.08 +
        oi_growth_score*.16 + quality*.16 + 50*.07
    )
    short_score=round(
        inv_mom*.37 + funding_short_bias*.16 + (long_share*100)*.08 +
        oi_growth_score*.16 + quality*.16 + 50*.07
    )

    if vf["regime"]=="EXTREME":
        long_score-=6;short_score-=6

    edge=abs(long_score-short_score)
    if edge<4:direction="WAIT"
    else:direction="LONG" if long_score>short_score else "SHORT"

    reasons=[]
    if abs(momentum)>=0.2:reasons.append(("momentum_up" if momentum>0 else "momentum_down"))
    if abs(funding_bps)>=0.25:reasons.append(("longs_pay_funding" if funding>0 else "shorts_pay_funding"))
    if abs(long_share-short_share)>=0.10:reasons.append(("long_oi_crowded" if long_share>short_share else "short_oi_crowded"))
    if oi_change>=2:reasons.append("oi_expanding")
    if vf["regime"] in ("HIGH","EXTREME"):reasons.append("high_volatility")

    return {
        "mint":"velocity:"+symbol,
        "symbol":symbol.replace("-PERP",""),
        "name":symbol,
        "price":price,
        "dex":"velocity",
        "url":"https://velocity.exchange",
        "perp_eligible":True,
        "perp_market":symbol,
        "data_source":"VELOCITY",
        "funding_rate":funding,
        "funding_bps":round(funding_bps,4),
        "oi_long_usd":round(v["oi_long_usd"],2),
        "oi_short_usd":round(v["oi_short_usd"],2),
        "open_interest_usd":round(total,2),
        "oi_change_pct":round(oi_change,3),
        "volatility_pct":vf["volatility_pct"],
        "volatility_regime":vf["regime"],
        "momentum_pct":vf["momentum_pct"],
        "long_score":clamp(long_score,0,100),
        "short_score":clamp(short_score,0,100),
        "direction":direction,
        "direction_edge":round(edge,1),
        "reasons":reasons,
        # compatibility fields
        "pump_score":0,"scalp_score":0,
        "market_risk":round(quality),
        "buy_pressure":round(long_share*100,1),
        "volume_accel":round(oi_growth_score,1),
        "m5":round(momentum,3),"h1":0.0,
        "liquidity":0.0,"market_cap":0.0,"age_minutes":999999,
        "volume_m5":0.0,"buys_m5":0,"sells_m5":0,
    }

def score_pair(p, source_boost=0):
    tx5 = (p.get("txns") or {}).get("m5") or {}
    buys5, sells5 = nz(tx5.get("buys")), nz(tx5.get("sells"))
    total5 = buys5 + sells5
    pressure = 50 if total5 == 0 else 100 * buys5 / total5

    vol = p.get("volume") or {}
    v5, v1 = nz(vol.get("m5")), nz(vol.get("h1"))
    expected5 = max(v1 / 12.0, 1.0)
    vol_accel = max(0, min(100, 35 + 20 * math.log10(max(v5 / expected5, 0.05))))

    pc = p.get("priceChange") or {}
    m5, h1, h6, h24 = nz(pc.get("m5")), nz(pc.get("h1")), nz(pc.get("h6")), nz(pc.get("h24"))
    trend_alignment=(1 if m5>0 else -1)+(1 if h1>0 else -1)+(1 if h6>0 else -1)+(1 if h24>0 else -1)
    trend_score=clamp(50+trend_alignment*10+h1*.35+h6*.12+h24*.04,0,100)
    bull_momentum = max(0, min(100, 50 + m5 * 5.0 + h1 * 0.55 + h6*.08))
    bear_momentum = max(0, min(100, 50 - m5 * 5.0 - h1 * 0.55 - h6*.08))

    liq = nz((p.get("liquidity") or {}).get("usd"))
    liquidity_score = max(0, min(100, 20 + 22 * math.log10(max(liq, 1) / 1000)))

    created = nz(p.get("pairCreatedAt"))
    age_min = (time.time()*1000 - created)/60000 if created else 999999
    age_score = 100 if 10 <= age_min <= 1440 else 80 if 5 <= age_min <= 4320 else 55 if age_min <= 10080 else 35

    fdv = nz(p.get("marketCap") or p.get("fdv"))
    ratio = liq / max(fdv, 1)
    ratio_score = max(0, min(100, ratio * 700))

    boost = min(100, source_boost)

    # Sniper score: early acceleration without chasing an already vertical move.
    if 0.30 <= m5 <= 6:
        sniper_momentum = 100
    elif 6 < m5 <= 10:
        sniper_momentum = 82
    elif 10 < m5 <= 12:
        sniper_momentum = 65
    elif 0 < m5 < 0.30:
        sniper_momentum = 45
    else:
        sniper_momentum = 10

    if age_min <= 30:
        sniper_age = 100
    elif age_min <= 120:
        sniper_age = 90
    elif age_min <= 360:
        sniper_age = 70
    elif age_min <= 1440:
        sniper_age = 35
    else:
        sniper_age = 10

    tx_activity = clamp(25 + 18 * math.log10(max(total5,1)),0,100)

    sniper = round(
        pressure*.21 + vol_accel*.24 + sniper_momentum*.18 +
        liquidity_score*.14 + tx_activity*.10 + ratio_score*.07 +
        sniper_age*.04 + boost*.02
    )

    pump = round(
        vol_accel * .22 + pressure * .22 + bull_momentum * .20 +
        liquidity_score * .14 + ratio_score * .12 + age_score * .07 + boost * .03
    )
    scalp = round(
        pressure * .24 + max(0,min(100,50+m5*6))*.25 +
        vol_accel*.18 + liquidity_score*.20 + ratio_score*.13
    )

    # Directional scores are used by the perp paper engine.
    long_score = round(
        pressure * .25 + bull_momentum * .30 + vol_accel * .18 +
        liquidity_score * .17 + age_score * .10
    )
    short_score = round(
        (100-pressure) * .25 + bear_momentum * .30 + vol_accel * .18 +
        liquidity_score * .17 + age_score * .10
    )

    # "Market Risk" is a market-quality gate, not a token security audit.
    market_risk = round(
        liquidity_score*.35 + ratio_score*.25 + age_score*.20 +
        max(0,min(100, 70 - abs(m5)*1.4))*.20
    )
    return {
        "sniper_score": max(0,min(100,sniper)),
        "pump_score": max(0,min(100,pump)),
        "scalp_score": max(0,min(100,scalp)),
        "long_score": max(0,min(100,long_score)),
        "short_score": max(0,min(100,short_score)),
        "market_risk": max(0,min(100,market_risk)),
        "buy_pressure": round(pressure,1),
        "volume_accel": round(vol_accel,1),
        "trend_score": round(trend_score,1),
        "trend_alignment": trend_alignment,
        "m5": m5, "h1": h1, "h6": h6, "h24": h24, "liquidity": liq, "market_cap": fdv,
        "age_minutes": round(age_min,1), "volume_m5": v5,
        "buys_m5": int(buys5), "sells_m5": int(sells5),
    }

async def discover(client):
    addresses, boosts = [], {}
    endpoints = [
        ("/token-boosts/top/v1", True),
        ("/token-boosts/latest/v1", True),
        ("/token-profiles/latest/v1", False),
    ]
    for ep, boosted in endpoints:
        try:
            r = await client.get(DEX + ep, timeout=15)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict): data=[data]
            for x in data or []:
                if x.get("chainId") != "solana" or not x.get("tokenAddress"): continue
                a=x["tokenAddress"]
                addresses.append(a)
                if boosted:
                    boosts[a] = max(boosts.get(a,0), nz(x.get("amount")) + nz(x.get("totalAmount"))*.15)
        except Exception as e:
            runtime["last_error"] = f"discovery {ep}: {e}"
    manual=[x.strip() for x in getv("watchlist","").split(",") if x.strip()]
    addresses.extend(manual)
    # preserve order / unique
    return list(dict.fromkeys(addresses))[:90], boosts

async def fetch_pairs(client, addresses, boosts):
    pairs=[]
    for k in range(0,len(addresses),30):
        batch=addresses[k:k+30]
        if not batch: continue
        try:
            url=DEX+"/tokens/v1/solana/"+",".join(batch)
            r=await client.get(url,timeout=20); r.raise_for_status()
            raw=r.json() or []
            # choose highest-liquidity pair per base token
            best={}
            for p in raw:
                mint=(p.get("baseToken") or {}).get("address")
                if not mint: continue
                if mint not in best or nz((p.get("liquidity") or {}).get("usd")) > nz((best[mint].get("liquidity") or {}).get("usd")):
                    best[mint]=p
            for mint,p in best.items():
                price=nz(p.get("priceUsd"))
                if price<=0: continue
                sc=score_pair(p, boosts.get(mint,0))
                pairs.append({
                    "mint":mint,
                    "symbol":(p.get("baseToken") or {}).get("symbol","?"),
                    "name":(p.get("baseToken") or {}).get("name","Unknown"),
                    "price":price,
                    "dex":p.get("dexId",""),
                    "url":p.get("url",""),
                    "perp_eligible": False,
                    "perp_market": None,
                    "data_source":"DEXSCREENER",
                    "funding_rate":0.0,"funding_bps":0.0,
                    "oi_long_usd":0.0,"oi_short_usd":0.0,"open_interest_usd":0.0,"oi_change_pct":0.0,
                    "volatility_pct":0.0,"volatility_regime":"SPOT","momentum_pct":sc.get("m5",0.0),
                    "direction":"LONG" if max(sc.get("pump_score",0),sc.get("scalp_score",0))>=f("min_pump_score") else "WAIT",
                    "direction_edge":0.0,"reasons":[],
                    **sc
                })
        except Exception as e:
            runtime["last_error"]=f"pairs: {e}"
    pairs.sort(key=lambda x:max(x["pump_score"],x["scalp_score"]),reverse=True)
    return pairs[:60]

async def fetch_legacy_perp_pairs(client):
    """Fetch public Solana DEX proxies for markets eligible for paper long/short."""
    out=[]
    for mint,spec in PERP_UNIVERSE.items():
        try:
            r=await client.get(DEX+"/tokens/v1/solana/"+mint, timeout=20)
            r.raise_for_status()
            raw=r.json() or []
            matches=[
                p for p in raw
                if (p.get("baseToken") or {}).get("address")==mint and nz(p.get("priceUsd"))>0
            ]
            if not matches:
                continue
            p=max(matches, key=lambda x:nz((x.get("liquidity") or {}).get("usd")))
            sc=score_pair(p,0)
            out.append({
                "mint":mint,
                "symbol":spec["symbol"],
                "name":spec["market"],
                "price":nz(p.get("priceUsd")),
                "dex":p.get("dexId",""),
                "url":p.get("url",""),
                "perp_eligible":True,
                "perp_market":spec["market"],
                "data_source":"DEX_FALLBACK",
                "funding_rate":0.0,"funding_bps":0.0,
                "oi_long_usd":0.0,"oi_short_usd":0.0,"open_interest_usd":0.0,"oi_change_pct":0.0,
                "volatility_pct":0.0,"volatility_regime":"FALLBACK","momentum_pct":sc.get("m5",0.0),
                "direction":"LONG" if sc.get("long_score",0)>=sc.get("short_score",0) else "SHORT",
                "direction_edge":abs(sc.get("long_score",0)-sc.get("short_score",0)),
                "reasons":["velocity_fallback"],
                **sc
            })
        except Exception as e:
            runtime["last_error"]=f"perp {spec['market']}: {e}"
    return out

def strategy_side(strategy):
    return "SHORT" if str(strategy).endswith("_SHORT") else "LONG"

def strategy_leverage(strategy):
    if not str(strategy).startswith("PERP_"):
        return 1.0
    return max(1.0,min(2.0,f("perp_leverage")))

def directional_raw_return(entry, price, side):
    raw=(price/max(entry,1e-12))-1.0
    return -raw if side=="SHORT" else raw

def directional_return_pct(p, price):
    return directional_raw_return(p.entry_price, price, strategy_side(p.strategy))*strategy_leverage(p.strategy)*100

def paper_position_value(p, price):
    raw=directional_raw_return(p.entry_price, price, strategy_side(p.strategy))
    value=p.remaining_cost*(1 + raw*strategy_leverage(p.strategy))
    return max(0.0,value)

def positions_with_marks():
    cands={x["mint"]:x for x in runtime["candidates"]}
    out=[]
    with SessionLocal() as s:
        for p in s.scalars(select(Position)).all():
            c=cands.get(p.mint)
            price=c["price"] if c else p.last_price
            side=strategy_side(p.strategy)
            leverage=strategy_leverage(p.strategy)
            ret=directional_return_pct(p,price)
            value=paper_position_value(p,price)
            mark_execution_bps=0.0
            if b("execution_simulator_enabled") and c and p.remaining_cost>0:
                execution_notional=p.remaining_cost*leverage
                est=execution_cost_estimate(c,p.strategy,execution_notional)
                fill=simulated_fill_price(price,side,"MARK_EXIT",est.get("adverse_bps",0))
                raw=directional_raw_return(p.entry_price,fill,side)
                gross=max(0.0,p.remaining_cost*(1+raw*leverage))
                fee=execution_notional*est.get("fee_bps",0)/10000.0
                value=max(0.0,gross-fee)
                ret=((value/max(p.remaining_cost,1e-9))-1)*100
                mark_execution_bps=est.get("all_in_bps",0)
            if c:
                current_net=expected_exit_total_pct(p,c)
                peak_c=dict(c);peak_c["price"]=p.peak_price
                peak_net=expected_exit_total_pct(p,peak_c)
                lock_floor=strategy_profit_lock(p.strategy,peak_net) if b("profit_lock_enabled") else None
            else:
                current_net=ret;peak_net=ret;lock_floor=None
            out.append({
                "id":p.id,"mint":p.mint,"symbol":p.symbol,"name":p.name,
                "strategy":p.strategy,"side":side,
                "leverage":leverage,
                "entry":p.entry_price,"price":price,
                "return_pct":current_net,"remaining_cost":p.remaining_cost,
                "market_value":value,"locked_pnl":p.locked_pnl,
                "peak_return_pct":peak_net,"profit_lock_floor_pct":lock_floor,
                "profit_lock_armed":lock_floor is not None,
                "mark_execution_bps":mark_execution_bps,
                "opened_at":p.opened_at.isoformat()
            })
    return out

def record_equity_snapshot(force=False):
    now=time.time()
    if not force and now-runtime["last_equity_snapshot"]<300:return
    positions=positions_with_marks()
    equity=f("cash")+sum(x["market_value"] for x in positions)
    realized=f("cash")-f("start_balance")
    with SessionLocal() as s:
        prior=s.scalars(select(EquityPoint).order_by(EquityPoint.id.desc()).limit(5000)).all()
        peak=max([x.equity for x in prior],default=f("start_balance"))
        peak=max(peak,equity)
        dd=(equity/peak-1)*100 if peak>0 else 0
        s.add(EquityPoint(equity=equity,realized_pnl=realized,drawdown_pct=dd,created_at=datetime.now(timezone.utc)))
        s.commit()
    runtime["last_equity_snapshot"]=now

def equity_curve(limit=120):
    with SessionLocal() as s:
        pts=s.scalars(select(EquityPoint).order_by(EquityPoint.id.desc()).limit(limit)).all()
    pts=list(reversed(pts))
    return [{"equity":x.equity,"drawdown_pct":x.drawdown_pct,"ts":x.created_at.isoformat()} for x in pts]

def metrics():
    positions=positions_with_marks()
    cash=f("cash")
    equity=cash+sum(x["market_value"] for x in positions)
    with SessionLocal() as s:
        trades=s.scalars(select(Trade).order_by(Trade.id.desc())).all()
    wins=[t for t in trades if t.pnl>=0]
    losses=[t for t in trades if t.pnl<0]
    gross_win=sum(t.pnl for t in wins)
    gross_loss=abs(sum(t.pnl for t in losses))
    profit_factor=(gross_win/gross_loss) if gross_loss>0 else (999 if gross_win>0 else 0)
    winrate=(len(wins)/len(trades)*100) if trades else 0
    start=f("start_balance")
    pnl=equity-start
    long_trades=[t for t in trades if strategy_side(t.strategy)=="LONG"]
    short_trades=[t for t in trades if strategy_side(t.strategy)=="SHORT"]
    def side_stats(items):
        sw=[t for t in items if t.pnl>=0]
        return {
            "trades":len(items),
            "wins":len(sw),
            "win_rate":(len(sw)/len(items)*100) if items else 0,
            "pnl":sum(t.pnl for t in items)
        }
    avg_win=(sum(t.pnl for t in wins)/len(wins)) if wins else 0
    avg_loss=(sum(t.pnl for t in losses)/len(losses)) if losses else 0
    expectancy=(sum(t.pnl for t in trades)/len(trades)) if trades else 0
    payoff=(avg_win/abs(avg_loss)) if avg_loss<0 else (999 if avg_win>0 else 0)
    curve=equity_curve(500)
    max_dd=min([x["drawdown_pct"] for x in curve],default=0)
    current_dd=curve[-1]["drawdown_pct"] if curve else 0
    strategy_names=sorted(set(t.strategy for t in trades))
    strategy_stats={}
    for name in strategy_names:
        items=[t for t in trades if t.strategy==name]
        sw=[t for t in items if t.pnl>=0]
        strategy_stats[name]={
            "trades":len(items),"pnl":sum(t.pnl for t in items),
            "win_rate":len(sw)/len(items)*100 if items else 0,
            "expectancy":sum(t.pnl for t in items)/len(items) if items else 0
        }
    return {
        "cash":cash,"equity":equity,"pnl":pnl,"pnl_pct":pnl/start*100 if start else 0,
        "trades":len(trades),"wins":len(wins),"losses":len(losses),
        "win_rate":winrate,"profit_factor":profit_factor,
        "avg_win":avg_win,"avg_loss":avg_loss,"expectancy":expectancy,"payoff_ratio":payoff,
        "max_drawdown_pct":max_dd,"current_drawdown_pct":current_dd,
        "open_positions":len(positions),
        "long":side_stats(long_trades),"short":side_stats(short_trades),
        "strategies":strategy_stats
    }

def today_realized():
    today=datetime.now(timezone.utc).date()
    with SessionLocal() as s:
        ts=s.scalars(select(Trade)).all()
    return sum(t.pnl for t in ts if (t.closed_at.replace(tzinfo=timezone.utc) if t.closed_at.tzinfo is None else t.closed_at).date()==today)

def consecutive_losses():
    with SessionLocal() as s:
        ts=s.scalars(select(Trade).order_by(Trade.id.desc()).limit(20)).all()
    n=0
    for t in ts:
        if t.pnl<0:n+=1
        else:break
    return n


def record_market_snapshots(candidates, force=False):
    now=time.time()
    if not force and now-runtime["last_market_snapshot"]<i("snapshot_interval_sec"):
        return
    ts=datetime.now(timezone.utc)
    rows=[]
    for c in (candidates or [])[:40]:
        price=nz(c.get("price"))
        if price<=0:continue
        rows.append(MarketSnapshot(
            mint=str(c.get("mint","")),symbol=str(c.get("symbol","?")),
            data_source=str(c.get("data_source","UNKNOWN")),
            perp_eligible=bool(c.get("perp_eligible")),
            price=price,pump_score=nz(c.get("pump_score")),scalp_score=nz(c.get("scalp_score")),
            long_score=nz(c.get("long_score")),short_score=nz(c.get("short_score")),
            market_risk=nz(c.get("market_risk")),direction=str(c.get("direction") or "WAIT"),
            direction_edge=nz(c.get("direction_edge")),funding_rate=nz(c.get("funding_rate")),
            open_interest_usd=nz(c.get("open_interest_usd")),
            volatility_regime=str(c.get("volatility_regime") or "UNKNOWN"),
            liquidity=nz(c.get("liquidity")),created_at=ts
        ))
    if rows:
        with SessionLocal() as s:
            s.add_all(rows);s.commit()
    runtime["last_market_snapshot"]=now

    if now-runtime["last_snapshot_cleanup"]>3600:
        cutoff=datetime.now(timezone.utc)-timedelta(days=i("snapshot_retention_days"))
        with SessionLocal() as s:
            s.query(MarketSnapshot).filter(MarketSnapshot.created_at<cutoff).delete(synchronize_session=False)
            s.commit()
        runtime["last_snapshot_cleanup"]=now

def snapshot_stats():
    with SessionLocal() as s:
        count=s.scalar(select(func.count()).select_from(MarketSnapshot)) or 0
        first=s.scalar(select(func.min(MarketSnapshot.created_at)))
        last=s.scalar(select(func.max(MarketSnapshot.created_at)))
        markets=s.scalar(select(func.count(func.distinct(MarketSnapshot.mint)))) or 0
    hours=0
    if first and last:
        hours=max(0,(last-first).total_seconds()/3600)
    return {
        "snapshots":int(count),"markets":int(markets),
        "first":first.isoformat() if first else None,
        "last":last.isoformat() if last else None,
        "coverage_hours":round(hours,2),
        "replay_ready":count>=300 and hours>=0.5,
        "walk_forward_ready":count>=1200 and hours>=2.0
    }

def load_research_snapshots(days=None):
    days=days or i("research_days")
    cutoff=datetime.now(timezone.utc)-timedelta(days=days)
    with SessionLocal() as s:
        rows=s.scalars(
            select(MarketSnapshot)
            .where(MarketSnapshot.created_at>=cutoff)
            .order_by(MarketSnapshot.created_at.asc())
            .limit(100000)
        ).all()
    return rows

def snapshot_score(s,strategy):
    if strategy=="PUMP_LONG":return s.pump_score
    if strategy=="SCALP_LONG":return s.scalp_score
    if strategy=="PERP_LONG":return s.long_score
    if strategy=="PERP_SHORT":return s.short_score
    return 0

def snapshot_signal_ok(s,strategy,threshold):
    if s.market_risk < f("min_market_risk"):return False
    if strategy=="PUMP_LONG":
        return (not s.perp_eligible) and s.pump_score>=threshold and s.liquidity>=f("min_liquidity")
    if strategy=="SCALP_LONG":
        return (not s.perp_eligible) and s.scalp_score>=threshold and s.liquidity>=f("min_liquidity")
    if strategy=="PERP_LONG":
        return s.perp_eligible and s.long_score>=threshold and s.direction=="LONG" and s.direction_edge>=f("min_direction_edge") and s.volatility_regime!="EXTREME"
    if strategy=="PERP_SHORT":
        return s.perp_eligible and s.short_score>=threshold and s.direction=="SHORT" and s.direction_edge>=f("min_direction_edge") and s.volatility_regime!="EXTREME"
    return False

def replay_metrics(trades):
    if not trades:
        return {"trades":0,"win_rate":0,"profit_factor":0,"expectancy_pct":0,"net_return_pct":0,
                "avg_win_pct":0,"avg_loss_pct":0,"max_drawdown_pct":0}
    wins=[x for x in trades if x["ret_pct"]>=0]
    losses=[x for x in trades if x["ret_pct"]<0]
    gw=sum(x["ret_pct"] for x in wins);gl=abs(sum(x["ret_pct"] for x in losses))
    pf=gw/gl if gl>0 else (999 if gw>0 else 0)
    equity=100.0;peak=100.0;maxdd=0
    for t in trades:
        equity*=max(0.01,1+t["ret_pct"]/100)
        peak=max(peak,equity)
        dd=(equity/peak-1)*100
        maxdd=min(maxdd,dd)
    return {
        "trades":len(trades),
        "win_rate":len(wins)/len(trades)*100,
        "profit_factor":pf,
        "expectancy_pct":sum(x["ret_pct"] for x in trades)/len(trades),
        "net_return_pct":equity-100,
        "avg_win_pct":sum(x["ret_pct"] for x in wins)/len(wins) if wins else 0,
        "avg_loss_pct":sum(x["ret_pct"] for x in losses)/len(losses) if losses else 0,
        "max_drawdown_pct":maxdd
    }

def replay_strategy(rows,strategy,threshold,hold_minutes=None):
    hold_minutes=hold_minutes or i("replay_hold_minutes")
    stop=f("stop_loss_pct")
    take=f("replay_take_profit_pct")
    leverage=f("perp_leverage") if strategy.startswith("PERP_") else 1.0
    side="SHORT" if strategy=="PERP_SHORT" else "LONG"

    grouped={}
    for s in rows:
        grouped.setdefault(s.mint,[]).append(s)

    trades=[]
    for mint,series in grouped.items():
        series.sort(key=lambda x:x.created_at)
        j=0
        while j<len(series)-1:
            entry=series[j]
            if not snapshot_signal_ok(entry,strategy,threshold):
                j+=1;continue
            entry_price=entry.price
            deadline=entry.created_at+timedelta(minutes=hold_minutes)
            exit_s=None;reason="TIME"
            k=j+1
            while k<len(series) and series[k].created_at<=deadline:
                cur=series[k]
                raw=(cur.price/entry_price-1)*100
                d=(raw if side=="LONG" else -raw)*leverage
                if d<=-stop:
                    exit_s=cur;reason="STOP";break
                if d>=take:
                    exit_s=cur;reason="TAKE";break
                exit_s=cur;k+=1
            if exit_s is None:
                j+=1;continue
            raw=(exit_s.price/entry_price-1)*100
            gross=(raw if side=="LONG" else -raw)*leverage
            # Reconstruct a conservative round-trip cost estimate from the recorded snapshot.
            proxy={
                "liquidity":entry.liquidity,"open_interest_usd":entry.open_interest_usd,
                "volatility_regime":entry.volatility_regime,"volatility_pct":0,
                "m5":0
            }
            assumed_notional=max(50.0,f("start_balance")*f("max_position_pct")/100*leverage)
            one_way=execution_cost_estimate(proxy,strategy,assumed_notional)["all_in_bps"]/100.0
            ret=gross-one_way*2
            trades.append({
                "mint":mint,"symbol":entry.symbol,"strategy":strategy,
                "entry_score":snapshot_score(entry,strategy),
                "entry_at":entry.created_at.isoformat(),"exit_at":exit_s.created_at.isoformat(),
                "ret_pct":ret,"reason":reason
            })
            # skip overlapping signal window for the same market
            while j<len(series) and series[j].created_at<=exit_s.created_at:
                j+=1
    return {"strategy":strategy,"threshold":threshold,"hold_minutes":hold_minutes,
            "metrics":replay_metrics(trades),"sample":trades[-20:]}

def walk_forward(rows,strategy):
    if len(rows)<1200:
        return {"ready":False,"reason":"need more recorded market snapshots","samples":len(rows)}
    times=sorted(set(x.created_at for x in rows))
    if len(times)<60:
        return {"ready":False,"reason":"need more time coverage","samples":len(rows)}
    split_time=times[int(len(times)*0.70)]
    train=[x for x in rows if x.created_at<=split_time]
    test=[x for x in rows if x.created_at>split_time]
    base=int(round(base_threshold(strategy)))
    best=None
    for th in range(max(60,base-6),min(92,base+7),2):
        r=replay_strategy(train,strategy,th)
        m=r["metrics"]
        if m["trades"]<5:continue
        objective=m["expectancy_pct"] + min(m["profit_factor"],3)*0.15 - abs(m["max_drawdown_pct"])*0.02
        if best is None or objective>best[0]:
            best=(objective,th,m)
    if best is None:
        return {"ready":False,"reason":"not enough train trades","samples":len(rows)}
    _,th,train_m=best
    test_r=replay_strategy(test,strategy,th)
    return {
        "ready":True,"split":split_time.isoformat(),"selected_threshold":th,
        "train":train_m,"test":test_r["metrics"],
        "robust":test_r["metrics"]["trades"]>=3 and test_r["metrics"]["expectancy_pct"]>0 and test_r["metrics"]["profit_factor"]>=1.0
    }

def monte_carlo():
    with SessionLocal() as s:
        trades=s.scalars(select(Trade).order_by(Trade.id.asc()).limit(1000)).all()
    if len(trades)<10:
        return {"ready":False,"trades":len(trades),"reason":"need at least 10 closed paper trades"}
    pnls=[t.pnl for t in trades]
    runs=max(100,min(5000,i("monte_carlo_runs")))
    start=f("start_balance")
    finals=[];dds=[]
    ruin=0
    random.seed(4403)
    for _ in range(runs):
        eq=start;peak=start;maxdd=0
        for _n in range(len(pnls)):
            eq+=random.choice(pnls)
            peak=max(peak,eq)
            dd=(eq/peak-1)*100 if peak>0 else -100
            maxdd=min(maxdd,dd)
        finals.append(eq-start);dds.append(maxdd)
        if eq<=start*.80:ruin+=1
    finals.sort();dds.sort()
    def pct(arr,p):
        if not arr:return 0
        return arr[min(len(arr)-1,max(0,int((len(arr)-1)*p)))]
    return {
        "ready":True,"runs":runs,"trades_per_run":len(pnls),
        "final_pnl_p10":pct(finals,.10),"final_pnl_p50":pct(finals,.50),"final_pnl_p90":pct(finals,.90),
        "max_dd_p50":pct(dds,.50),"max_dd_p10":pct(dds,.10),
        "prob_finish_below_start":sum(1 for x in finals if x<0)/runs*100,
        "prob_20pct_capital_loss":ruin/runs*100
    }

def research_report():
    rows=load_research_snapshots()
    strategies=["LAUNCH_SNIPER","SNIPER_LONG","PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
    replays={}
    walks={}
    for s in strategies:
        replays[s]=replay_strategy(rows,s,base_threshold(s))
        walks[s]=walk_forward(rows,s)
    report={
        "version":APP_VERSION,"data":snapshot_stats(),
        "replay":replays,"walk_forward":walks,"monte_carlo":monte_carlo(),
        "note":"Replay uses snapshots recorded by NOVA after V4.4 deployment; it is not tick-level exchange backtesting."
    }
    runtime["last_research_report"]=report
    return report




def suggested_strategy(c):
    if c.get("perp_eligible"):
        return "PERP_SHORT" if c.get("direction")=="SHORT" else "PERP_LONG"
    if b("sniper_enabled") and nz(c.get("sniper_score")) >= f("min_sniper_score"):
        return "SNIPER_LONG"
    return "PUMP_LONG" if nz(c.get("pump_score"))>=nz(c.get("scalp_score")) else "SCALP_LONG"

def route_quality(c,strategy,notional_usd=None):
    if notional_usd:
        notional=nz(notional_usd)
    else:
        leverage=strategy_leverage(strategy)
        notional=max(25.0,f("start_balance")*f("max_position_pct")/100*leverage*.65)
    source=str(c.get("data_source") or "UNKNOWN")
    source_q={"VELOCITY":95,"DEXSCREENER":88,"DEX_FALLBACK":55}.get(source,55)
    if c.get("perp_eligible"):
        age=iso_age_seconds(runtime.get("last_perp_refresh"))
    else:
        age=iso_age_seconds(runtime.get("last_spot_refresh"))
    freshness=clamp(100-age/max(f("max_data_age_sec"),1)*70,0,100)
    sec=(c.get("security") or {})
    if c.get("perp_eligible"):
        security_q=75.0
    elif sec.get("status")=="UNKNOWN" or sec.get("score") is None:
        security_q=70.0
    else:
        security_q=nz(sec.get("score"),70)
    est=execution_cost_estimate(c,strategy,notional)
    max_cost=max(f("max_execution_cost_bps"),1)
    execution_q=clamp(100-est.get("all_in_bps",0)/max_cost*70,0,100)
    q=source_q*.30+freshness*.20+execution_q*.25+security_q*.25
    venue="VELOCITY-PERP-SHADOW" if source=="VELOCITY" else "DEXSCREENER-SPOT-SHADOW" if not c.get("perp_eligible") else "PERP-FALLBACK-SHADOW"
    return {
        "venue":venue,"quality":round(clamp(q,0,100),1),"source_quality":source_q,
        "freshness_quality":round(freshness,1),"execution_quality":round(execution_q,1),
        "security_quality":round(security_q,1),"execution":est
    }

def route_gate(c,strategy):
    route=route_quality(c,strategy)
    if operating_mode()=="SHADOW" and c.get("perp_eligible") and c.get("data_source")!="VELOCITY":
        return False,"shadow requires primary perp source",route
    if route["quality"]<f("min_route_quality"):
        return False,"route quality too low",route
    return True,"ok",route

def signal_quality(c,strategy,signal_score):
    pw=portfolio_weight(strategy)
    route=route_quality(c,strategy)
    sec=(c.get("security") or {})
    if c.get("perp_eligible"):
        sq=100.0
    elif sec.get("status")=="UNKNOWN" or sec.get("score") is None:
        sq=70.0
    else:
        sq=nz(sec.get("score"),70)
    market=nz(c.get("market_risk"),50)
    quality=nz(signal_score)
    quality*=0.58+0.42*clamp(pw,0,1)
    quality*=0.70+0.30*route["quality"]/100
    quality*=0.80+0.20*clamp(sq,0,100)/100
    quality*=0.85+0.15*clamp(market,0,100)/100
    return round(clamp(quality,0,100),2),route

def token_security_status():
    spots=[c for c in runtime.get("candidates",[]) if not c.get("perp_eligible")]
    rows=[]
    for c in spots[:20]:
        sec=c.get("security") or {"status":"UNKNOWN","score":None,"flags":[]}
        rows.append({
            "mint":c.get("mint"),"symbol":c.get("symbol"),"status":sec.get("status"),
            "score":sec.get("score"),"hard_block":sec.get("hard_block",False),
            "top1_pct":sec.get("top1_pct"),"top5_pct":sec.get("top5_pct"),
            "mint_authority_active":sec.get("mint_authority_active"),
            "freeze_authority_active":sec.get("freeze_authority_active"),
            "flags":sec.get("flags",[]),"source":sec.get("source")
        })
    return {"coverage":security_coverage(),"tokens":rows}

def system_events(limit=40):
    with SessionLocal() as s:
        rows=s.scalars(select(SystemEvent).order_by(SystemEvent.id.desc()).limit(limit)).all()
    return [{
        "level":x.level,"code":x.code,"message":x.message,
        "detail":x.detail,"created_at":x.created_at.isoformat()
    } for x in rows]

def recent_decisions(limit=40):
    with SessionLocal() as s:
        rows=s.scalars(select(DecisionLog).order_by(DecisionLog.id.desc()).limit(limit)).all()
    return [{
        "symbol":x.symbol,"strategy":x.strategy,"mode":x.operating_mode,
        "outcome":x.outcome,"reason":x.reason,"signal_score":x.signal_score,
        "quality_score":x.quality_score,"route_quality":x.route_quality,
        "security_score":x.security_score,"created_at":x.created_at.isoformat()
    } for x in rows]

def shadow_hours():
    if operating_mode()!="SHADOW":return 0.0
    v=getv("shadow_started_at","")
    if not v:return 0.0
    return iso_age_seconds(v)/3600.0

def live_readiness():
    m=metrics()
    ex=execution_stats(300)
    health=source_health()
    sec=security_coverage()
    rr=runtime.get("last_research_report")
    robust=0;mc={}
    if rr:
        robust=sum(1 for x in (rr.get("walk_forward") or {}).values() if x.get("ready") and x.get("robust"))
        mc=rr.get("monte_carlo") or {}
    checks=[]
    def add(name,passed,value,target,critical=True):
        checks.append({"name":name,"pass":bool(passed),"value":value,"target":target,"critical":critical})
    add("closed paper trades",m["trades"]>=i("readiness_min_trades"),m["trades"],f">= {i('readiness_min_trades')}")
    add("profit factor",m["profit_factor"]>=f("readiness_min_pf"),round(m["profit_factor"],2),f">= {f('readiness_min_pf')}")
    add("positive expectancy",m["expectancy"]>0,round(m["expectancy"],3),"> 0")
    add("max drawdown",abs(m["max_drawdown_pct"])<=f("readiness_max_drawdown_pct"),round(m["max_drawdown_pct"],2),f"<= {f('readiness_max_drawdown_pct')}%")
    add("execution simulator",b("execution_simulator_enabled"),b("execution_simulator_enabled"),True)
    add("execution cost",ex["fills"]>=20 and ex["avg_all_in_bps"]<=f("readiness_max_exec_bps"),
        round(ex["avg_all_in_bps"],1),f"<= {f('readiness_max_exec_bps')} bp after >=20 fills")
    add("market data health",health["status"]=="OK",health["status"],"OK")
    add("security coverage",sec["coverage_pct"]>=60,round(sec["coverage_pct"],1),">= 60%",False)
    add("shadow observation",shadow_hours()>=f("readiness_min_shadow_hours"),round(shadow_hours(),2),f">= {f('readiness_min_shadow_hours')}h")
    add("walk-forward robust strategies",robust>=i("readiness_min_wf_robust"),robust,f">= {i('readiness_min_wf_robust')}")
    mc_ok=bool(mc.get("ready")) and nz(mc.get("prob_finish_below_start"),100)<=f("readiness_max_mc_below_start_pct")
    add("monte carlo downside",mc_ok,round(nz(mc.get("prob_finish_below_start"),100),1),f"<= {f('readiness_max_mc_below_start_pct')}%")
    passed=sum(1 for x in checks if x["pass"])
    score=round(passed/max(len(checks),1)*100)
    critical_fail=[x["name"] for x in checks if x["critical"] and not x["pass"]]
    status="READY_FOR_MANUAL_REVIEW" if not critical_fail and score>=90 else "OBSERVING" if score>=55 else "NOT_READY"
    return {
        "score":score,"status":status,"live_execution_locked":LIVE_EXECUTION_LOCKED,
        "shadow_hours":round(shadow_hours(),2),
        "checks":checks,"critical_failures":critical_fail,
        "message":"Passing this gate does not guarantee profitability and does not unlock live execution."
    }

def volatility_cost_multiplier(regime):
    return {
        "LOW":0.75,"NORMAL":1.0,"HIGH":1.65,"EXTREME":2.50,
        "WARMUP":1.15,"SPOT":1.0,"FALLBACK":1.25,"UNKNOWN":1.15
    }.get(str(regime or "UNKNOWN").upper(),1.15)

def execution_reference_depth(c,strategy):
    if str(strategy).startswith("PERP_"):
        oi=max(nz(c.get("open_interest_usd")),0)
        # OI is not order-book depth. This is deliberately conservative proxy depth.
        return max(100000.0,oi*0.01)
    return max(1000.0,nz(c.get("liquidity")))


def sol_usd_reference():
    for c in runtime.get("candidates",[]):
        if str(c.get("symbol","")).upper()=="SOL" and nz(c.get("price"))>0:
            return nz(c.get("price"))
    return 150.0

def launch_execution_cost_estimate(c,notional_usd):
    """
    Conservative PAPER estimate for a first-seconds Pump.fun bonding-curve trade.
    Fee model defaults to 1.25% Pump.fun + 0.50% future PumpPortal Local interface fee.
    Slippage/impact are additional and intentionally conservative.
    """
    notional=max(1.0,nz(notional_usd))
    lm=c.get("launch_metrics") or {}
    flow_sol=max(0.01,nz(lm.get("buy_sol_2s"),nz(lm.get("buy_sol_5s"),0.01)))
    depth_proxy=max(150.0,flow_sol*sol_usd_reference()*6.0)

    fee_bps=f("launch_protocol_fee_bps")+f("launch_interface_fee_bps")
    ratio=clamp(notional/max(depth_proxy,1),0,1)
    impact=f("launch_impact_coefficient_bps")*math.sqrt(ratio)
    confidence=nz(lm.get("sample_confidence"),0)/100
    low_conf_penalty=(1-clamp(confidence,0,1))*45
    slippage=f("launch_slippage_floor_bps")+impact*.45+low_conf_penalty
    latency=max(20.0,min(400.0,f("simulated_latency_ms")*.35))
    latency_bps=20+max(0,nz(lm.get("acceleration_score"))-70)*.8
    adverse=slippage+impact+latency_bps
    return {
        "fee_bps":round(fee_bps,3),
        "spread_bps":0.0,
        "half_spread_bps":0.0,
        "slippage_bps":round(slippage,3),
        "impact_bps":round(impact,3),
        "latency_bps":round(latency_bps,3),
        "adverse_bps":round(adverse,3),
        "all_in_bps":round(fee_bps+adverse,3),
        "latency_ms":round(latency,1),
        "reference_depth_usd":round(depth_proxy,2),
        "order_to_depth_pct":round(ratio*100,4),
    }

def execution_cost_estimate(c,strategy,notional_usd):
    if strategy=="LAUNCH_SNIPER":
        return launch_execution_cost_estimate(c,notional_usd)
    notional=max(1.0,nz(notional_usd))
    perp=str(strategy).startswith("PERP_")
    fee_bps=f("perp_fee_bps") if perp else f("spot_fee_bps")
    depth=execution_reference_depth(c,strategy)
    vol_mult=volatility_cost_multiplier(c.get("volatility_regime"))

    # Spread widens as reference depth falls. We charge half spread per fill.
    depth_scale=clamp(math.sqrt(100000.0/max(depth,1.0)),0.45,4.0)
    full_spread_bps=f("base_spread_bps")*depth_scale*vol_mult
    half_spread_bps=full_spread_bps/2.0

    ratio=clamp(notional/max(depth,1.0),0,1)
    impact_bps=f("impact_coefficient_bps")*math.sqrt(ratio)*vol_mult
    slippage_bps=f("slippage_floor_bps")*vol_mult + impact_bps*.35

    latency_ms=max(0.0,f("simulated_latency_ms"))
    vol_pct=abs(nz(c.get("volatility_pct")))
    if vol_pct<=0:
        vol_pct=abs(nz(c.get("m5")))*0.15
    latency_bps=(vol_pct*100.0)*math.sqrt(max(latency_ms,1.0)/15000.0)*0.25

    adverse_bps=half_spread_bps+slippage_bps+impact_bps+latency_bps
    all_in_bps=fee_bps+adverse_bps
    return {
        "fee_bps":round(fee_bps,3),
        "spread_bps":round(full_spread_bps,3),
        "half_spread_bps":round(half_spread_bps,3),
        "slippage_bps":round(slippage_bps,3),
        "impact_bps":round(impact_bps,3),
        "latency_bps":round(latency_bps,3),
        "adverse_bps":round(adverse_bps,3),
        "all_in_bps":round(all_in_bps,3),
        "latency_ms":round(latency_ms,1),
        "reference_depth_usd":round(depth,2),
        "order_to_depth_pct":round(ratio*100,4),
    }

def simulated_fill_price(mid_price,side,phase,adverse_bps):
    mid=max(nz(mid_price),1e-12)
    a=max(0,nz(adverse_bps))/10000.0
    side=str(side).upper()
    phase=str(phase).upper()
    # Adverse price direction:
    # LONG entry buys higher; LONG exit sells lower.
    # SHORT entry sells lower; SHORT exit buys higher.
    higher=(side=="LONG" and phase=="ENTRY") or (side=="SHORT" and phase!="ENTRY")
    return mid*(1+a if higher else 1-a)

def record_execution_event(position_id,trade_id,c,strategy,phase,notional_usd,mid_price,fill_price,est):
    with SessionLocal() as s:
        s.add(ExecutionEvent(
            position_id=position_id,trade_id=trade_id,
            symbol=str(c.get("symbol") or "?"),strategy=strategy,phase=phase,
            side=strategy_side(strategy),mid_price=mid_price,fill_price=fill_price,
            notional_usd=notional_usd,fee_bps=est["fee_bps"],spread_bps=est["spread_bps"],
            slippage_bps=est["slippage_bps"],impact_bps=est["impact_bps"],
            latency_bps=est["latency_bps"],adverse_bps=est["adverse_bps"],
            all_in_bps=est["all_in_bps"],latency_ms=est["latency_ms"],
            created_at=datetime.now(timezone.utc)
        ))
        s.commit()

def execution_quality_gate(c,strategy,notional_usd):
    if not b("execution_simulator_enabled"):
        return True,"ok",{"all_in_bps":0,"adverse_bps":0,"fee_bps":0}
    est=execution_cost_estimate(c,strategy,notional_usd)
    if est["all_in_bps"]>f("max_execution_cost_bps"):
        runtime["execution_blocks"]+=1
        runtime["last_execution_block"]={
            "time":datetime.now(timezone.utc).isoformat(),
            "symbol":c.get("symbol"),"strategy":strategy,
            "reason":"execution cost too high","estimate":est
        }
        return False,"execution cost too high",est
    return True,"ok",est

def execution_stats(limit=300):
    with SessionLocal() as s:
        rows=s.scalars(select(ExecutionEvent).order_by(ExecutionEvent.id.desc()).limit(limit)).all()
    if not rows:
        return {
            "fills":0,"avg_all_in_bps":0,"avg_adverse_bps":0,"avg_fee_bps":0,
            "avg_impact_bps":0,"avg_latency_bps":0,"entry_fills":0,"exit_fills":0,
            "blocks":runtime["execution_blocks"],"last_block":runtime["last_execution_block"],
            "recent":[]
        }
    def avg(attr):
        return sum(getattr(x,attr) for x in rows)/len(rows)
    recent=[{
        "symbol":x.symbol,"strategy":x.strategy,"phase":x.phase,"side":x.side,
        "mid_price":x.mid_price,"fill_price":x.fill_price,"notional_usd":x.notional_usd,
        "all_in_bps":x.all_in_bps,"adverse_bps":x.adverse_bps,"fee_bps":x.fee_bps,
        "impact_bps":x.impact_bps,"latency_bps":x.latency_bps,
        "created_at":x.created_at.isoformat()
    } for x in rows[:20]]
    return {
        "fills":len(rows),
        "avg_all_in_bps":round(avg("all_in_bps"),3),
        "avg_adverse_bps":round(avg("adverse_bps"),3),
        "avg_fee_bps":round(avg("fee_bps"),3),
        "avg_impact_bps":round(avg("impact_bps"),3),
        "avg_latency_bps":round(avg("latency_bps"),3),
        "entry_fills":sum(1 for x in rows if x.phase=="ENTRY"),
        "exit_fills":sum(1 for x in rows if x.phase!="ENTRY"),
        "blocks":runtime["execution_blocks"],
        "last_block":runtime["last_execution_block"],
        "recent":recent
    }


def router_soft_entry_count():
    now=time.time()
    runtime["router_soft_entry_times"]=[
        t for t in runtime["router_soft_entry_times"] if now-t<3600
    ]
    return len(runtime["router_soft_entry_times"])

def entry_router_assess(c,strategy=None,signal=None):
    strategy=strategy or suggested_strategy(c)
    signal=nz(signal if signal is not None else strategy_entry_score(strategy,c))
    threshold=effective_threshold(strategy,c)

    result={
        "state":"BLOCKED",
        "reason":"unknown",
        "strategy":strategy,
        "signal":round(signal,1),
        "threshold":round(threshold,1),
        "soft":False,
        "risk_multiplier":1.0,
    }

    if signal<threshold:
        result["reason"]="signal below threshold"
        return result

    # Strict path first.
    ok,reason=gate(c,strategy)
    if ok:
        collateral=planned_collateral(c,strategy)
        if collateral<5:
            result["reason"]="position too small"
            return result
        est=execution_cost_estimate(c,strategy,collateral*max(1.0,min(2.0,strategy_leverage(strategy))))
        result["execution_cost_bps"]=round(nz(est.get("all_in_bps")),1)
        if nz(est.get("all_in_bps"))>f("max_execution_cost_bps"):
            result["reason"]="execution cost too high"
            return result
        result.update({"state":"READY","reason":"all final gates passed"})
        return result

    result["strict_reason"]=reason

    # Never soften anything except near-miss spot liquidity/market-quality in PAPER.
    if not b("selective_entry_router_enabled") or operating_mode()!="PAPER":
        result["reason"]=reason
        return result
    if strategy not in ("SCALP_LONG","PUMP_LONG"):
        result["reason"]=reason
        return result
    if reason not in ("low spot liquidity","low spot market quality"):
        result["reason"]=reason
        return result

    liq=nz(c.get("liquidity"))
    mq=nz(c.get("market_risk"))
    bp=nz(c.get("buy_pressure"),50)
    m5=nz(c.get("m5"))

    if signal < threshold+f("router_min_signal_margin"):
        result["reason"]="soft router needs stronger signal"
        return result
    if liq < f("router_soft_liquidity_floor"):
        result["reason"]="liquidity below soft floor"
        return result
    if mq < f("router_soft_market_quality_floor"):
        result["reason"]="market quality below soft floor"
        return result
    if bp < f("router_min_buy_pressure"):
        result["reason"]="soft router weak buy pressure"
        return result
    if m5 > f("router_max_soft_m5_pct") or m5 < -2:
        result["reason"]="soft router momentum unsafe"
        return result
    if router_soft_entry_count() >= i("router_max_soft_entries_per_hour"):
        result["reason"]="soft-entry hourly limit"
        return result

    # Re-run every other guard with the controlled soft-pass flag.
    probe=dict(c)
    probe["_router_soft_pass"]=True
    ok2,reason2=gate(probe,strategy)
    if not ok2:
        result["reason"]=reason2
        return result

    collateral=planned_collateral(probe,strategy)
    if collateral<5:
        result["reason"]="position too small"
        return result

    est=execution_cost_estimate(
        probe,strategy,
        collateral*max(1.0,min(2.0,strategy_leverage(strategy)))
    )
    result["execution_cost_bps"]=round(nz(est.get("all_in_bps")),1)
    if nz(est.get("all_in_bps"))>f("max_execution_cost_bps"):
        result["reason"]="execution cost too high"
        return result

    result.update({
        "state":"SOFT_PASS",
        "reason":"strong signal; controlled paper exploration",
        "soft":True,
        "risk_multiplier":f("router_soft_risk_multiplier"),
        "liquidity":round(liq,2),
        "market_quality":round(mq,1),
        "buy_pressure":round(bp,1),
    })
    return result

def attach_entry_router_status(c):
    try:
        strategy=c.get("best_strategy") or suggested_strategy(c)
        signal=strategy_entry_score(strategy,c)
        c["entry_router"]=entry_router_assess(c,strategy,signal)
    except Exception as e:
        c["entry_router"]={
            "state":"BLOCKED","reason":f"router error: {str(e)[:80]}",
            "strategy":c.get("best_strategy") or "UNKNOWN"
        }
    return c

def planned_collateral(c,strategy):
    m=metrics()
    leverage=max(1.0,min(2.0,strategy_leverage(strategy)))
    rm=risk_multiplier()
    pw=portfolio_weight(strategy)
    strategy_risk_mult=f("sniper_risk_multiplier") if strategy=="SNIPER_LONG" else 1.0
    target_mult=daily_target_risk_multiplier()
    governor_mult=governor_risk_multiplier(strategy)
    router_mult=f("router_soft_risk_multiplier") if c.get("_router_soft_pass") else 1.0
    risk_budget=m["equity"]*f("risk_pct")/100*rm*pw*strategy_risk_mult*target_mult*governor_mult*router_mult
    effective_stop=max(0.25,strategy_max_loss_pct(strategy))
    collateral=risk_budget/max((effective_stop/100)*leverage,0.001)
    collateral=min(collateral,m["equity"]*f("max_position_pct")/100,f("cash"))
    return max(0.0,collateral)

def market_regime():
    cands=runtime.get("candidates") or []
    if not cands:
        return {"name":"WARMUP","confidence":0,"risk_on":50,"risk_off":50,
                "spot_breadth":50,"perp_bias":0,"high_vol_share":0}

    spot=[c for c in cands if not c.get("perp_eligible")]
    perp=[c for c in cands if c.get("perp_eligible")]

    spot_up=[]
    for c in spot[:20]:
        bp=nz(c.get("buy_pressure"),50)
        m5=nz(c.get("m5"),0)
        score=clamp(50 + (bp-50)*0.55 + m5*4.0,0,100)
        spot_up.append(score)
    spot_breadth=sum(spot_up)/len(spot_up) if spot_up else 50

    long_votes=0;short_votes=0;wait_votes=0
    perp_mom=[]
    high_vol=0
    for c in perp[:20]:
        d=str(c.get("direction") or "WAIT")
        if d=="LONG":long_votes+=1
        elif d=="SHORT":short_votes+=1
        else:wait_votes+=1
        perp_mom.append(nz(c.get("momentum_pct"),nz(c.get("m5"),0)))
        if c.get("volatility_regime") in ("HIGH","EXTREME"):high_vol+=1

    directional=max(1,long_votes+short_votes)
    perp_bias=(long_votes-short_votes)/directional*100
    avg_mom=sum(perp_mom)/len(perp_mom) if perp_mom else 0
    high_share=(high_vol/max(1,len(perp)))*100 if perp else 0

    risk_on=clamp(
        spot_breadth*.45 + (50+perp_bias*.45)*.30 + clamp(50+avg_mom*7,0,100)*.25,
        0,100
    )
    risk_off=100-risk_on

    if high_share>=55:
        name="HIGH_VOL"
        confidence=clamp(55+(high_share-55)*0.8,55,95)
    elif risk_on>=63:
        name="RISK_ON";confidence=clamp(50+(risk_on-50)*1.5,50,95)
    elif risk_off>=63:
        name="RISK_OFF";confidence=clamp(50+(risk_off-50)*1.5,50,95)
    else:
        name="CHOP";confidence=clamp(60-abs(risk_on-50),45,60)

    return {
        "name":name,"confidence":round(confidence,1),
        "risk_on":round(risk_on,1),"risk_off":round(risk_off,1),
        "spot_breadth":round(spot_breadth,1),"perp_bias":round(perp_bias,1),
        "avg_perp_momentum":round(avg_mom,3),"high_vol_share":round(high_share,1),
        "long_votes":long_votes,"short_votes":short_votes,"wait_votes":wait_votes
    }

def strategy_regime_weight(strategy, regime=None):
    regime=regime or market_regime()
    name=regime.get("name","CHOP")
    table={
        "RISK_ON":{"LAUNCH_SNIPER":0.55,"SNIPER_LONG":0.90,"PUMP_LONG":1.00,"SCALP_LONG":0.95,"PERP_LONG":1.00,"PERP_SHORT":0.50},
        "RISK_OFF":{"LAUNCH_SNIPER":0.20,"SNIPER_LONG":0.45,"PUMP_LONG":0.45,"SCALP_LONG":0.65,"PERP_LONG":0.50,"PERP_SHORT":1.00},
        "CHOP":{"LAUNCH_SNIPER":0.35,"SNIPER_LONG":0.70,"PUMP_LONG":0.60,"SCALP_LONG":1.00,"PERP_LONG":0.72,"PERP_SHORT":0.72},
        "HIGH_VOL":{"LAUNCH_SNIPER":0.15,"SNIPER_LONG":0.35,"PUMP_LONG":0.35,"SCALP_LONG":0.50,"PERP_LONG":0.55,"PERP_SHORT":0.55},
        "WARMUP":{"LAUNCH_SNIPER":0.30,"SNIPER_LONG":0.65,"PUMP_LONG":0.70,"SCALP_LONG":0.70,"PERP_LONG":0.70,"PERP_SHORT":0.70},
    }
    return table.get(name,table["CHOP"]).get(strategy,0.60)

def strategy_health_weight(strategy):
    h=strategy_health(strategy)
    status=h.get("status","LEARNING")
    return {
        "HEALTHY":1.00,
        "NEUTRAL":0.82,
        "LEARNING":0.75,
        "WEAK":0.45,
        "PAUSED":0.0,
    }.get(status,0.70)

def portfolio_weight(strategy):
    if not b("portfolio_brain_enabled"):
        return 1.0
    rw=strategy_regime_weight(strategy)
    hw=strategy_health_weight(strategy)
    # Never raises per-trade risk above the configured base risk.
    return round(clamp(rw*hw,0,1),3)

def portfolio_exposure():
    positions=positions_with_marks()
    cash=f("cash")
    equity=cash+sum(x["market_value"] for x in positions)
    denom=max(equity,1e-9)
    by_strategy={}
    by_direction={"LONG":0.0,"SHORT":0.0}
    total=0.0
    items=[]
    for p in positions:
        exposure=max(0,nz(p.get("remaining_cost")))
        total+=exposure
        by_strategy[p["strategy"]]=by_strategy.get(p["strategy"],0)+exposure
        side=p.get("side","LONG")
        by_direction[side]=by_direction.get(side,0)+exposure
        items.append({
            "symbol":p.get("symbol"),"mint":p.get("mint"),"strategy":p.get("strategy"),
            "side":side,"exposure":exposure,"exposure_pct":exposure/denom*100
        })
    return {
        "equity":equity,
        "total_usd":total,"total_pct":total/denom*100,
        "by_strategy_usd":by_strategy,
        "by_strategy_pct":{k:v/denom*100 for k,v in by_strategy.items()},
        "by_direction_usd":by_direction,
        "by_direction_pct":{k:v/denom*100 for k,v in by_direction.items()},
        "positions":items
    }

def recent_return_series(mint,points=None):
    points=points or i("correlation_lookback_points")
    with SessionLocal() as s:
        rows=s.scalars(
            select(MarketSnapshot)
            .where(MarketSnapshot.mint==mint)
            .order_by(MarketSnapshot.id.desc())
            .limit(points+1)
        ).all()
    rows=list(reversed(rows))
    vals=[x.price for x in rows if x.price and x.price>0]
    out=[]
    for a,bp in zip(vals[:-1],vals[1:]):
        if a>0:out.append(bp/a-1)
    return out

def pearson_corr(a,b):
    n=min(len(a),len(b))
    if n<6:return None
    a=a[-n:];b=b[-n:]
    ma=sum(a)/n;mb=sum(b)/n
    da=[x-ma for x in a];db=[x-mb for x in b]
    va=sum(x*x for x in da);vb=sum(x*x for x in db)
    if va<=1e-18 or vb<=1e-18:return None
    return sum(x*y for x,y in zip(da,db))/math.sqrt(va*vb)

def correlation_guard(c,strategy):
    if not b("portfolio_brain_enabled"):
        return {"blocked":False,"max_risk_corr":0,"with_symbol":None,"raw_corr":None}
    side=strategy_side(strategy)
    sign=1 if side=="LONG" else -1
    candidate=recent_return_series(c.get("mint",""))
    if len(candidate)<6:
        return {"blocked":False,"max_risk_corr":0,"with_symbol":None,"raw_corr":None,"reason":"warming correlation history"}

    exp=portfolio_exposure()
    worst=None
    for p in exp["positions"]:
        series=recent_return_series(p["mint"])
        corr=pearson_corr(candidate,series)
        if corr is None:continue
        other_sign=1 if p["side"]=="LONG" else -1
        # Positive risk correlation means both positions tend to gain/lose together
        # after accounting for LONG/SHORT direction.
        risk_corr=corr*sign*other_sign
        row={"symbol":p["symbol"],"strategy":p["strategy"],"raw_corr":corr,"risk_corr":risk_corr}
        if worst is None or risk_corr>worst["risk_corr"]:worst=row

    threshold=f("correlation_threshold")
    if worst and worst["risk_corr"]>=threshold:
        return {"blocked":True,"max_risk_corr":round(worst["risk_corr"],3),
                "raw_corr":round(worst["raw_corr"],3),"with_symbol":worst["symbol"],
                "with_strategy":worst["strategy"],"reason":"correlated portfolio risk"}
    return {"blocked":False,"max_risk_corr":round(worst["risk_corr"],3) if worst else 0,
            "raw_corr":round(worst["raw_corr"],3) if worst else None,
            "with_symbol":worst["symbol"] if worst else None,
            "with_strategy":worst["strategy"] if worst else None}

def portfolio_gate(c,strategy):
    if not b("portfolio_brain_enabled"):
        return True,"ok",{}
    weight=portfolio_weight(strategy)
    if weight < f("min_portfolio_weight"):
        return False,"portfolio weight too low",{"weight":weight}

    exp=portfolio_exposure()
    side=strategy_side(strategy)
    if exp["total_pct"] >= f("max_total_exposure_pct"):
        return False,"max total exposure",{"exposure":exp}
    if exp["by_strategy_pct"].get(strategy,0) >= f("max_strategy_exposure_pct"):
        return False,"max strategy exposure",{"exposure":exp}
    if exp["by_direction_pct"].get(side,0) >= f("max_direction_exposure_pct"):
        return False,"max direction exposure",{"exposure":exp}

    corr=correlation_guard(c,strategy)
    if corr.get("blocked"):
        return False,"correlation guard",corr
    return True,"ok",{"weight":weight,"correlation":corr}

def portfolio_status():
    regime=market_regime()
    exp=portfolio_exposure()
    strategies=["SNIPER_LONG","PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
    return {
        "enabled":b("portfolio_brain_enabled"),
        "regime":regime,
        "weights":{s:portfolio_weight(s) for s in strategies},
        "exposure":exp,
        "limits":{
            "total_pct":f("max_total_exposure_pct"),
            "strategy_pct":f("max_strategy_exposure_pct"),
            "direction_pct":f("max_direction_exposure_pct"),
            "correlation_threshold":f("correlation_threshold"),
            "min_weight":f("min_portfolio_weight")
        },
        "last_block":runtime.get("last_portfolio_block")
    }

def strategy_entry_score(strategy,c):
    if strategy=="LAUNCH_SNIPER":return nz(c.get("launch_score"))
    if strategy=="SNIPER_LONG":return nz(c.get("sniper_score"))
    if strategy=="PUMP_LONG":return nz(c.get("pump_score"))
    if strategy=="SCALP_LONG":return nz(c.get("scalp_score"))
    if strategy=="PERP_LONG":return nz(c.get("long_score"))
    if strategy=="PERP_SHORT":return nz(c.get("short_score"))
    return max(nz(c.get("long_score")),nz(c.get("short_score")),nz(c.get("pump_score")),nz(c.get("scalp_score")))

def base_threshold(strategy):
    return {
        "SNIPER_LONG":f("min_sniper_score"),
        "PUMP_LONG":f("min_pump_score"),
        "SCALP_LONG":f("min_scalp_score"),
        "PERP_LONG":f("min_long_score"),
        "PERP_SHORT":f("min_short_score")
    }.get(strategy,75.0)

def trade_feature_rows(strategy=None,limit=200):
    with SessionLocal() as s:
        q=select(TradeFeature)
        if strategy:q=q.where(TradeFeature.strategy==strategy)
        rows=s.scalars(q.order_by(TradeFeature.id.desc()).limit(limit)).all()
    return rows

def perf_from_feature_rows(rows):
    n=len(rows)
    if not n:
        return {"samples":0,"pnl":0,"expectancy":0,"win_rate":0,"profit_factor":0}
    pnl=sum(x.pnl for x in rows)
    wins=[x for x in rows if x.pnl>=0]
    losses=[x for x in rows if x.pnl<0]
    gw=sum(x.pnl for x in wins);gl=abs(sum(x.pnl for x in losses))
    pf=gw/gl if gl>0 else (999 if gw>0 else 0)
    return {
        "samples":n,"pnl":pnl,"expectancy":pnl/n,
        "win_rate":len(wins)/n*100,"profit_factor":pf
    }

def optimizer_report(strategy):
    rows=trade_feature_rows(strategy,250)
    min_samples=i("optimizer_min_samples")
    base=base_threshold(strategy)
    if len(rows)<min_samples:
        return {"strategy":strategy,"ready":False,"samples":len(rows),"base_threshold":base,
                "recommended_threshold":base,"effective_threshold":base,"reason":"collecting data","buckets":[]}
    buckets=[]
    candidates=[]
    for th in range(60,91,2):
        subset=[x for x in rows if x.entry_score>=th]
        if len(subset)<max(6,min_samples//3):continue
        p=perf_from_feature_rows(subset)
        # Conservative objective: positive expectancy + PF, with a small sample-size bonus.
        quality=p["expectancy"] + min(p["profit_factor"],3)*0.10 + min(len(subset),40)*0.005
        buckets.append({"threshold":th,**p})
        candidates.append((quality,th,p))
    if not candidates:
        rec=base;reason="insufficient score diversity"
    else:
        candidates.sort(key=lambda x:x[0],reverse=True)
        _,rec,p=candidates[0]
        reason=f"best bounded forward sample: n={p['samples']} exp={p['expectancy']:.2f}"
    max_shift=f("max_adaptive_shift")
    rec=clamp(rec,base-max_shift,base+max_shift)

    recent=perf_from_feature_rows(rows[:min(30,len(rows))])
    effective=base
    if b("adaptive_enabled"):
        if recent["samples"]>=min_samples and (recent["expectancy"]<0 or recent["profit_factor"]<0.9):
            effective=min(base+max_shift,max(base+2,rec))
            reason="defensive tighten: negative recent edge"
        elif recent["samples"]>=min_samples and recent["expectancy"]>0 and recent["profit_factor"]>=1.25:
            # Loosening is deliberately capped at 2 points to avoid aggressive overfit.
            effective=max(base-2,min(base,rec))
            reason="small bounded loosen: positive recent edge"
    return {
        "strategy":strategy,"ready":True,"samples":len(rows),"base_threshold":base,
        "recommended_threshold":round(rec,2),"effective_threshold":round(effective,2),
        "recent":recent,"reason":reason,"buckets":buckets[-10:]
    }

def effective_threshold(strategy,c=None):
    base=base_threshold(strategy)
    if not b("adaptive_enabled"):return base
    rep=optimizer_report(strategy)
    th=rep.get("effective_threshold",base)
    # Regime-aware tightening. Never loosen because of volatility.
    if c and c.get("volatility_regime")=="HIGH":th+=2
    return min(95,th)

def strategy_health(strategy):
    rows=trade_feature_rows(strategy,30)
    p=perf_from_feature_rows(rows)
    status="LEARNING"
    if p["samples"]>=i("optimizer_min_samples"):
        if p["expectancy"]>0 and p["profit_factor"]>=1.15:status="HEALTHY"
        elif p["expectancy"]<0 and p["profit_factor"]<0.85:status="WEAK"
        else:status="NEUTRAL"
    pause_until=runtime["strategy_pauses"].get(strategy)
    if pause_until and datetime.now(timezone.utc)<pause_until:status="PAUSED"
    return {"strategy":strategy,"status":status,**p,
            "pause_until":pause_until.isoformat() if pause_until else None}

def maybe_pause_strategy(strategy):
    if not b("adaptive_enabled"):return
    rows=trade_feature_rows(strategy,20)
    min_samples=i("strategy_pause_min_samples")
    if len(rows)<min_samples:return
    recent=perf_from_feature_rows(rows[:min_samples])
    if recent["expectancy"]<0 and recent["win_rate"]<35 and recent["profit_factor"]<0.75:
        runtime["strategy_pauses"][strategy]=datetime.now(timezone.utc)+timedelta(minutes=i("strategy_pause_minutes"))

def risk_multiplier():
    m=metrics()
    dd=abs(min(0,m.get("current_drawdown_pct",0)))
    mult=1.0
    soft=f("drawdown_soft_cut_pct");hard=f("drawdown_hard_cut_pct")
    if dd>=hard:mult=0.40
    elif dd>=soft:mult=0.70
    losses=consecutive_losses()
    if losses>=2:mult=min(mult,0.70)
    if losses>=3:mult=min(mult,0.45)
    return round(mult,2)

def adaptive_status():
    strategies=["SNIPER_LONG","PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
    return {
        "enabled":b("adaptive_enabled"),
        "risk_multiplier":risk_multiplier(),
        "health":{s:strategy_health(s) for s in strategies},
        "optimizers":{s:optimizer_report(s) for s in strategies}
    }


def strategy_recent_trade_stats(strategy,limit=None):
    limit=limit or i("governor_recent_trades")
    with SessionLocal() as s:
        rows=s.scalars(
            select(Trade).where(Trade.strategy==strategy).order_by(Trade.id.desc()).limit(limit)
        ).all()

    wins=[x for x in rows if nz(x.pnl)>0]
    losses=[x for x in rows if nz(x.pnl)<=0]
    gw=sum(nz(x.pnl) for x in wins)
    gl=abs(sum(nz(x.pnl) for x in losses))
    pf=gw/gl if gl>0 else (999 if gw>0 else 0)
    exp_pct=sum(nz(x.pnl_pct) for x in rows)/len(rows) if rows else 0
    streak=0
    for x in rows:
        if nz(x.pnl)<0:streak+=1
        else:break
    return {
        "samples":len(rows),"wins":len(wins),
        "win_rate":100*len(wins)/len(rows) if rows else 0,
        "pnl":sum(nz(x.pnl) for x in rows),
        "expectancy_pct":exp_pct,
        "profit_factor":pf,
        "loss_streak":streak
    }

def strategy_governor_status(strategy):
    stats=strategy_recent_trade_stats(strategy)
    pause_until=runtime["governor_pauses"].get(strategy)
    if pause_until and datetime.now(timezone.utc)<pause_until:
        return {"strategy":strategy,"state":"PAUSED","risk_multiplier":0.0,
                "pause_until":pause_until.isoformat(),**stats}

    if stats["loss_streak"]>=i("governor_loss_streak_limit"):
        until=datetime.now(timezone.utc)+timedelta(minutes=i("governor_loss_streak_pause_minutes"))
        runtime["governor_pauses"][strategy]=until
        runtime["governor_last_reason"][strategy]="loss streak"
        return {"strategy":strategy,"state":"PAUSED","risk_multiplier":0.0,
                "pause_until":until.isoformat(),**stats}

    if stats["samples"]<i("governor_min_samples"):
        return {"strategy":strategy,"state":"PROBATION",
                "risk_multiplier":f("governor_probation_risk_multiplier"),
                "pause_until":None,**stats}

    if stats["expectancy_pct"]<=0 or stats["profit_factor"]<f("governor_min_profit_factor"):
        until=datetime.now(timezone.utc)+timedelta(minutes=i("governor_pause_minutes"))
        runtime["governor_pauses"][strategy]=until
        runtime["governor_last_reason"][strategy]="negative edge"
        return {"strategy":strategy,"state":"PAUSED","risk_multiplier":0.0,
                "pause_until":until.isoformat(),**stats}

    if stats["profit_factor"]<1.15 or stats["win_rate"]<42:
        return {"strategy":strategy,"state":"CAUTION",
                "risk_multiplier":f("governor_caution_risk_multiplier"),
                "pause_until":None,**stats}

    return {"strategy":strategy,"state":"ENABLED","risk_multiplier":1.0,
            "pause_until":None,**stats}

def governor_risk_multiplier(strategy):
    if not b("edge_governor_enabled"):return 1.0
    return nz(strategy_governor_status(strategy).get("risk_multiplier"),0)

def strategy_governor_gate(strategy):
    if not b("edge_governor_enabled"):return True,"ok"
    st=strategy_governor_status(strategy)
    if st["state"]=="PAUSED":
        return False,"edge governor paused strategy"
    return True,"ok"

def survival_guard_status():
    realized=today_realized()
    open_pnl=global_open_pnl()
    combined=realized+open_pnl
    day_limit=-(f("start_balance")*f("survival_daily_loss_pct")/100)
    combined_limit=-(f("start_balance")*f("survival_combined_loss_pct")/100)
    return {
        "realized":realized,"open_pnl":open_pnl,"combined":combined,
        "daily_limit_usd":day_limit,"combined_limit_usd":combined_limit,
        "blocked":realized<=day_limit or combined<=combined_limit
    }

def gate(c,strategy=None):
    if b("killed"): return False,"kill switch"
    if not b("bot_enabled"): return False,"bot stopped"

    health=source_health()
    if health["last_loop_age_sec"]>f("max_data_age_sec"):
        return False,"stale market data"
    if operating_mode()=="SHADOW" and not b("execution_simulator_enabled"):
        return False,"shadow requires execution simulator"
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]:
        return False,"loss pause"

    if strategy:
        spu=runtime["strategy_pauses"].get(strategy)
        if spu and datetime.now(timezone.utc)<spu:
            return False,"strategy adaptive pause"
        gok,greason=strategy_governor_gate(strategy)
        if not gok:
            return False,greason

    # V5.1 HOTFIX:
    # SPOT and PERP no longer share one market-quality gate.
    if c.get("perp_eligible"):
        # PERP gate = OI + direction edge + funding + volatility + primary source in SHADOW.
        # The generic spot market-quality score is intentionally NOT a hard gate here.
        if c.get("data_source")=="VELOCITY":
            if c.get("open_interest_usd",0) < f("min_perp_oi_usd"):
                return False,"low perp open interest"
            if c.get("direction_edge",0) < f("min_direction_edge"):
                return False,"weak directional edge"
        if abs(c.get("funding_rate",0)) > f("max_abs_funding_rate"):
            return False,"extreme funding"
        if b("block_extreme_volatility") and c.get("volatility_regime")=="EXTREME":
            return False,"extreme volatility"
        if operating_mode()=="SHADOW" and c.get("data_source")!="VELOCITY":
            return False,"shadow requires primary perp source"
    else:
        # SPOT gate = liquidity + quality + security.
        # Capital Shield applies stricter floors than older persisted dashboard settings.
        min_liq=f("min_liquidity")
        min_quality=f("min_spot_market_quality")

        router_soft=(
            b("selective_entry_router_enabled")
            and operating_mode()=="PAPER"
            and bool(c.get("_router_soft_pass"))
            and strategy in ("SCALP_LONG","PUMP_LONG")
        )

        if router_soft:
            # Soft-pass only relaxes the SPOT floor slightly for PAPER exploration.
            # All security, extreme-move, route, portfolio, execution and survival guards remain active.
            min_liq=max(min_liq,f("router_soft_liquidity_floor"))
            min_quality=max(min_quality,f("router_soft_market_quality_floor"))
        elif b("capital_shield_enabled"):
            min_liq=max(min_liq,f("capital_shield_min_liquidity"))
            min_quality=max(min_quality,f("capital_shield_min_market_quality"))

        if c.get("liquidity",0) < min_liq:
            return False,"low spot liquidity"
        if c.get("market_risk",0) < min_quality:
            return False,"low spot market quality"
        if b("capital_shield_enabled") and abs(nz(c.get("m5"))) >= f("max_spot_abs_m5_pct"):
            return False,"spot move too extreme"

        sec=c.get("security") or {}
        sec_status=sec.get("status","UNKNOWN")
        sec_score=sec.get("score")
        if sec_status!="UNKNOWN" and sec_score is not None and nz(sec_score)<f("min_token_security_score"):
            return False,"token security score too low"
        if operating_mode()=="SHADOW" and b("security_required_shadow") and sec_status=="UNKNOWN":
            return False,"shadow requires token security scan"
        if operating_mode()=="SHADOW" and b("security_hard_block_shadow") and sec.get("hard_block"):
            return False,"shadow token security hard block"

    if strategy and strategy_side(strategy)=="SHORT" and not c.get("perp_eligible"):
        return False,"short unavailable for spot-only token"

    with SessionLocal() as s:
        if s.scalar(select(Position).where(Position.mint==c["mint"])):
            return False,"already open"
        if len(s.scalars(select(Position)).all()) >= i("max_positions"):
            return False,"max positions"

    cd=runtime["cooldowns"].get(c["mint"])
    if cd and time.time()<cd:
        return False,"cooldown"

    start_balance=f("start_balance")
    if today_realized() <= -(start_balance*f("daily_loss_limit_pct")/100):
        return False,"daily loss limit"
    if b("capital_shield_enabled") and global_equity_guard_status()["blocked"]:
        return False,"global equity guard"
    if survival_guard_status()["blocked"]:
        return False,"survival loss guard"

    if b("daily_target_lock_enabled"):
        target=daily_target_status()
        if target["locked"]:
            runtime["daily_target_locked"]=True
            if not runtime.get("daily_target_lock_time"):
                runtime["daily_target_lock_time"]=datetime.now(timezone.utc).isoformat()
            return False,"daily profit target locked"

    if open_risk_pct() >= f("max_total_open_risk_pct"):
        return False,"portfolio open risk cap"

    if strategy:
        rok,rreason,rdetail=route_gate(c,strategy)
        if not rok:
            return False,rreason

        pok,preason,pdetail=portfolio_gate(c,strategy)
        if not pok:
            runtime["last_portfolio_block"]={
                "time":datetime.now(timezone.utc).isoformat(),
                "symbol":c.get("symbol"),"strategy":strategy,
                "reason":preason,"detail":pdetail
            }
            return False,preason

    return True,"ok"

def open_position(c,strategy):
    ok,reason=gate(c,strategy)
    if not ok:return False,reason
    leverage=max(1.0,min(2.0,strategy_leverage(strategy)))
    collateral=planned_collateral(c,strategy)
    if not c.get("perp_eligible") and b("capital_shield_enabled"):
        liq=max(0,nz(c.get("liquidity")))
        liq_pct=min(f("max_spot_position_liquidity_pct"),f("recovery_spot_liquidity_position_cap_pct"))
        liq_cap=liq*liq_pct/100
        collateral=min(collateral,liq_cap)
    if collateral<5:return False,"position too small"

    execution_notional=collateral*leverage
    eok,ereason,est=execution_quality_gate(c,strategy,execution_notional)
    if not eok:return False,ereason

    mid_price=nz(c.get("price"))
    side=strategy_side(strategy)
    fill_price=simulated_fill_price(mid_price,side,"ENTRY",est.get("adverse_bps",0)) if b("execution_simulator_enabled") else mid_price
    fee=execution_notional*est.get("fee_bps",0)/10000.0 if b("execution_simulator_enabled") else collateral*f("execution_cost_pct")/100
    cash_need=collateral+fee
    if cash_need>f("cash"): return False,"cash"

    setv("cash",f("cash")-cash_need)
    now=datetime.now(timezone.utc)
    position_id=None
    with SessionLocal() as s:
        pos=Position(
            mint=c["mint"],symbol=c["symbol"],name=c["name"],strategy=strategy,
            entry_price=fill_price,last_price=mid_price,peak_price=mid_price,
            initial_notional=collateral,remaining_cost=collateral,locked_pnl=-fee,
            opened_at=now
        )
        s.add(pos);s.flush()
        position_id=pos.id
        s.add(PositionFeature(
            position_id=pos.id,strategy=strategy,entry_score=strategy_entry_score(strategy,c),
            long_score=nz(c.get("long_score")),short_score=nz(c.get("short_score")),
            pump_score=nz(c.get("pump_score")),scalp_score=nz(c.get("scalp_score")),
            market_risk=nz(c.get("market_risk")),direction_edge=nz(c.get("direction_edge")),
            funding_rate=nz(c.get("funding_rate")),open_interest_usd=nz(c.get("open_interest_usd")),
            volatility_regime=str(c.get("volatility_regime") or "UNKNOWN"),created_at=now
        ))
        s.commit()
    if b("execution_simulator_enabled"):
        record_execution_event(position_id,None,c,strategy,"ENTRY",execution_notional,mid_price,fill_price,est)
    if not c.get("perp_eligible"):
        arm_realtime_exit_ref(c["mint"],position_id)

    if c.get("_router_soft_pass"):
        now_ts=time.time()
        runtime["router_soft_entry_times"].append(now_ts)
        runtime["router_soft_entries"]+=1
        runtime["router_last_entry"]={
            "time":datetime.now(timezone.utc).isoformat(),
            "symbol":c.get("symbol"),"strategy":strategy,
            "mode":"SOFT_PASS","signal":strategy_entry_score(strategy,c)
        }
        record_event(
            "INFO","ROUTER_SOFT_ENTRY",
            f"{c.get('symbol')} {strategy} controlled PAPER soft entry",
            {"signal":strategy_entry_score(strategy,c),
             "market_quality":c.get("market_risk"),
             "liquidity":c.get("liquidity"),
             "risk_multiplier":f("router_soft_risk_multiplier")},
            dedupe_sec=10
        )
    else:
        runtime["router_last_entry"]={
            "time":datetime.now(timezone.utc).isoformat(),
            "symbol":c.get("symbol"),"strategy":strategy,
            "mode":"READY","signal":strategy_entry_score(strategy,c)
        }
    return True,"opened"

def partial_sell(p, c, fraction):
    fraction=min(fraction,p.remaining_cost/max(p.initial_notional,1e-9))
    cost_basis=min(p.initial_notional*fraction,p.remaining_cost)
    if cost_basis<=0:return
    side=strategy_side(p.strategy)
    leverage=strategy_leverage(p.strategy)
    mid_price=nz(c.get("price"))
    execution_notional=cost_basis*leverage
    est=execution_cost_estimate(c,p.strategy,execution_notional) if b("execution_simulator_enabled") else {
        "fee_bps":f("execution_cost_pct")*100,"adverse_bps":0,"spread_bps":0,
        "slippage_bps":0,"impact_bps":0,"latency_bps":0,"all_in_bps":f("execution_cost_pct")*100,
        "latency_ms":0
    }
    fill_price=simulated_fill_price(mid_price,side,"PARTIAL_EXIT",est.get("adverse_bps",0)) if b("execution_simulator_enabled") else mid_price
    raw=directional_raw_return(p.entry_price,fill_price,side)
    gross_return=max(0.0,cost_basis*(1+raw*leverage))
    fee=execution_notional*est.get("fee_bps",0)/10000.0
    net=max(0.0,gross_return-fee)
    pnl=net-cost_basis
    setv("cash",f("cash")+net)
    p.remaining_cost-=cost_basis
    p.locked_pnl+=pnl
    if b("execution_simulator_enabled"):
        record_execution_event(p.id,None,c,p.strategy,"PARTIAL_EXIT",execution_notional,mid_price,fill_price,est)

def close_position(p,c,reason):
    side=strategy_side(p.strategy)
    leverage=strategy_leverage(p.strategy)
    mid_price=nz(c.get("price"))
    execution_notional=p.remaining_cost*leverage
    est=execution_cost_estimate(c,p.strategy,execution_notional) if b("execution_simulator_enabled") else {
        "fee_bps":f("execution_cost_pct")*100,"adverse_bps":0,"spread_bps":0,
        "slippage_bps":0,"impact_bps":0,"latency_bps":0,"all_in_bps":f("execution_cost_pct")*100,
        "latency_ms":0
    }
    fill_price=simulated_fill_price(mid_price,side,"EXIT",est.get("adverse_bps",0)) if b("execution_simulator_enabled") else mid_price
    raw=directional_raw_return(p.entry_price,fill_price,side)
    gross_return=max(0.0,p.remaining_cost*(1+raw*leverage))
    fee=execution_notional*est.get("fee_bps",0)/10000.0
    net=max(0.0,gross_return-fee)
    rem_pnl=net-p.remaining_cost

    # Approximate funding carry for paper perps.
    funding_pnl=0.0
    pf_snapshot=None
    if str(p.strategy).startswith("PERP_"):
        with SessionLocal() as s:
            pf_snapshot=s.scalar(select(PositionFeature).where(PositionFeature.position_id==p.id))
        if pf_snapshot:
            hours=max(0,(datetime.now(timezone.utc)-(p.opened_at if p.opened_at.tzinfo else p.opened_at.replace(tzinfo=timezone.utc))).total_seconds()/3600)
            avg_funding=(nz(pf_snapshot.funding_rate)+nz(c.get("funding_rate")))/2.0
            avg_collateral=(p.initial_notional+p.remaining_cost)/2.0
            signed=-1 if side=="LONG" else 1
            funding_pnl=signed*avg_funding*hours*avg_collateral*leverage
            funding_pnl=clamp(funding_pnl,-p.initial_notional*.05,p.initial_notional*.05)

    total=p.locked_pnl+rem_pnl+funding_pnl
    setv("cash",f("cash")+net+funding_pnl)
    pnl_pct=total/max(p.initial_notional,1e-9)*100
    closed=datetime.now(timezone.utc)
    trade_id=None
    with SessionLocal() as s:
        obj=s.get(Position,p.id)
        if not obj:return
        pf=s.scalar(select(PositionFeature).where(PositionFeature.position_id==p.id))
        tr=Trade(mint=p.mint,symbol=p.symbol,strategy=p.strategy,pnl=total,pnl_pct=pnl_pct,
                 reason=reason,opened_at=p.opened_at,closed_at=closed)
        s.add(tr);s.flush()
        trade_id=tr.id
        if pf:
            s.add(TradeFeature(
                trade_id=tr.id,strategy=p.strategy,entry_score=pf.entry_score,
                market_risk=pf.market_risk,direction_edge=pf.direction_edge,
                funding_rate=pf.funding_rate,open_interest_usd=pf.open_interest_usd,
                volatility_regime=pf.volatility_regime,pnl=total,pnl_pct=pnl_pct,closed_at=closed
            ))
            s.delete(pf)
        s.delete(obj);s.commit()
    if b("execution_simulator_enabled"):
        record_execution_event(p.id,trade_id,c,p.strategy,"EXIT",execution_notional,mid_price,fill_price,est)
    runtime["cooldowns"][p.mint]=time.time()+i("cooldown_minutes")*60
    runtime["realtime_exit_refs"].pop(p.mint,None)
    runtime["realtime_exit_last_eval"].pop(p.mint,None)
    runtime["launch_marks"].pop(p.mint,None)
    if p.strategy=="LAUNCH_SNIPER" and p.mint in runtime["launch_watch"]:
        runtime["launch_watch"][p.mint]["status"]="CLOSED"
        runtime["launch_watch"][p.mint]["last_reason"]=reason
    runtime["realtime_exit_last_eval"].pop(p.mint+":rest",None)
    record_event("INFO","POSITION_CLOSED",f"{p.symbol} {p.strategy} closed: {reason}",
                 {"pnl":round(total,4),"pnl_pct":round(pnl_pct,3),"funding_pnl":round(funding_pnl,4)})
    maybe_pause_strategy(p.strategy)
    recovery_limit=min(i("max_consecutive_losses"),i("governor_loss_streak_limit"))
    if consecutive_losses()>=recovery_limit:
        pause_minutes=max(i("loss_pause_minutes"),i("governor_loss_streak_pause_minutes"))
        runtime["pause_until"]=datetime.now(timezone.utc)+timedelta(minutes=pause_minutes)




def daily_target_status():
    realized=today_realized()
    open_pnl=global_open_pnl()
    combined=realized+open_pnl
    target_usd=f("start_balance")*f("daily_profit_target_pct")/100.0
    derisk_usd=f("start_balance")*f("daily_de_risk_start_pct")/100.0
    secure_trigger_usd=f("start_balance")*(f("daily_profit_target_pct")+f("daily_profit_secure_buffer_pct"))/100.0

    hit_realized=realized>=target_usd
    secure_ready=combined>=secure_trigger_usd
    derisk=combined>=derisk_usd or realized>=derisk_usd

    return {
        "target_pct":f("daily_profit_target_pct"),
        "target_usd":target_usd,
        "de_risk_start_pct":f("daily_de_risk_start_pct"),
        "de_risk":derisk,
        "realized":realized,
        "open_pnl":open_pnl,
        "combined":combined,
        "progress_pct":clamp(combined/max(target_usd,1e-9)*100,0,200),
        "target_hit_realized":hit_realized,
        "secure_ready":secure_ready,
        "locked":runtime.get("daily_target_locked",False) or hit_realized,
        "lock_time":runtime.get("daily_target_lock_time"),
        "note":"Target is a risk-management objective, not a guaranteed return."
    }

def daily_target_risk_multiplier():
    if not b("daily_target_lock_enabled"):
        return 1.0
    st=daily_target_status()
    if st["locked"]:
        return 0.0
    if st["de_risk"]:
        return clamp(f("daily_de_risk_multiplier"),0.1,1.0)
    return 1.0

def open_risk_pct():
    with SessionLocal() as s:
        rows=s.scalars(select(Position)).all()
    total_risk=0.0
    for p in rows:
        stop=strategy_max_loss_pct(p.strategy)/100.0
        total_risk += max(0,nz(p.remaining_cost))*stop
    return total_risk/max(f("start_balance"),1e-9)*100.0

def strategy_max_loss_pct(strategy):
    hard=f("stop_loss_pct")
    if not b("capital_shield_enabled"):
        return hard
    if strategy=="LAUNCH_SNIPER":
        return f("launch_max_net_loss_pct")
    if strategy=="SNIPER_LONG":
        return min(hard,f("sniper_max_loss_pct"),f("recovery_sniper_loss_cap_pct"))
    if strategy=="SCALP_LONG":
        return min(hard,f("scalp_max_loss_pct"),f("recovery_scalp_loss_cap_pct"))
    if strategy=="PUMP_LONG":
        return min(hard,f("pump_max_loss_pct"),f("recovery_pump_loss_cap_pct"))
    if strategy in ("PERP_LONG","PERP_SHORT"):
        return min(hard,f("perp_max_loss_pct"),f("recovery_perp_loss_cap_pct"))
    return hard

def strategy_breakeven_rule(strategy):
    if strategy=="LAUNCH_SNIPER":return 4.00,0.35
    if strategy=="SNIPER_LONG":return 0.50,0.02
    if strategy=="SCALP_LONG":return 0.75,0.02
    if strategy=="PUMP_LONG":return 1.00,0.05
    if strategy in ("PERP_LONG","PERP_SHORT"):return 0.70,0.02
    return 1.0,0.02

def global_open_pnl():
    positions=positions_with_marks()
    return sum(nz(p.get("market_value"))-nz(p.get("remaining_cost"))+nz(p.get("locked_pnl")) for p in positions)

def global_equity_guard_status():
    realized=today_realized()
    open_pnl=global_open_pnl()
    combined=realized+open_pnl
    limit=-(f("start_balance")*f("global_equity_guard_pct")/100)
    return {
        "realized_today":realized,
        "open_pnl":open_pnl,
        "combined_pnl":combined,
        "limit_usd":limit,
        "blocked":combined<=limit
    }

def expected_exit_total_pct(p,c):
    """Estimated total P&L % if the remaining position were closed now, after simulated exit friction."""
    side=strategy_side(p.strategy)
    leverage=strategy_leverage(p.strategy)
    mid=nz(c.get("price"))
    if mid<=0:return directional_return_pct(p,mid)
    execution_notional=max(0,p.remaining_cost*leverage)
    if b("execution_simulator_enabled") and execution_notional>0:
        est=execution_cost_estimate(c,p.strategy,execution_notional)
        fill=simulated_fill_price(mid,side,"MARK_EXIT",est.get("adverse_bps",0))
        fee=execution_notional*est.get("fee_bps",0)/10000.0
    else:
        fill=mid
        fee=p.remaining_cost*f("execution_cost_pct")/100
    raw=directional_raw_return(p.entry_price,fill,side)
    gross=max(0.0,p.remaining_cost*(1+raw*leverage))
    net=max(0.0,gross-fee)
    total=p.locked_pnl+(net-p.remaining_cost)
    return total/max(p.initial_notional,1e-9)*100

def strategy_profit_lock(strategy,peak_net_pct):
    p=nz(peak_net_pct)
    if strategy=="LAUNCH_SNIPER":
        if p>=25:return max(15.0,p-7.0)
        if p>=15:return 8.0
        if p>=10:return 5.0
        if p>=7:return 3.0
        if p>=5:return 1.25
    elif strategy=="SNIPER_LONG":
        if p>=3:return max(2.0,p-0.8)
        if p>=2:return 1.15
        if p>=1.25:return 0.45
        if p>=0.75:return 0.08
    elif strategy=="SCALP_LONG":
        if p>=5:return max(3.0,p-1.5)
        if p>=3:return 1.50
        if p>=1.75:return 0.60
        if p>=1.00:return 0.12
    elif strategy=="PUMP_LONG":
        if p>=20:return max(10.0,p-6.0)
        if p>=10:return 4.0
        if p>=6:return 2.0
        if p>=3:return 0.75
        if p>=1.50:return 0.15
    elif strategy in ("PERP_LONG","PERP_SHORT"):
        if p>=8:return max(4.0,p-3.0)
        if p>=5:return 2.0
        if p>=2.5:return 0.75
        if p>=1.0:return 0.10
    return None

def strategy_tp_plan(strategy):
    if strategy=="LAUNCH_SNIPER":
        return [(5.0,.35),(10.0,.30),(18.0,.20)]
    if strategy=="SNIPER_LONG":
        return [(0.80,.40),(1.50,.35),(2.50,.25)]
    if strategy=="SCALP_LONG":
        return [(1.50,.30),(2.50,.30),(4.00,.25)]
    if strategy=="PUMP_LONG":
        return [(3.00,.20),(6.00,.20),(10.00,.20)]
    if strategy in ("PERP_LONG","PERP_SHORT"):
        return [(1.50,.20),(3.00,.25),(5.00,.25)]
    return [(2.0,.25),(4.0,.25),(6.0,.25)]

def manage_positions():
    cands={x["mint"]:x for x in runtime["candidates"]}
    cands.update(runtime.get("launch_marks",{}))
    with SessionLocal() as s:
        pos=s.scalars(select(Position)).all()
        for p in pos:s.expunge(p)

    # Account-level emergency guard. It uses realized + current open P&L.
    guard=global_equity_guard_status()
    flatten_all=b("capital_shield_enabled") and guard["blocked"] and bool(pos)
    if flatten_all:
        record_event("WARN","GLOBAL_EQUITY_GUARD","Global equity guard triggered",
                     {"combined_pnl":guard["combined_pnl"],"limit_usd":guard["limit_usd"]},dedupe_sec=60)

    # Daily Profit Secure: once combined paper P&L exceeds target + buffer,
    # close open positions to bank the day and stop opening new trades.
    survival=survival_guard_status()
    survival_flatten=survival["blocked"] and bool(pos)
    if survival_flatten:
        record_event("WARN","SURVIVAL_GUARD","Recovery survival guard triggered",
                     {"combined":survival["combined"],"daily":survival["realized"]},dedupe_sec=60)

    target=daily_target_status()
    secure_daily=b("daily_target_lock_enabled") and target["secure_ready"] and bool(pos)
    if secure_daily:
        record_event("INFO","DAILY_TARGET_SECURE","Daily profit target secure triggered",
                     {"combined":target["combined"],"target_usd":target["target_usd"]},dedupe_sec=60)

    for p in pos:
        c=cands.get(p.mint)
        if not c:
            last_seen=runtime["position_price_seen"].get(p.mint,0)
            if time.time()-last_seen>f("stale_position_price_sec"):
                record_event("WARN","POSITION_PRICE_STALE",f"{p.symbol} open-position price is stale",
                             {"mint":p.mint,"strategy":p.strategy},dedupe_sec=120)
            continue

        runtime["position_price_seen"][p.mint]=time.time()
        with SessionLocal() as s:
            obj=s.get(Position,p.id)
            if not obj:continue

            side=strategy_side(obj.strategy)
            obj.last_price=c["price"]
            if side=="SHORT":
                obj.peak_price=min(obj.peak_price,c["price"])
            else:
                obj.peak_price=max(obj.peak_price,c["price"])

            net_ret=expected_exit_total_pct(obj,c)
            peak_c=dict(c);peak_c["price"]=obj.peak_price
            peak_net_ret=expected_exit_total_pct(obj,peak_c)
            pull_net=net_ret-peak_net_ret

            prevliq=runtime["prev_liq"].get(obj.mint,c.get("liquidity",0))
            opened=obj.opened_at if obj.opened_at.tzinfo else obj.opened_at.replace(tzinfo=timezone.utc)
            held_seconds=max(0,(datetime.now(timezone.utc)-opened).total_seconds())
            held_minutes=held_seconds/60
            if obj.strategy=="LAUNCH_SNIPER":
                max_hold=f("launch_max_hold_sec")/60
            elif obj.strategy=="SNIPER_LONG":
                max_hold=f("sniper_max_hold_minutes")
            else:
                max_hold=f("perp_max_hold_minutes") if str(obj.strategy).startswith("PERP_") else f("spot_max_hold_minutes")
            reason=None

            if secure_daily:
                reason="DAILY_TARGET_SECURE"
            elif survival_flatten:
                reason="SURVIVAL_GUARD"
            elif flatten_all:
                reason="GLOBAL_EQUITY_GUARD"

            # First-seconds launch controls use raw event-native market-cap movement
            # plus net-after-friction P&L. Creator sell is an immediate emergency exit.
            elif obj.strategy=="LAUNCH_SNIPER" and (c.get("launch_metrics") or {}).get("creator_sell"):
                reason="LAUNCH_CREATOR_SELL"
            elif obj.strategy=="LAUNCH_SNIPER" and nz(c.get("launch_raw_move_pct"))<=-f("launch_raw_stop_pct"):
                reason="LAUNCH_RAW_STOP"
            elif obj.strategy=="LAUNCH_SNIPER" and held_seconds>=f("launch_scratch_after_sec") and \
                 peak_net_ret<f("launch_scratch_peak_pct") and net_ret<=-f("launch_scratch_loss_pct"):
                reason="LAUNCH_SCRATCH_EXIT"
            elif obj.strategy=="LAUNCH_SNIPER" and held_seconds>=f("launch_flow_reversal_after_sec") and \
                 nz((c.get("launch_metrics") or {}).get("buy_pressure_2s"),50)<f("launch_flow_reversal_pressure") and \
                 int((c.get("launch_metrics") or {}).get("sells_2s") or 0)>=int((c.get("launch_metrics") or {}).get("buys_2s") or 0):
                reason="LAUNCH_FLOW_REVERSAL"

            # Strategy-specific hard stop is intentionally tighter than the user-visible
            # absolute stop-loss cap.
            elif net_ret<=-strategy_max_loss_pct(obj.strategy):
                reason="CAPITAL_SHIELD_STOP"

            # Sniper scratch is much faster than ordinary strategies.
            elif obj.strategy=="SNIPER_LONG" and held_minutes>=f("sniper_scratch_minutes") and \
                 peak_net_ret<0.40 and net_ret<=-f("sniper_scratch_loss_pct"):
                reason="SNIPER_SCRATCH_EXIT"

            # Strategy-specific recovery scratch: fail fast when there is no follow-through.
            elif b("capital_shield_enabled") and obj.strategy=="SCALP_LONG" and held_minutes>=3 and \
                 peak_net_ret<0.35 and net_ret<=-0.45:
                reason="SCALP_SCRATCH_EXIT"
            elif b("capital_shield_enabled") and obj.strategy=="PUMP_LONG" and held_minutes>=4 and \
                 peak_net_ret<0.50 and net_ret<=-0.70:
                reason="PUMP_SCRATCH_EXIT"
            elif b("capital_shield_enabled") and held_minutes>=f("scratch_after_minutes") and \
                 peak_net_ret<f("scratch_min_peak_pct") and net_ret<=-f("scratch_loss_pct"):
                reason="SCRATCH_EXIT"

            # Early break-even protection after modest net profit.
            elif b("capital_shield_enabled"):
                be_trigger,be_floor=strategy_breakeven_rule(obj.strategy)
                if peak_net_ret>=be_trigger and net_ret<=be_floor:
                    reason="EARLY_BREAK_EVEN"

            # Strategy-specific Profit Lock.
            if reason is None and b("profit_lock_enabled"):
                floor=strategy_profit_lock(obj.strategy,peak_net_ret)
                if floor is not None and net_ret<=floor:
                    reason="PROFIT_LOCK"

            if reason is None and held_minutes>=max_hold:
                reason="LAUNCH_TIME_EXIT" if obj.strategy=="LAUNCH_SNIPER" else "TIME_STOP"
            elif reason is None and str(obj.strategy).startswith("PERP_") and c.get("direction") in ("LONG","SHORT") and c.get("direction")!=side and nz(c.get("direction_edge"))>=f("reversal_exit_edge"):
                reason="SIGNAL_REVERSAL"
            elif reason is None and not str(obj.strategy).startswith("PERP_") and obj.strategy!="LAUNCH_SNIPER":
                shield_min_liq=max(f("min_liquidity"),f("capital_shield_min_liquidity")) if b("capital_shield_enabled") else f("min_liquidity")
                if c.get("liquidity",0)<shield_min_liq*.70:
                    reason="LIQUIDITY_EMERGENCY"
                elif prevliq>0 and c.get("liquidity",0)<prevliq*(.80 if b("capital_shield_enabled") else .65):
                    reason="LIQUIDITY_DROP"
            elif reason is None and obj.strategy=="SNIPER_LONG" and held_minutes>=0.50 and \
                 (nz(c.get("buy_pressure"))<50 or nz(c.get("m5"))<=-0.20):
                reason="SNIPER_MOMENTUM_FADE"
            elif reason is None and side=="LONG" and c["buy_pressure"]<24 and net_ret>0:
                reason="MOMENTUM_EXIT"
            elif reason is None and side=="SHORT" and c["buy_pressure"]>76 and net_ret>0:
                reason="SHORT_SQUEEZE_EXIT"

            if reason is None:
                tp1,tp2,tp3=strategy_tp_plan(obj.strategy)
                if net_ret>=tp1[0] and not obj.tp1:
                    partial_sell(obj,c,tp1[1]);obj.tp1=True
                if net_ret>=tp2[0] and not obj.tp2:
                    partial_sell(obj,c,tp2[1]);obj.tp2=True
                if net_ret>=tp3[0] and not obj.tp3:
                    if obj.strategy=="SNIPER_LONG":
                        reason="SNIPER_TAKE_PROFIT"
                    else:
                        partial_sell(obj,c,tp3[1]);obj.tp3=True

                trail=None
                if peak_net_ret>=60:trail=-14
                elif peak_net_ret>=35:trail=-11
                elif peak_net_ret>=20:trail=-8
                elif peak_net_ret>=12:trail=-6
                if trail is not None and pull_net<=trail:
                    reason="TRAILING_EXIT"

            s.commit()
            s.expunge(obj)

        if reason:
            close_position(obj,c,reason)
            if reason=="DAILY_TARGET_SECURE":
                runtime["daily_target_locked"]=True
                runtime["daily_target_lock_time"]=datetime.now(timezone.utc).isoformat()




def launch_diag_inc(key,amount=1):
    runtime["launch_diag"][key]=runtime["launch_diag"].get(key,0)+amount

def launch_entry_count_hour():
    now=time.time()
    runtime["launch_entry_times"]=[t for t in runtime["launch_entry_times"] if now-t<3600]
    return len(runtime["launch_entry_times"])

def launch_prune_histories():
    now=time.time()
    window=max(60,f("launch_creator_window_sec"))
    for creator,arr in list(runtime["launch_creator_history"].items()):
        arr=[t for t in arr if now-t<=window]
        if arr:runtime["launch_creator_history"][creator]=arr
        else:runtime["launch_creator_history"].pop(creator,None)
    for symbol,arr in list(runtime["launch_symbol_history"].items()):
        arr=[x for x in arr if now-nz(x.get("t"))<=window]
        if arr:runtime["launch_symbol_history"][symbol]=arr
        else:runtime["launch_symbol_history"].pop(symbol,None)

def register_launch_token(data):
    mint=str(data.get("mint") or data.get("tokenAddress") or "").strip()
    if len(mint)<30:return None
    now=time.time()
    launch_prune_histories()

    creator=str(
        data.get("traderPublicKey") or data.get("creator") or
        data.get("txSigner") or data.get("user") or ""
    )
    symbol=str(data.get("symbol") or "?").strip()[:40] or "?"
    name=str(data.get("name") or symbol).strip()[:120] or symbol

    ch=runtime["launch_creator_history"].setdefault(creator,[]) if creator else []
    creator_count=len(ch)
    creator_spam=bool(creator and creator_count>=i("launch_creator_max_tokens"))
    if creator:ch.append(now)

    sh=runtime["launch_symbol_history"].setdefault(symbol.upper(),[])
    duplicate_count=len({x.get("mint") for x in sh if x.get("mint")})
    duplicate_symbol=duplicate_count>=i("launch_duplicate_symbol_max")
    sh.append({"t":now,"mint":mint})

    mcap=None
    mcap_kind=None
    if data.get("marketCapSol") is not None:
        mcap=nz(data.get("marketCapSol"));mcap_kind="SOL"
    elif data.get("marketCapQuote") is not None:
        mcap=nz(data.get("marketCapQuote"));mcap_kind=str(data.get("quoteMint") or "QUOTE")

    initial_buy=nz(
        data.get("initialBuy") or data.get("initialBuySol") or
        data.get("solAmount") or data.get("amountSol")
    )

    info={
        "mint":mint,"symbol":symbol,"name":name,"creator":creator,
        "created_t":now,"initial_buy_sol":initial_buy,
        "create_mcap":mcap,"mcap_kind":mcap_kind,
        "creator_spam":creator_spam,
        "creator_token_count":creator_count+1 if creator else 0,
        "duplicate_symbol":duplicate_symbol,
        "duplicate_symbol_count":duplicate_count+1,
        "creator_sell":False,
        "trades":[],
        "last_event_t":now,
        "status":"WATCHING",
        "last_reason":"collecting first trades",
    }
    runtime["launch_watch"][mint]=info
    runtime["launch_new_tokens"]+=1
    launch_diag_inc("NEW_TOKEN")

    if creator_spam:launch_diag_inc("CREATOR_SPAM")
    if duplicate_symbol:launch_diag_inc("DUPLICATE_SYMBOL")

    return info

def add_launch_trade(data):
    mint=str(data.get("mint") or data.get("tokenAddress") or "").strip()
    info=runtime["launch_watch"].get(mint)
    if not info:return None

    tx=str(data.get("txType") or data.get("action") or data.get("type") or "").lower()
    if tx in ("create","migration","migrate"):
        return None

    now=time.time()
    buy=tx=="buy" or str(data.get("isBuy","")).lower()=="true"
    sell=tx=="sell" or str(data.get("isSell","")).lower()=="true"
    if not buy and not sell:
        return None

    trader=str(
        data.get("traderPublicKey") or data.get("txSigner") or
        data.get("trader") or data.get("user") or ""
    )
    sol_amt=abs(nz(data.get("solAmount") or data.get("sol_amount") or data.get("amountSol")))

    mcap=None;mcap_kind=None
    if data.get("marketCapSol") is not None:
        mcap=nz(data.get("marketCapSol"));mcap_kind="SOL"
    elif data.get("marketCapQuote") is not None:
        mcap=nz(data.get("marketCapQuote"));mcap_kind=str(data.get("quoteMint") or "QUOTE")

    event={
        "t":now,"buy":buy,"sell":sell,"trader":trader,"sol":sol_amt,
        "mcap":mcap,"mcap_kind":mcap_kind,
    }
    info["trades"].append(event)
    info["trades"]=[e for e in info["trades"] if now-e["t"]<=30]
    info["last_event_t"]=now

    if sell and info.get("creator") and trader==info.get("creator"):
        info["creator_sell"]=True
        launch_diag_inc("CREATOR_SELL")

    runtime["launch_trades_seen"]+=1
    runtime["launch_last_event"]=now
    return launch_metrics(mint)

def launch_metrics(mint):
    info=runtime["launch_watch"].get(mint)
    if not info:return None
    now=time.time()
    trades=[e for e in info.get("trades",[]) if now-e["t"]<=10]
    info["trades"]=trades

    def win(sec):
        return [e for e in trades if now-e["t"]<=sec]
    e1,e2,e5=win(1),win(2),win(5)
    b2=[e for e in e2 if e["buy"]]
    s2=[e for e in e2 if e["sell"]]
    b5=[e for e in e5 if e["buy"]]

    independent_buyers={
        e.get("trader") for e in b2
        if e.get("trader") and e.get("trader")!=info.get("creator")
    }
    independent_buyers5={
        e.get("trader") for e in b5
        if e.get("trader") and e.get("trader")!=info.get("creator")
    }

    # Beta prior prevents 1/1 from looking like a reliable 100% buy-pressure event.
    pressure=100*(len(b2)+2)/max(len(e2)+4,1)
    raw_pressure=100*len(b2)/max(len(e2),1)
    buy_sol=sum(e.get("sol",0) for e in b2)
    sell_sol=sum(e.get("sol",0) for e in s2)
    net_sol=buy_sol-sell_sol

    by_buyer={}
    for e in b2:
        t=e.get("trader") or "UNKNOWN"
        by_buyer[t]=by_buyer.get(t,0)+e.get("sol",0)
    top_share=100*max(by_buyer.values())/max(sum(by_buyer.values()),1e-9) if by_buyer else 100

    older=[e for e in e5 if now-e["t"]>1]
    rate_now=len(e1)
    old_rate=len(older)/4.0
    accel=clamp((rate_now/max(old_rate,.25))*20,0,100)

    current_mcap=None;current_kind=None
    for e in reversed(trades):
        if nz(e.get("mcap"))>0:
            current_mcap=nz(e.get("mcap"));current_kind=e.get("mcap_kind");break
    create_mcap=nz(info.get("create_mcap"))
    mcap_multiple=current_mcap/create_mcap if current_mcap and create_mcap and current_kind==info.get("mcap_kind") else None

    event_score=clamp(len(e2)/6*100,0,100)
    buyer_score=clamp(len(independent_buyers)/4*100,0,100)
    flow_score=clamp(32*math.log10(1+max(0,buy_sol)*10),0,100)
    momentum_score=50
    if mcap_multiple is not None:
        momentum_score=clamp(45+(mcap_multiple-1)*90,0,100)

    sample_conf=clamp((len(e2)/5)*.55+(len(independent_buyers)/3)*.45,0,1)
    raw_score=(
        pressure*.20 + event_score*.20 + buyer_score*.20 +
        flow_score*.15 + accel*.15 + momentum_score*.10
    )

    penalty=0
    if info.get("creator_spam"):penalty+=28
    if info.get("duplicate_symbol"):penalty+=22
    if top_share>f("launch_max_top_buyer_share_pct"):
        penalty+=min(30,(top_share-f("launch_max_top_buyer_share_pct"))*.8)
    if info.get("creator_sell"):penalty+=70

    score=clamp(raw_score*(.78+.22*sample_conf)-penalty,0,100)
    age=now-info["created_t"]

    m={
        "mint":mint,"symbol":info.get("symbol"),"name":info.get("name"),
        "creator":info.get("creator"),"age_sec":round(age,2),
        "score":round(score,1),"raw_score":round(raw_score,1),
        "sample_confidence":round(sample_conf*100,1),
        "events_1s":len(e1),"events_2s":len(e2),"events_5s":len(e5),
        "buys_2s":len(b2),"sells_2s":len(s2),
        "unique_buyers_2s":len(independent_buyers),
        "unique_buyers_5s":len(independent_buyers5),
        "buy_pressure_2s":round(pressure,1),
        "raw_buy_pressure_2s":round(raw_pressure,1),
        "buy_sol_2s":round(buy_sol,4),"sell_sol_2s":round(sell_sol,4),
        "net_sol_2s":round(net_sol,4),
        "top_buyer_share_pct":round(top_share,1),
        "acceleration_score":round(accel,1),
        "current_mcap":current_mcap,"mcap_kind":current_kind,
        "create_mcap":info.get("create_mcap"),
        "mcap_multiple":round(mcap_multiple,3) if mcap_multiple is not None else None,
        "creator_spam":bool(info.get("creator_spam")),
        "duplicate_symbol":bool(info.get("duplicate_symbol")),
        "creator_sell":bool(info.get("creator_sell")),
    }

    if runtime["launch_best"] is None or score>nz(runtime["launch_best"].get("score")) or \
       now-nz(runtime["launch_best"].get("_t"))>8:
        runtime["launch_best"]={**m,"_t":now}
    return m

def launch_gate(m):
    if not b("launch_sniper_enabled"):return False,"launch sniper off"
    if operating_mode()!="PAPER":return False,"launch sniper paper only"
    if b("killed") or not b("bot_enabled"):return False,"bot stopped"
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]:
        return False,"loss pause"

    gok,greason=strategy_governor_gate("LAUNCH_SNIPER")
    if not gok:return False,greason

    if survival_guard_status()["blocked"]:return False,"survival loss guard"
    if global_equity_guard_status()["blocked"]:return False,"global equity guard"
    if b("daily_target_lock_enabled") and daily_target_status()["locked"]:
        return False,"daily profit target locked"
    if open_risk_pct()>=f("max_total_open_risk_pct"):
        return False,"portfolio open risk cap"

    with SessionLocal() as s:
        if s.scalar(select(Position).where(Position.mint==m["mint"])):
            return False,"already open"
        rows=s.scalars(select(Position)).all()
        if len(rows)>=i("max_positions"):return False,"max positions"
        launch_open=sum(1 for p in rows if p.strategy=="LAUNCH_SNIPER")
        if launch_open>=i("launch_max_positions"):return False,"launch max positions"

    if launch_entry_count_hour()>=i("launch_max_entries_per_hour"):
        return False,"launch hourly limit"

    age=nz(m.get("age_sec"))
    if age<f("launch_min_age_sec"):return False,"launch too early"
    if age>f("launch_entry_max_age_sec"):return False,"launch too old"
    if m.get("creator_sell"):return False,"creator sold"
    if m.get("creator_spam"):return False,"creator spam"
    if m.get("duplicate_symbol"):return False,"duplicate ticker spam"
    if int(m.get("events_2s") or 0)<i("launch_min_events_2s"):return False,"launch low events"
    if int(m.get("unique_buyers_2s") or 0)<i("launch_min_unique_buyers_2s"):return False,"launch low buyers"
    if nz(m.get("buy_pressure_2s"))<f("launch_min_buy_pressure_2s"):return False,"launch low buy pressure"
    if nz(m.get("buy_sol_2s"))<f("launch_min_buy_sol_2s"):return False,"launch low buy flow"
    if nz(m.get("top_buyer_share_pct"),100)>f("launch_max_top_buyer_share_pct"):
        return False,"launch buyer concentration"
    if nz(m.get("score"))<f("launch_min_score"):return False,"launch low score"
    if not nz(m.get("current_mcap")) or not m.get("mcap_kind"):
        return False,"launch no price reference"

    mult=m.get("mcap_multiple")
    if mult is not None:
        if mult>f("launch_max_mcap_multiple"):return False,"launch late chase"
        if mult<f("launch_min_mcap_multiple"):return False,"launch already fading"

    return True,"ok"

def launch_confirmation_ready(m):
    mint=m["mint"];now=time.time()
    required=max(1,i("launch_confirmation_required"))
    if required<=1:return True
    prev=runtime["launch_confirmations"].get(mint)

    if not prev or now-nz(prev.get("first_t"))>f("launch_confirmation_window_sec"):
        runtime["launch_confirmations"][mint]={
            "first_t":now,"last_t":now,"count":1,
            "score":nz(m.get("score")),"buy_pressure":nz(m.get("buy_pressure_2s"))
        }
        launch_diag_inc("CONFIRM_WAIT")
        return False

    if now-nz(prev.get("last_t"))<max(.05,f("launch_confirmation_gap_ms")/1000):
        return False

    # A second print is only confirmation if the burst has not materially collapsed.
    if nz(m.get("score"))<nz(prev.get("score"))-7 or \
       nz(m.get("buy_pressure_2s"))<nz(prev.get("buy_pressure"))-12:
        runtime["launch_confirmations"][mint]={
            "first_t":now,"last_t":now,"count":1,
            "score":nz(m.get("score")),"buy_pressure":nz(m.get("buy_pressure_2s"))
        }
        launch_diag_inc("CONFIRM_RESET")
        return False

    prev["count"]+=1;prev["last_t"]=now
    prev["score"]=max(prev["score"],nz(m.get("score")))
    prev["buy_pressure"]=max(prev["buy_pressure"],nz(m.get("buy_pressure_2s")))
    runtime["launch_confirmations"][mint]=prev
    if prev["count"]>=required:
        runtime["launch_confirmations"].pop(mint,None)
        launch_diag_inc("CONFIRMED")
        return True
    return False

def launch_candidate(m,price=1.0):
    info=runtime["launch_watch"].get(m["mint"],{})
    # "liquidity" is deliberately not used as a real LP claim; this is a paper launch proxy.
    depth_proxy=max(150.0,nz(m.get("buy_sol_2s"))*sol_usd_reference()*6)
    return {
        "mint":m["mint"],"symbol":m.get("symbol") or "?","name":m.get("name") or "?",
        "price":max(1e-9,nz(price,1.0)),
        "liquidity":depth_proxy,
        "market_risk":nz(m.get("score")),
        "buy_pressure":nz(m.get("buy_pressure_2s"),50),
        "m5":0.0,
        "pump_score":0.0,"scalp_score":0.0,"sniper_score":0.0,
        "launch_score":nz(m.get("score")),
        "perp_eligible":False,
        "data_source":"PUMPPORTAL_LAUNCH",
        "volatility_regime":"EXTREME",
        "security":{
            "status":"LAUNCH_BEHAVIORAL",
            "score":max(0,min(100,nz(m.get("score")))),
            "hard_block":bool(m.get("creator_sell") or m.get("creator_spam") or m.get("duplicate_symbol")),
            "flags":[x for x,v in {
                "CREATOR_SELL":m.get("creator_sell"),
                "CREATOR_SPAM":m.get("creator_spam"),
                "DUPLICATE_TICKER":m.get("duplicate_symbol"),
            }.items() if v],
            "source":"FIRST_SECONDS_BEHAVIOR"
        },
        "launch_metrics":m,
        "launch_raw_move_pct":0.0,
    }

def launch_position_size(m):
    eq=max(0,nz(metrics().get("equity")))
    cap=eq*f("launch_max_position_pct")/100
    gov=governor_risk_multiplier("LAUNCH_SNIPER")
    # In probation this is intentionally tiny; after positive edge it can scale only to the cap.
    amount=min(cap*gov,f("cash"))
    return max(0,amount)

def open_launch_position(m):
    ok,reason=launch_gate(m)
    if not ok:return False,reason

    collateral=launch_position_size(m)
    if collateral<5:return False,"launch position too small"

    c=launch_candidate(m,1.0)
    est=launch_execution_cost_estimate(c,collateral)
    mid=1.0
    fill=simulated_fill_price(mid,"LONG","ENTRY",est.get("adverse_bps",0))
    fee=collateral*est.get("fee_bps",0)/10000
    cash_need=collateral+fee
    if cash_need>f("cash"):return False,"cash"

    setv("cash",f("cash")-cash_need)
    now_dt=datetime.now(timezone.utc)
    position_id=None
    with SessionLocal() as s:
        pos=Position(
            mint=m["mint"],symbol=m.get("symbol") or "?",name=m.get("name") or "?",
            strategy="LAUNCH_SNIPER",
            entry_price=fill,last_price=mid,peak_price=mid,
            initial_notional=collateral,remaining_cost=collateral,locked_pnl=-fee,
            opened_at=now_dt
        )
        s.add(pos);s.flush();position_id=pos.id
        s.add(PositionFeature(
            position_id=pos.id,strategy="LAUNCH_SNIPER",entry_score=nz(m.get("score")),
            long_score=0,short_score=0,pump_score=0,scalp_score=0,
            market_risk=nz(m.get("score")),direction_edge=0,
            funding_rate=0,open_interest_usd=0,
            volatility_regime="EXTREME",created_at=now_dt
        ))
        s.commit()

    if b("execution_simulator_enabled"):
        record_execution_event(position_id,None,c,"LAUNCH_SNIPER","ENTRY",collateral,mid,fill,est)

    # Store the event-native quote reference for immediate exit marking.
    runtime["realtime_exit_refs"][m["mint"]]={
        "position_id":position_id,
        "mcap_quote":nz(m.get("current_mcap")),
        "mcap_kind":m.get("mcap_kind"),
        "armed_at":time.time()
    }
    runtime["launch_marks"][m["mint"]]=c
    runtime["launch_entries"]+=1
    runtime["launch_entry_times"].append(time.time())
    runtime["launch_last_entry"]={
        "time":now_dt.isoformat(),"symbol":m.get("symbol"),"mint":m["mint"],
        "score":m.get("score"),"position_usd":round(collateral,2)
    }
    runtime["launch_watch"][m["mint"]]["status"]="IN_POSITION"
    runtime["launch_watch"][m["mint"]]["last_reason"]="launch entry opened"
    launch_diag_inc("ENTRY")
    record_event(
        "INFO","LAUNCH_SNIPER_ENTRY",
        f"{m.get('symbol')} first-seconds PAPER entry",
        {"score":m.get("score"),"age_sec":m.get("age_sec"),
         "events_2s":m.get("events_2s"),"buyers":m.get("unique_buyers_2s"),
         "pressure":m.get("buy_pressure_2s"),"position_usd":round(collateral,2)}
    )
    log_decision(c,"LAUNCH_SNIPER","OPENED","FIRST_SECONDS_ENTRY",
                 nz(m.get("score")),nz(m.get("score")),100,nz(m.get("score")))
    return True,"opened"

async def evaluate_launch_mint(mint,m):
    if not m or mint in runtime["launch_evaluating"]:return
    runtime["launch_evaluating"].add(mint)
    try:
        ok,reason=launch_gate(m)
        info=runtime["launch_watch"].get(mint)
        if info:
            info["last_reason"]=reason
            info["status"]="READY" if ok else "WATCHING"

        if not ok:
            launch_diag_inc(reason.upper().replace(" ","_"))
            return
        launch_diag_inc("GATE_PASS")

        if not launch_confirmation_ready(m):
            if info:info["status"]="CONFIRMING"
            return

        async with entry_lock:
            # Recompute from latest events immediately before entry.
            latest=launch_metrics(mint)
            if not latest:return
            ok2,reason2=launch_gate(latest)
            if not ok2:
                if info:
                    info["status"]="WATCHING";info["last_reason"]=reason2
                launch_diag_inc(reason2.upper().replace(" ","_"))
                return
            opened,oreason=open_launch_position(latest)
            if not opened:
                if info:
                    info["status"]="BLOCKED";info["last_reason"]=oreason
                launch_diag_inc(oreason.upper().replace(" ","_"))
    finally:
        runtime["launch_evaluating"].discard(mint)

def launch_status():
    now=time.time()
    rows=[]
    for mint,info in list(runtime["launch_watch"].items()):
        age=now-nz(info.get("created_t"))
        if age>180 and not open_position_for_mint(mint):
            runtime["launch_watch"].pop(mint,None)
            runtime["launch_marks"].pop(mint,None)
            continue
        m=launch_metrics(mint)
        if m:
            rows.append({
                **m,
                "status":info.get("status"),
                "reason":info.get("last_reason"),
            })
    rows.sort(key=lambda x:(nz(x.get("score")), -nz(x.get("age_sec"))),reverse=True)
    best=rows[0] if rows else runtime.get("launch_best")
    return {
        "enabled":b("launch_sniper_enabled"),
        "mode":"FIRST_SECONDS_PAPER",
        "new_tokens":runtime["launch_new_tokens"],
        "trades_seen":runtime["launch_trades_seen"],
        "watching":sum(1 for x in runtime["launch_watch"].values() if x.get("status")!="IN_POSITION"),
        "entries":runtime["launch_entries"],
        "entries_last_hour":launch_entry_count_hour(),
        "max_entries_per_hour":i("launch_max_entries_per_hour"),
        "last_event_age_sec":round(now-runtime["launch_last_event"],3) if runtime["launch_last_event"] else None,
        "best":best,
        "top":rows[:6],
        "diag":dict(runtime["launch_diag"]),
        "last_entry":runtime["launch_last_entry"],
        "rules":{
            "score":f("launch_min_score"),
            "events_2s":i("launch_min_events_2s"),
            "buyers_2s":i("launch_min_unique_buyers_2s"),
            "buy_pressure_2s":f("launch_min_buy_pressure_2s"),
            "min_buy_sol_2s":f("launch_min_buy_sol_2s"),
            "max_top_buyer_share":f("launch_max_top_buyer_share_pct"),
            "max_age_sec":f("launch_entry_max_age_sec")
        }
    }

def pulse_metrics(mint):
    now=time.time()
    buf=runtime["pulse_buffers"].get(mint,[])
    buf=[e for e in buf if now-e["t"]<=30]
    runtime["pulse_buffers"][mint]=buf

    def window(sec):
        return [e for e in buf if now-e["t"]<=sec]

    e2,e5,e10,e30=window(2),window(5),window(10),window(30)
    buys5=[e for e in e5 if e["buy"]]
    buys10=[e for e in e10 if e["buy"]]
    old=[e for e in e30 if now-e["t"]>5]

    raw_pressure=100*len(buys5)/max(len(e5),1)
    # Beta(2,2) prior stops 1/1 from pretending to be a high-confidence 100% signal.
    smoothed_pressure=100*(len(buys5)+2)/max(len(e5)+4,1)

    rate5=len(e5)/5.0
    old_rate=len(old)/25.0
    accel=clamp((rate5/max(old_rate,0.08))*18,0,100)
    frequency=clamp(len(e2)*18+len(e5)*4,0,100)
    unique_buyers=len({e.get("trader") for e in buys10 if e.get("trader")})
    unique_score=clamp(unique_buyers*15,0,100)
    buy_sol5=sum(e.get("sol",0) for e in buys5)
    flow_score=clamp(20*math.log10(max(buy_sol5,0.01)*10+1),0,100)

    raw_score=clamp(
        smoothed_pressure*.27 + accel*.24 + frequency*.20 +
        unique_score*.17 + flow_score*.12,0,100
    )

    event_conf=clamp(len(e5)/5.0,0,1)
    buyer_conf=clamp(unique_buyers/3.0,0,1)
    sample_confidence=clamp(event_conf*.60+buyer_conf*.40,0,1)
    confidence_factor=.75+.25*sample_confidence
    score=clamp(raw_score*confidence_factor,0,100)

    last=buf[-1] if buf else {}
    return {
        "mint":mint,
        "score":round(score,1),
        "raw_score":round(raw_score,1),
        "sample_confidence":round(sample_confidence*100,1),
        "events_2s":len(e2),"events_5s":len(e5),"events_10s":len(e10),
        "buy_pressure_5s":round(smoothed_pressure,1),
        "raw_buy_pressure_5s":round(raw_pressure,1),
        "unique_buyers_10s":unique_buyers,
        "buy_sol_5s":round(buy_sol5,4),
        "acceleration_score":round(accel,1),
        "frequency_score":round(frequency,1),
        "last_mcap_quote":last.get("mcap_quote"),
        "last_mcap_kind":last.get("mcap_kind"),
        "last_quote_price":last.get("quote_price"),
        "last_pool":last.get("pool"),
        "age_sec":round(now-last["t"],2) if last else None
    }

def add_pulse_event(data):
    mint=str(data.get("mint") or data.get("tokenAddress") or "").strip()
    if len(mint)<30:return None
    now=time.time()
    tx=str(data.get("txType") or data.get("type") or "").lower()
    buy=tx in ("buy","create") or str(data.get("isBuy","")).lower()=="true"
    sol_amt=abs(nz(data.get("solAmount") or data.get("sol_amount") or data.get("amountSol")))
    trader=str(data.get("traderPublicKey") or data.get("txSigner") or data.get("trader") or data.get("user") or "")

    mcap_kind=None
    mcap_quote=None
    if data.get("marketCapSol") is not None:
        mcap_kind="SOL"
        mcap_quote=nz(data.get("marketCapSol"))
    elif data.get("marketCapQuote") is not None:
        mcap_kind=str(data.get("quoteMint") or "QUOTE")
        mcap_quote=nz(data.get("marketCapQuote"))

    quote_price=nz(data.get("price")) if data.get("price") is not None else None
    pool=str(data.get("pool") or "")

    buf=runtime["pulse_buffers"].setdefault(mint,[])
    buf.append({
        "t":now,"buy":buy,"sol":sol_amt,"trader":trader,
        "mcap_quote":mcap_quote,"mcap_kind":mcap_kind,
        "quote_price":quote_price,"pool":pool
    })
    runtime["pulse_buffers"][mint]=[e for e in buf if now-e["t"]<=30]
    runtime["pulse_last_event"]=now
    runtime["pulse_events_total"]+=1
    m=pulse_metrics(mint)

    if int(m.get("events_5s") or 0)>=2 or nz(m.get("score"))>=35:
        runtime["pulse_recent_best"][mint]={**m,"seen_at":now}

    effective=pulse_effective_thresholds()
    if m["score"]>=effective["score"]-10:
        runtime["pulse_hot"][mint]={**m,"seen_at":now}

    return m


def open_position_for_mint(mint):
    with SessionLocal() as s:
        p=s.scalar(select(Position).where(Position.mint==mint))
        if p:
            s.expunge(p)
    return p

def arm_realtime_exit_ref(mint,position_id=None):
    m=pulse_metrics(mint)
    mcap=nz(m.get("last_mcap_quote"))
    kind=m.get("last_mcap_kind")
    if mcap>0 and kind:
        runtime["realtime_exit_refs"][mint]={
            "position_id":position_id,
            "mcap_quote":mcap,
            "mcap_kind":kind,
            "armed_at":time.time()
        }

async def realtime_exit_check(mint,m):
    """Event-driven exit wakeup for an already-open spot position."""
    if not b("realtime_exit_enabled"):
        return
    p=open_position_for_mint(mint)
    if not p:
        return

    now=time.time()
    min_gap=max(.10,f("realtime_exit_min_interval_ms")/1000.0)
    if now-runtime["realtime_exit_last_eval"].get(mint,0)<min_gap:
        return
    runtime["realtime_exit_last_eval"][mint]=now
    runtime["realtime_exit_last_event"]=now
    runtime["realtime_exit_checks"]+=1

    try:
        ref=runtime["realtime_exit_refs"].get(mint)

        # V6.6: first-seconds positions are marked directly from the same PumpPortal trade stream.
        if p.strategy=="LAUNCH_SNIPER":
            lm=launch_metrics(mint)
            if lm and ref:
                cur_mcap=nz(lm.get("current_mcap"))
                cur_kind=lm.get("mcap_kind")
                if cur_mcap>0 and cur_kind==ref.get("mcap_kind") and nz(ref.get("mcap_quote"))>0:
                    ratio=cur_mcap/nz(ref.get("mcap_quote"))
                    if 0.05<ratio<20:
                        lc=launch_candidate(lm,ratio)
                        lc["launch_raw_move_pct"]=(ratio-1)*100
                        runtime["launch_marks"][mint]=lc
                        runtime["realtime_exit_direct_marks"]+=1
                        await manage_positions_safe()
                        return

        cur_mcap=nz(m.get("last_mcap_quote"))
        cur_kind=m.get("last_mcap_kind")

        # Fast path: relative market-cap movement is a same-quote price proxy.
        if ref and cur_mcap>0 and cur_kind==ref.get("mcap_kind") and ref.get("mcap_quote",0)>0:
            ratio=cur_mcap/ref["mcap_quote"]
            if 0.05<ratio<20:
                c0=next((x for x in runtime.get("candidates",[]) if x.get("mint")==mint),None)
                if c0:
                    synthetic=dict(c0)
                    synthetic["price"]=p.entry_price*ratio
                    synthetic["realtime_mark"]=True
                    synthetic["realtime_ratio"]=ratio
                    synthetic["pulse"]=m
                    merge_position_updates([synthetic])
                    runtime["realtime_exit_direct_marks"]+=1
                    await manage_positions_safe()
                    return

        # If no compatible direct mark exists, the websocket event still wakes an
        # immediate REST refresh instead of waiting for the 4-second fallback loop.
        rest_key=mint+":rest"
        rest_gap=max(.35,f("realtime_exit_rest_fallback_ms")/1000.0)
        if now-runtime["realtime_exit_last_eval"].get(rest_key,0)>=rest_gap:
            runtime["realtime_exit_last_eval"][rest_key]=now
            runtime["realtime_exit_rest_checks"]+=1
            async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Realtime-Exit/6.6"}) as client:
                rows=await fetch_pairs(client,[mint],{})
                if rows:
                    rows[0]["position_watch"]=True
                    merge_position_updates(rows)
                    runtime["position_price_seen"][mint]=time.time()
                    await manage_positions_safe()

        # Arm a quote reference as soon as a compatible event becomes available.
        if not ref and cur_mcap>0 and cur_kind:
            arm_realtime_exit_ref(mint,p.id)

    except Exception as e:
        runtime["realtime_exit_error"]=str(e)[:200]
        record_event(
            "ERROR","REALTIME_EXIT_ERROR",runtime["realtime_exit_error"],
            {"mint":mint},dedupe_sec=120
        )

async def sync_pulse_subscriptions(ws):
    """Prioritize open positions and prune stale metered trade subscriptions."""
    while True:
        try:
            now=time.time()
            open_mints=set(open_spot_mints())

            missing=[m for m in open_mints if m not in runtime["pulse_subscribed"]]
            if missing:
                await ws.send(json.dumps({"method":"subscribeTokenTrade","keys":missing}))
                runtime["pulse_subscribed"].update(missing)
                for m in missing:
                    runtime["pulse_subscription_birth"][m]=now

            ttl=max(30,f("pulse_subscription_ttl_sec"))
            stale=[]
            for mint,born in list(runtime["pulse_subscription_birth"].items()):
                if mint in open_mints:
                    continue

                linfo=runtime["launch_watch"].get(mint)
                if linfo and now-nz(linfo.get("created_t"))>f("launch_watch_ttl_sec"):
                    if linfo.get("status") not in ("IN_POSITION","CLOSED"):
                        linfo["status"]="EXPIRED"
                        linfo["last_reason"]="first-seconds window expired"
                    stale.append(mint)
                    continue

                hot=runtime["pulse_hot"].get(mint,{})
                hot_recent=now-nz(hot.get("seen_at"))<30
                if not hot_recent and now-born>ttl:
                    stale.append(mint)

            if stale:
                batch=stale[:60]
                await ws.send(json.dumps({"method":"unsubscribeTokenTrade","keys":batch}))
                for m in batch:
                    runtime["pulse_subscribed"].discard(m)
                    runtime["pulse_subscription_birth"].pop(m,None)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            runtime["pulse_stream_error"]=f"subscription sync: {str(e)[:150]}"

        await asyncio.sleep(1)


def pulse_effective_thresholds():
    shifts=runtime["pulse_adaptive_shifts"]

    # Recovery baseline is deliberately more tradable than old persisted values,
    # while still bounded by hard floors + confirmation + final safety gates.
    base_score=min(f("pulse_min_score"),f("pulse_recovery_score"))
    base_events=min(i("pulse_min_events_5s"),i("pulse_recovery_events_5s"))
    base_pressure=min(f("pulse_min_buy_pressure_5s"),f("pulse_recovery_buy_pressure"))
    base_buyers=min(i("pulse_min_unique_buyers_10s"),i("pulse_recovery_unique_buyers"))

    score=clamp(base_score+nz(shifts.get("score")),f("pulse_score_floor"),f("pulse_score_ceiling"))
    events=max(i("pulse_events_floor"),min(12,int(round(base_events+nz(shifts.get("events"))))))
    pressure=clamp(base_pressure+nz(shifts.get("pressure")),f("pulse_buy_pressure_floor"),f("pulse_buy_pressure_ceiling"))
    buyers=max(i("pulse_unique_buyers_floor"),min(i("pulse_unique_buyers_ceiling"),
               int(round(base_buyers+nz(shifts.get("buyers"))))))

    return {
        "score":round(score,1),
        "events_5s":events,
        "buy_pressure_5s":round(pressure,1),
        "unique_buyers_10s":buyers,
        "base":{
            "score":base_score,
            "events_5s":base_events,
            "buy_pressure_5s":base_pressure,
            "unique_buyers_10s":base_buyers
        }
    }

def pulse_diag_record(reason,mint=None,m=None,detail=None,dedupe_sec=3):
    now=time.time()
    mint=str(mint or (m or {}).get("mint") or "")
    key=f"{reason}:{mint}"
    prev=runtime["pulse_diag_dedupe"].get(key,0)
    if now-prev<dedupe_sec:
        return
    runtime["pulse_diag_dedupe"][key]=now

    event={
        "t":now,"reason":reason,"mint":mint,
        "score":nz((m or {}).get("score")),
        "events_5s":int((m or {}).get("events_5s") or 0),
        "buy_pressure_5s":nz((m or {}).get("buy_pressure_5s")),
        "unique_buyers_10s":int((m or {}).get("unique_buyers_10s") or 0),
        "acceleration_score":nz((m or {}).get("acceleration_score")),
        "detail":detail or {}
    }
    runtime["pulse_diag_events"].append(event)

    window=max(300,i("pulse_diag_window_sec")*3)
    runtime["pulse_diag_events"]=[
        e for e in runtime["pulse_diag_events"]
        if now-e["t"]<=window
    ][-5000:]

def pulse_diag_gate_reason(reason):
    r=str(reason or "").lower()
    if "liquidity" in r:return "LOW_LIQUIDITY"
    if "market quality" in r or "market risk" in r:return "LOW_MARKET_QUALITY"
    if "security" in r:return "SECURITY_BLOCK"
    if "execution" in r or "route" in r:return "EXECUTION_BLOCK"
    if "portfolio" in r or "correlation" in r:return "PORTFOLIO_BLOCK"
    if "late chase" in r or "move too extreme" in r:return "LATE_CHASE"
    if "daily" in r or "equity guard" in r:return "RISK_GUARD"
    if "cooldown" in r:return "COOLDOWN"
    return "FINAL_GATE_BLOCK"

def pulse_diag_summary(window_sec=None):
    now=time.time()
    window=max(60,int(window_sec or i("pulse_diag_window_sec")))
    events=[e for e in runtime["pulse_diag_events"] if now-e["t"]<=window]
    counts={}
    for e in events:
        counts[e["reason"]]=counts.get(e["reason"],0)+1

    recent_best=[]
    for mint,item in list(runtime["pulse_recent_best"].items()):
        if now-nz(item.get("seen_at"))<=60:
            recent_best.append(item)
        elif now-nz(item.get("seen_at"))>180:
            runtime["pulse_recent_best"].pop(mint,None)

    recent_best.sort(key=lambda x:(nz(x.get("score")),nz(x.get("acceleration_score"))),reverse=True)
    best=recent_best[0] if recent_best else None

    return {
        "window_sec":window,
        "observed_bursts":counts.get("OBSERVED",0),
        "counts":counts,
        "front_gate_pass":counts.get("PULSE_GATE_PASS",0),
        "final_pass":counts.get("FINAL_GATE_PASS",0),
        "entries":counts.get("ENTRY",0),
        "best_live":best,
        "effective_thresholds":pulse_effective_thresholds(),
        "adaptive_enabled":b("adaptive_pulse_enabled"),
        "adaptive_shifts":dict(runtime["pulse_adaptive_shifts"]),
        "last_adapt_age_sec":round(now-runtime["pulse_adaptive_last"],1) if runtime["pulse_adaptive_last"] else None,
        "actions":runtime["pulse_adaptive_actions"][-5:]
    }

def pulse_recent_sniper_performance(limit=20):
    with SessionLocal() as s:
        rows=s.scalars(
            select(Trade).where(Trade.strategy=="SNIPER_LONG").order_by(Trade.id.desc()).limit(limit)
        ).all()
    if not rows:
        return {"count":0,"last_trade_id":0,"win_rate":0.0,"net_pnl":0.0}
    wins=sum(1 for t in rows if nz(t.pnl)>0)
    return {
        "count":len(rows),
        "last_trade_id":max(t.id for t in rows),
        "win_rate":100*wins/max(len(rows),1),
        "net_pnl":sum(nz(t.pnl) for t in rows)
    }

def pulse_adapt_action(message):
    runtime["pulse_adaptive_actions"].append({
        "time":datetime.now(timezone.utc).isoformat(),
        "message":message,
        "thresholds":pulse_effective_thresholds()
    })
    runtime["pulse_adaptive_actions"]=runtime["pulse_adaptive_actions"][-20:]
    record_event("INFO","PULSE_ADAPT",message,{"thresholds":pulse_effective_thresholds()},dedupe_sec=20)

def maybe_adapt_pulse_thresholds():
    if not b("adaptive_pulse_enabled"):
        return
    now=time.time()
    if now-runtime["pulse_adaptive_last"]<max(30,i("pulse_adapt_interval_sec")):
        return
    runtime["pulse_adaptive_last"]=now

    summary=pulse_diag_summary()
    counts=summary["counts"]
    observed=summary["observed_bursts"]
    min_obs=i("pulse_adapt_min_observations")
    shifts=runtime["pulse_adaptive_shifts"]
    changed=False
    message=None

    perf=pulse_recent_sniper_performance()
    if perf["count"]>=5 and perf["last_trade_id"]>runtime["pulse_last_perf_trade_id"]:
        runtime["pulse_last_perf_trade_id"]=perf["last_trade_id"]
        if perf["net_pnl"]<0 or perf["win_rate"]<40:
            shifts["score"]=min(8,nz(shifts.get("score"))+2)
            shifts["pressure"]=min(8,nz(shifts.get("pressure"))+1)
            changed=True
            message=f"Performance guard tightened Pulse: {perf['count']} trades, win {perf['win_rate']:.1f}%."

    if not changed and observed>=min_obs:
        passes=counts.get("PULSE_GATE_PASS",0)
        pass_rate=passes/max(observed,1)
        th=pulse_effective_thresholds()

        if pass_rate<0.02:
            # Structure first: events -> independent buyers -> pressure -> score.
            if counts.get("LOW_EVENTS",0)>0 and th["events_5s"]>i("pulse_events_floor"):
                shifts["events"]=nz(shifts.get("events"))-1
                changed=True;message="Adaptive Pulse relaxed 5s event count by 1."
            elif counts.get("LOW_UNIQUE_BUYERS",0)>0 and th["unique_buyers_10s"]>i("pulse_unique_buyers_floor"):
                shifts["buyers"]=nz(shifts.get("buyers"))-1
                changed=True;message="Adaptive Pulse relaxed unique buyers by 1."
            elif counts.get("LOW_BUY_PRESSURE",0)>0 and th["buy_pressure_5s"]>f("pulse_buy_pressure_floor"):
                shifts["pressure"]=nz(shifts.get("pressure"))-1
                changed=True;message="Adaptive Pulse relaxed buy pressure by 1 point."
            elif counts.get("LOW_SCORE",0)>0 and th["score"]>f("pulse_score_floor"):
                shifts["score"]=nz(shifts.get("score"))-1
                changed=True;message="Adaptive Pulse relaxed score by 1 point."

        elif pass_rate>0.18 and passes>=8 and th["score"]<f("pulse_score_ceiling"):
            shifts["score"]=nz(shifts.get("score"))+1
            changed=True;message="Adaptive Pulse tightened score because too many bursts passed."

    if changed and message:
        pulse_adapt_action(message)

def pulse_gate_metrics(m):
    if not b("realtime_pulse_enabled"):
        return False,"pulse disabled"

    # A real burst must have enough independent activity to be statistically meaningful.
    real_burst=int(m.get("events_5s") or 0)>=3 and int(m.get("unique_buyers_10s") or 0)>=2
    if real_burst:
        pulse_diag_record("OBSERVED",m=m,dedupe_sec=3)

    th=pulse_effective_thresholds()
    failures=[]

    if int(m.get("events_5s") or 0)<th["events_5s"]:
        failures.append(("LOW_EVENTS","pulse event count"))
    if int(m.get("unique_buyers_10s") or 0)<th["unique_buyers_10s"]:
        failures.append(("LOW_UNIQUE_BUYERS","pulse unique buyers"))
    if nz(m.get("buy_pressure_5s"))<th["buy_pressure_5s"]:
        failures.append(("LOW_BUY_PRESSURE","pulse buy pressure"))
    if nz(m.get("score"))<th["score"]:
        failures.append(("LOW_SCORE","pulse score"))

    if real_burst:
        for diag_reason,_ in failures:
            pulse_diag_record(diag_reason,m=m,dedupe_sec=3)

    if failures:
        maybe_adapt_pulse_thresholds()
        return False,failures[0][1]

    pulse_diag_record("PULSE_GATE_PASS",m=m,dedupe_sec=5)
    maybe_adapt_pulse_thresholds()
    return True,"ok"


def pulse_confirmation_ready(mint,m):
    if not b("realtime_pulse_enabled"):
        return False
    now=time.time()
    required=max(1,i("pulse_confirmation_required"))
    if required<=1:return True

    prev=runtime["pulse_confirmations"].get(mint)
    if not prev or now-nz(prev.get("first_t"))>f("pulse_confirmation_window_sec"):
        runtime["pulse_confirmations"][mint]={
            "first_t":now,"last_t":now,"count":1,
            "score":nz(m.get("score")),"events":int(m.get("events_5s") or 0)
        }
        pulse_diag_record("CONFIRM_WAIT",mint=mint,m=m,dedupe_sec=2)
        return False

    min_gap=max(.1,f("pulse_confirmation_min_gap_ms")/1000.0)
    if now-nz(prev.get("last_t"))<min_gap:
        return False

    # Confirmation must not be a collapsing burst.
    if nz(m.get("score")) < nz(prev.get("score"))-6:
        runtime["pulse_confirmations"][mint]={
            "first_t":now,"last_t":now,"count":1,
            "score":nz(m.get("score")),"events":int(m.get("events_5s") or 0)
        }
        pulse_diag_record("CONFIRM_RESET",mint=mint,m=m,dedupe_sec=2)
        return False

    prev["count"]=int(prev.get("count") or 1)+1
    prev["last_t"]=now
    prev["score"]=max(nz(prev.get("score")),nz(m.get("score")))
    prev["events"]=max(int(prev.get("events") or 0),int(m.get("events_5s") or 0))
    runtime["pulse_confirmations"][mint]=prev

    if prev["count"]>=required:
        runtime["pulse_confirmations"].pop(mint,None)
        pulse_diag_record("CONFIRMED",mint=mint,m=m,dedupe_sec=5)
        return True
    return False

async def evaluate_pulse_mint(mint,m):
    if mint in runtime["pulse_evaluating"]:return
    runtime["pulse_evaluating"].add(mint)
    try:
        ok,reason=pulse_gate_metrics(m)
        if not ok:return
        if not pulse_confirmation_ready(mint,m):
            return

        # Prevent repeated evaluation/open attempts on the same burst.
        hot=runtime["pulse_hot"].get(mint,{})
        last_eval=nz(hot.get("last_eval"))
        if last_eval and time.time()-last_eval<f("pulse_cooldown_sec"):
            return
        hot["last_eval"]=time.time()
        runtime["pulse_hot"][mint]=hot

        timeout=max(2,min(10,f("pulse_eval_timeout_sec")))
        async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Pulse-Sniper/6.6"}) as client:
            rows=await asyncio.wait_for(fetch_pairs(client,[mint],{}),timeout=timeout)
            if not rows:
                pulse_diag_record("NO_DEX_PAIR",mint=mint,m=m,dedupe_sec=10)
                return
            c=rows[0]
            if mint in runtime["pulse_recent_best"]:
                runtime["pulse_recent_best"][mint]["symbol"]=c.get("symbol")
                runtime["pulse_recent_best"][mint]["liquidity"]=c.get("liquidity")
                runtime["pulse_recent_best"][mint]["m5"]=c.get("m5")
                runtime["pulse_recent_best"][mint]["market_quality"]=c.get("market_risk")

            # The real-time pulse supplements — never bypasses — price/liquidity/security gates.
            c["pulse"]=m
            c["pulse_score"]=m["score"]
            c["sniper_score"]=max(nz(c.get("sniper_score")),nz(m.get("score")))
            c["best_strategy"]="SNIPER_LONG"

            if nz(c.get("m5"))>f("pulse_max_m5_pct"):
                pulse_diag_record("LATE_CHASE",mint=mint,m=m,detail={"m5":c.get("m5")},dedupe_sec=10)
                log_decision(c,"SNIPER_LONG","BLOCKED","pulse late chase",
                             c["sniper_score"],0,0,nz((c.get("security") or {}).get("score"),50))
                return

            # Scan security if not already cached. PAPER tolerates UNKNOWN, SHADOW remains strict.
            cached=runtime["token_security"].get(mint)
            if cached:
                c["security"]={k:v for k,v in cached.items() if k!="_ts"}
            else:
                c["security"]=await scan_token_security(client,c)

            merge_position_updates([c])

            async with entry_lock:
                gate_ok,gate_reason=sniper_gate(c)
                quality,route=signal_quality(c,"SNIPER_LONG",c["sniper_score"])
                sec_score=nz((c.get("security") or {}).get("score"),50)
                if not gate_ok:
                    pulse_diag_record(pulse_diag_gate_reason(gate_reason),mint=mint,m=m,
                                      detail={"reason":gate_reason},dedupe_sec=10)
                    log_decision(c,"SNIPER_LONG","BLOCKED",gate_reason,
                                 c["sniper_score"],quality,route.get("quality",0),sec_score)
                    return

                pulse_diag_record("FINAL_GATE_PASS",mint=mint,m=m,dedupe_sec=10)
                opened,oreason=open_position(c,"SNIPER_LONG")
                if opened:
                    runtime["pulse_entries"]+=1
                    runtime["sniper_entries"]+=1
                    pulse_diag_record("ENTRY",mint=mint,m=m,detail={"symbol":c.get("symbol")},dedupe_sec=10)
                    log_decision(c,"SNIPER_LONG","OPENED","REALTIME_PULSE_ENTRY",
                                 c["sniper_score"],quality,route.get("quality",0),sec_score)
                    record_event("INFO","REALTIME_PULSE_ENTRY",
                                 f"{c.get('symbol')} real-time pulse sniper opened",
                                 {"pulse":m,"m5":c.get("m5"),"liquidity":c.get("liquidity")})
                else:
                    pulse_diag_record(pulse_diag_gate_reason(oreason),mint=mint,m=m,
                                      detail={"reason":oreason},dedupe_sec=10)
                    log_decision(c,"SNIPER_LONG","BLOCKED",oreason,
                                 c["sniper_score"],quality,route.get("quality",0),sec_score)
    except asyncio.TimeoutError:
        runtime["pulse_stream_error"]="pulse candidate evaluation timeout"
    except Exception as e:
        runtime["pulse_stream_error"]=f"pulse eval: {str(e)[:180]}"
    finally:
        runtime["pulse_evaluating"].discard(mint)

async def pumpportal_realtime_loop():
    if not PUMPPORTAL_API_KEY:
        runtime["pulse_stream_mode"]="FALLBACK_ONLY"
        runtime["pulse_stream_error"]="PUMPPORTAL_API_KEY not configured"
        return
    if not PUMPPORTAL_TRADE_STREAM_ENABLED:
        runtime["pulse_stream_mode"]="KEY_READY_TRADE_STREAM_OFF"
        runtime["pulse_stream_error"]="Trade stream disabled by environment setting"
        return

    uri=f"{PUMPPORTAL_WS_BASE}?api-key={PUMPPORTAL_API_KEY}"
    backoff=2

    while True:
        sync_task=None
        try:
            async with websockets.connect(
                uri,ping_interval=20,ping_timeout=20,close_timeout=5,max_size=2_000_000
            ) as ws:
                runtime["pulse_stream_connected"]=True
                runtime["pulse_stream_mode"]="PUMPPORTAL_REALTIME"
                runtime["pulse_stream_error"]=None
                backoff=2

                await ws.send(json.dumps({"method":"subscribeNewToken"}))
                await ws.send(json.dumps({"method":"subscribeMigration"}))

                now=time.time()
                seeds=[
                    c.get("mint") for c in runtime.get("candidates",[])
                    if not c.get("perp_eligible") and c.get("mint")
                ][:30]
                seeds=list(dict.fromkeys(open_spot_mints()+seeds))

                if seeds:
                    await ws.send(json.dumps({"method":"subscribeTokenTrade","keys":seeds}))
                    runtime["pulse_subscribed"].update(seeds)
                    for m in seeds:
                        runtime["pulse_subscription_birth"][m]=now

                sync_task=asyncio.create_task(sync_pulse_subscriptions(ws))

                async for raw in ws:
                    try:
                        data=json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(data,dict):
                        continue

                    mint=str(data.get("mint") or data.get("tokenAddress") or "").strip()
                    if len(mint)<30:
                        continue

                    tx=str(data.get("txType") or data.get("action") or data.get("type") or "").lower()

                    is_create=(tx=="create") or bool(data.get("name") and data.get("symbol"))

                    if is_create:
                        register_launch_token(data)

                        # Fresh launches get priority inside a small rolling first-seconds universe.
                        active=[
                            (m0,info) for m0,info in runtime["launch_watch"].items()
                            if time.time()-nz(info.get("created_t"))<=f("launch_watch_ttl_sec")
                            and not open_position_for_mint(m0)
                        ]
                        if len(active)>i("launch_max_active_watch"):
                            active.sort(key=lambda x:nz(x[1].get("created_t")))
                            evict=[x[0] for x in active[:len(active)-i("launch_max_active_watch")]]
                            if evict:
                                await ws.send(json.dumps({"method":"unsubscribeTokenTrade","keys":evict}))
                                for em in evict:
                                    runtime["pulse_subscribed"].discard(em)
                                    runtime["pulse_subscription_birth"].pop(em,None)
                                    if em in runtime["launch_watch"]:
                                        runtime["launch_watch"][em]["status"]="EXPIRED"
                                        runtime["launch_watch"][em]["last_reason"]="launch watch capacity"

                        cap=max(30,i("pulse_max_trade_subscriptions"))
                        if mint not in runtime["pulse_subscribed"] and len(runtime["pulse_subscribed"])<cap:
                            await ws.send(json.dumps({"method":"subscribeTokenTrade","keys":[mint]}))
                            runtime["pulse_subscribed"].add(mint)
                            runtime["pulse_subscription_birth"][mint]=time.time()

                    lm=add_launch_trade(data)
                    if lm:
                        asyncio.create_task(evaluate_launch_mint(mint,lm))

                    m=add_pulse_event(data)
                    if not m:
                        continue

                    # Open spot/launch positions are managed from each incoming trade event.
                    if open_position_for_mint(mint):
                        asyncio.create_task(realtime_exit_check(mint,m))

                    ok,_=pulse_gate_metrics(m)
                    if ok:
                        asyncio.create_task(evaluate_pulse_mint(mint,m))

                    if len(runtime["pulse_subscribed"])>max(40,i("pulse_max_trade_subscriptions"))+20:
                        raise RuntimeError("subscription cap recycle")

        except Exception as e:
            runtime["pulse_stream_connected"]=False
            runtime["pulse_stream_error"]=str(e)[:220]
            record_event(
                "WARN","PULSE_STREAM_RECONNECT","Real-time pulse stream reconnecting",
                {"error":runtime["pulse_stream_error"]},dedupe_sec=60
            )
            await asyncio.sleep(backoff)
            backoff=min(backoff*2,30)
        finally:
            runtime["pulse_stream_connected"]=False
            if sync_task:
                sync_task.cancel()
                try:
                    await sync_task
                except BaseException:
                    pass

            if len(runtime["pulse_subscribed"])>max(40,i("pulse_max_trade_subscriptions"))+20:
                runtime["pulse_subscribed"].clear()
                runtime["pulse_subscription_birth"].clear()

def sniper_gate(c):
    if not b("sniper_enabled"):
        return False,"sniper disabled"
    if c.get("perp_eligible"):
        return False,"sniper spot only"
    required=effective_threshold("SNIPER_LONG",c)
    effective_score=max(nz(c.get("sniper_score")),nz(c.get("pulse_score")))
    if effective_score < required:
        return False,"sniper score"
    if nz(c.get("buy_pressure")) < f("sniper_min_buy_pressure"):
        return False,"sniper buy pressure"
    if nz(c.get("volume_accel")) < f("sniper_min_volume_accel"):
        return False,"sniper volume acceleration"
    m5=nz(c.get("m5"))
    if m5 < f("sniper_min_m5_pct"):
        return False,"sniper momentum too weak"
    if m5 > f("sniper_max_m5_pct"):
        return False,"sniper late chase"
    if nz(c.get("age_minutes"),999999) > f("sniper_max_age_minutes"):
        return False,"sniper market too old"

    with SessionLocal() as s:
        n=len(s.scalars(select(Position).where(Position.strategy=="SNIPER_LONG")).all())
        if n>=i("sniper_max_positions"):
            return False,"sniper max positions"

    return gate(c,"SNIPER_LONG")

async def choose_sniper_entry():
    if not b("bot_enabled") or b("killed") or not b("sniper_enabled"):
        return
    async with entry_lock:
        ranked=sorted(
            runtime.get("sniper_candidates",[]),
            key=lambda c:(nz(c.get("sniper_score")),nz(c.get("volume_accel")),nz(c.get("buy_pressure"))),
            reverse=True
        )
        for c in ranked:
            ok,reason=sniper_gate(c)
            signal=nz(c.get("sniper_score"))
            quality,route=signal_quality(c,"SNIPER_LONG",signal)
            sec=c.get("security") or {}
            sec_score=nz(sec.get("score"),50)

            if not ok:
                log_decision(c,"SNIPER_LONG","BLOCKED",reason,signal,quality,route.get("quality",0),sec_score)
                continue

            opened,oreason=open_position(c,"SNIPER_LONG")
            if opened:
                runtime["sniper_entries"]+=1
                log_decision(c,"SNIPER_LONG","OPENED","sniper entry accepted",signal,quality,route.get("quality",0),sec_score)
                record_event("INFO","SNIPER_ENTRY",f"{c.get('symbol')} SNIPER_LONG opened",
                             {"score":signal,"quality":quality,"m5":c.get("m5"),
                              "buy_pressure":c.get("buy_pressure"),"volume_accel":c.get("volume_accel")})
                return
            log_decision(c,"SNIPER_LONG","BLOCKED",oreason,signal,quality,route.get("quality",0),sec_score)

async def choose_entry_safe():
    async with entry_lock:
        choose_entry()

def choose_entry():
    if not b("bot_enabled") or b("killed"):return
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]:return

    opportunities=[]
    for c in runtime["candidates"]:
        if c.get("perp_eligible"):
            if c.get("data_source")=="VELOCITY":
                if c.get("direction")=="LONG" and c["long_score"]>=effective_threshold("PERP_LONG",c):
                    opportunities.append((c["long_score"],c,"PERP_LONG"))
                elif c.get("direction")=="SHORT" and c["short_score"]>=effective_threshold("PERP_SHORT",c):
                    opportunities.append((c["short_score"],c,"PERP_SHORT"))
            else:
                if c["long_score"]>=effective_threshold("PERP_LONG",c):
                    opportunities.append((c["long_score"],c,"PERP_LONG"))
                if c["short_score"]>=effective_threshold("PERP_SHORT",c):
                    opportunities.append((c["short_score"],c,"PERP_SHORT"))
        else:
            if c["pump_score"]>=effective_threshold("PUMP_LONG",c):
                opportunities.append((c["pump_score"],c,"PUMP_LONG"))
            if c["scalp_score"]>=effective_threshold("SCALP_LONG",c):
                opportunities.append((c["scalp_score"],c,"SCALP_LONG"))

    ranked=[]
    for signal,c,strategy in opportunities:
        quality,route=signal_quality(c,strategy,signal)
        assessment=entry_router_assess(c,strategy,signal)
        # READY first, then controlled SOFT_PASS, then quality.
        state_rank={"READY":2,"SOFT_PASS":1,"BLOCKED":0}.get(assessment["state"],0)
        ranked.append((state_rank,quality,signal,route,c,strategy,assessment))
    ranked.sort(key=lambda x:(x[0],x[1]),reverse=True)

    for state_rank,quality,signal,route,c,strategy,assessment in ranked:
        c["entry_router"]=assessment
        sec_score=nz((c.get("security") or {}).get("score"),100 if c.get("perp_eligible") else 50)

        if assessment["state"]=="BLOCKED":
            log_decision(c,strategy,"BLOCKED",assessment["reason"],
                         signal,quality,route.get("quality",0),sec_score)
            continue

        trade_candidate=dict(c)
        if assessment["state"]=="SOFT_PASS":
            trade_candidate["_router_soft_pass"]=True

        opened,oreason=open_position(trade_candidate,strategy)
        if opened:
            outcome_reason="selective router soft pass" if assessment["state"]=="SOFT_PASS" else "entry accepted"
            log_decision(c,strategy,"OPENED",outcome_reason,
                         signal,quality,route.get("quality",0),sec_score)
            record_event(
                "INFO","POSITION_OPENED",f"{c.get('symbol')} {strategy} opened",
                {"mode":operating_mode(),"router_state":assessment["state"],
                 "signal":signal,"quality":quality,
                 "route_quality":route.get("quality"),"security_score":sec_score}
            )
            return

        log_decision(c,strategy,"BLOCKED",oreason,
                     signal,quality,route.get("quality",0),sec_score)


def open_spot_mints():
    with SessionLocal() as s:
        rows=s.scalars(select(Position)).all()
    out=[]
    for p in rows:
        if not str(p.strategy).startswith("PERP_") and p.mint and not str(p.mint).startswith("velocity:"):
            out.append(p.mint)
    return list(dict.fromkeys(out))

async def fetch_open_spot_pairs(client,boosts):
    mints=open_spot_mints()
    if not mints:return []
    # Fetch separately from scanner so open positions never disappear merely
    # because the token drops out of discovery/top-candidate ranking.
    rows=await fetch_pairs(client,mints,boosts)
    now=time.time()
    for c in rows:
        runtime["position_price_seen"][c["mint"]]=now
        c["position_watch"]=True
    return rows


async def manage_positions_safe():
    async with position_manage_lock:
        manage_positions()

def merge_position_updates(updates):
    if not updates:return
    current={x["mint"]:x for x in runtime.get("candidates",[])}
    for fresh in updates:
        old=current.get(fresh["mint"],{})
        # Preserve expensive metadata/security from the main scanner while
        # replacing fresh price/liquidity/transaction fields.
        merged={**old,**fresh}
        if old.get("security") and not fresh.get("security"):
            merged["security"]=old["security"]
        current[fresh["mint"]]=merged
    runtime["candidates"]=list(current.values())


async def sniper_scan_loop():
    runtime["sniper_scanner_alive"]=True
    record_event("INFO","SNIPER_START","Micro-Pump Sniper scanner started",
                 {"interval_sec":i("sniper_scan_interval_sec")},dedupe_sec=5)
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Sniper-Scanner/6.6"}) as client:
        while True:
            try:
                if b("sniper_enabled"):
                    addresses=[];boosts={}
                    for ep,boosted in [
                        ("/token-boosts/latest/v1",True),
                        ("/token-profiles/latest/v1",False),
                    ]:
                        try:
                            r=await client.get(DEX+ep,timeout=12)
                            r.raise_for_status()
                            data=r.json()
                            if isinstance(data,dict):data=[data]
                            for x in data or []:
                                if x.get("chainId")!="solana" or not x.get("tokenAddress"):continue
                                a=x["tokenAddress"];addresses.append(a)
                                if boosted:
                                    boosts[a]=max(boosts.get(a,0),nz(x.get("amount"))+nz(x.get("totalAmount"))*.15)
                        except Exception:
                            pass

                    addresses=list(dict.fromkeys(addresses))[:45]
                    rows=await fetch_pairs(client,addresses,boosts) if addresses else []

                    # Pre-filter before RPC security calls.
                    rows=[
                        c for c in rows
                        if not c.get("perp_eligible")
                        and nz(c.get("sniper_score"))>=max(60,f("min_sniper_score")-8)
                        and nz(c.get("buy_pressure"))>=max(50,f("sniper_min_buy_pressure")-8)
                        and nz(c.get("volume_accel"))>=max(40,f("sniper_min_volume_accel")-10)
                        and f("sniper_min_m5_pct")<=nz(c.get("m5"))<=f("sniper_max_m5_pct")
                        and nz(c.get("age_minutes"),999999)<=f("sniper_max_age_minutes")
                    ][:6]

                    # Security scan only the strongest few sniper candidates.
                    for c in rows[:3]:
                        c["security"]=await scan_token_security(client,c)
                    for c in rows[3:]:
                        cached=runtime["token_security"].get(c["mint"])
                        c["security"]={k:v for k,v in cached.items() if k!="_ts"} if cached else {
                            "status":"UNKNOWN","score":None,"hard_block":False,
                            "flags":["sniper security scan pending"],"source":"SOLANA_RPC"
                        }

                    runtime["sniper_candidates"]=rows
                    merge_position_updates(rows)
                    runtime["last_sniper_scan"]=time.time()
                    runtime["sniper_scan_error"]=None
                    runtime["sniper_scanner_alive"]=True
                    await choose_sniper_entry()
            except Exception as e:
                runtime["sniper_scan_error"]=str(e)
                record_event("ERROR","SNIPER_SCAN_ERROR",str(e)[:220],{},dedupe_sec=120)

            await asyncio.sleep(max(5,min(30,i("sniper_scan_interval_sec"))))

async def position_watch_loop():
    runtime["position_watcher_alive"]=True
    record_event("INFO","FAST_WATCH_START","Independent fast position watcher started",
                 {"interval_sec":i("position_watch_interval_sec")},dedupe_sec=5)
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Fast-Position-Watcher/6.6"}) as client:
        while True:
            try:
                with SessionLocal() as s:
                    positions=s.scalars(select(Position)).all()
                    for p in positions:s.expunge(p)

                runtime["last_position_watch"]=time.time()
                if positions:
                    updates=[]
                    spot_mints=[p.mint for p in positions if not str(p.strategy).startswith("PERP_")]
                    if spot_mints:
                        fresh_spot=await fetch_pairs(client,list(dict.fromkeys(spot_mints)),{})
                        for c in fresh_spot:
                            c["position_watch"]=True
                            runtime["position_price_seen"][c["mint"]]=time.time()
                        updates.extend(fresh_spot)

                    if any(str(p.strategy).startswith("PERP_") for p in positions):
                        raw=await fetch_velocity_markets(client)
                        if raw:
                            perps=[perp_intelligence(v) for v in raw]
                            open_mints={p.mint for p in positions if str(p.strategy).startswith("PERP_")}
                            updates.extend([c for c in perps if c["mint"] in open_mints])

                    merge_position_updates(updates)
                    runtime["position_watch_error"]=None
                    await manage_positions_safe()

                runtime["position_watcher_alive"]=True
            except Exception as e:
                runtime["position_watch_error"]=str(e)
                record_event("ERROR","FAST_WATCH_ERROR",str(e)[:220],{},dedupe_sec=120)

            await asyncio.sleep(max(3,min(30,i("position_watch_interval_sec"))))

def today_guard_status():
    realized=today_realized()
    limit=-(f("start_balance")*f("daily_loss_limit_pct")/100)
    return {
        "today_pnl":realized,
        "loss_limit_usd":limit,
        "blocked":realized<=limit,
        "remaining_before_guard":max(0,realized-limit) if realized>limit else 0
    }

async def engine_loop():
    runtime["loop_alive"]=True
    record_event("INFO","ENGINE_START","NOVA engine loop started",{"version":APP_VERSION},dedupe_sec=5)
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Trader-Ultimate/6.6"}) as client:
        addresses=[];boosts={};last_discovery=0
        while True:
            try:
                now=time.time()
                now_iso=datetime.now(timezone.utc).isoformat()
                if now-last_discovery>60 or not addresses:
                    addresses,boosts=await discover(client);last_discovery=now

                spot_pairs=await fetch_pairs(client,addresses,boosts)

                # CRITICAL V5.2: open spot positions are fetched independently of
                # discovery ranking, so stop/trailing management cannot lose track
                # of a token merely because it leaves the candidate list.
                open_spot_pairs=await fetch_open_spot_pairs(client,boosts)
                if open_spot_pairs:
                    by_mint={c["mint"]:c for c in spot_pairs}
                    for c in open_spot_pairs:by_mint[c["mint"]]=c
                    spot_pairs=list(by_mint.values())

                if spot_pairs:
                    runtime["last_spot_refresh"]=now_iso

                velocity_raw=await fetch_velocity_markets(client)
                if velocity_raw:
                    runtime["last_perp_refresh"]=now_iso
                    perp_pairs=[perp_intelligence(v) for v in velocity_raw]
                else:
                    perp_pairs=await fetch_legacy_perp_pairs(client)

                merged={c["mint"]:c for c in spot_pairs}
                for c in perp_pairs:merged[c["mint"]]=c
                pairs=list(merged.values())
                pairs.sort(key=lambda x:max(x["pump_score"],x["scalp_score"],x["long_score"],x["short_score"]),reverse=True)

                if pairs:
                    # Security scans are real Solana RPC reads and cached to protect RPC limits.
                    spot_ranked=[c for c in pairs if not c.get("perp_eligible")]
                    scan_n=max(1,min(12,i("security_scan_top_n")))
                    for c in spot_ranked[:scan_n]:
                        c["security"]=await scan_token_security(client,c)
                    for c in spot_ranked[scan_n:]:
                        cached=runtime["token_security"].get(c["mint"])
                        c["security"]={k:v for k,v in cached.items() if k!="_ts"} if cached else {
                            "status":"UNKNOWN","score":None,"hard_block":False,
                            "flags":["not scanned in current top-N window"],"source":"SOLANA_RPC"
                        }
                    for c in pairs:
                        if c.get("perp_eligible"):
                            c["security"]={"status":"N/A","score":100,"hard_block":False,"flags":[],"source":"PERP_MARKET"}
                        st=suggested_strategy(c)
                        signal=strategy_entry_score(st,c)
                        quality,route=signal_quality(c,st,signal)
                        c["best_strategy"]=st
                        c["quality_score"]=quality
                        c["route"]=route
                        attach_entry_router_status(c)

                    runtime["candidates"]=pairs
                    runtime["last_refresh"]=now_iso
                    runtime["last_successful_loop"]=now_iso
                    runtime["engine_error_streak"]=0
                    await manage_positions_safe()
                    await choose_entry_safe()
                    record_equity_snapshot()
                    record_market_snapshots(pairs)
                    for c in pairs:runtime["prev_liq"][c["mint"]]=c.get("liquidity",0)

                    h=source_health()
                    if h["status"]=="WARN":
                        record_event("WARN","DATA_HEALTH","Market data health warning",h)
                else:
                    runtime["engine_error_streak"]+=1
                    record_event("WARN","NO_CANDIDATES","No candidates returned by market sources",{},dedupe_sec=300)

                runtime["last_error"]=None if pairs else runtime["last_error"]
            except Exception as e:
                runtime["engine_error_streak"]+=1
                runtime["last_error"]=str(e)
                record_event("ERROR","ENGINE_LOOP_ERROR",str(e)[:220],
                             {"streak":runtime["engine_error_streak"]},dedupe_sec=180)
            await asyncio.sleep(15)

@app.on_event("startup")
async def startup():
    # V5.6.1 critical cadence migration: older DB values can survive deploys.
    # Only operational polling cadences are migrated; user risk/strategy settings are preserved.
    if i("position_watch_interval_sec") != 4:
        setv("position_watch_interval_sec","4")
    if i("sniper_scan_interval_sec") != 8:
        setv("sniper_scan_interval_sec","8")
    if today_realized() < f("start_balance")*f("daily_profit_target_pct")/100:
        runtime["daily_target_locked"]=False
        runtime["daily_target_lock_time"]=None
    asyncio.create_task(engine_loop())
    asyncio.create_task(position_watch_loop())
    asyncio.create_task(sniper_scan_loop())
    asyncio.create_task(pumpportal_realtime_loop())

class SettingsIn(BaseModel):
    risk_pct: Optional[float]=None
    max_position_pct: Optional[float]=None
    max_positions: Optional[int]=None
    stop_loss_pct: Optional[float]=None
    daily_loss_limit_pct: Optional[float]=None
    min_pump_score: Optional[float]=None
    min_scalp_score: Optional[float]=None
    min_long_score: Optional[float]=None
    min_short_score: Optional[float]=None
    perp_leverage: Optional[float]=None
    min_direction_edge: Optional[float]=None
    min_perp_oi_usd: Optional[float]=None
    max_abs_funding_rate: Optional[float]=None
    block_extreme_volatility: Optional[bool]=None
    adaptive_enabled: Optional[bool]=None
    optimizer_min_samples: Optional[int]=None
    max_adaptive_shift: Optional[float]=None
    strategy_pause_min_samples: Optional[int]=None
    strategy_pause_minutes: Optional[int]=None
    drawdown_soft_cut_pct: Optional[float]=None
    drawdown_hard_cut_pct: Optional[float]=None
    snapshot_interval_sec: Optional[int]=None
    snapshot_retention_days: Optional[int]=None
    replay_hold_minutes: Optional[int]=None
    replay_take_profit_pct: Optional[float]=None
    research_days: Optional[int]=None
    monte_carlo_runs: Optional[int]=None
    portfolio_brain_enabled: Optional[bool]=None
    max_total_exposure_pct: Optional[float]=None
    max_strategy_exposure_pct: Optional[float]=None
    max_direction_exposure_pct: Optional[float]=None
    correlation_threshold: Optional[float]=None
    correlation_lookback_points: Optional[int]=None
    min_portfolio_weight: Optional[float]=None
    execution_simulator_enabled: Optional[bool]=None
    spot_fee_bps: Optional[float]=None
    perp_fee_bps: Optional[float]=None
    base_spread_bps: Optional[float]=None
    slippage_floor_bps: Optional[float]=None
    impact_coefficient_bps: Optional[float]=None
    simulated_latency_ms: Optional[float]=None
    max_execution_cost_bps: Optional[float]=None
    max_data_age_sec: Optional[float]=None
    min_token_security_score: Optional[float]=None
    security_scan_ttl_sec: Optional[int]=None
    security_scan_top_n: Optional[int]=None
    security_required_shadow: Optional[bool]=None
    security_hard_block_shadow: Optional[bool]=None
    min_route_quality: Optional[float]=None
    spot_max_hold_minutes: Optional[float]=None
    perp_max_hold_minutes: Optional[float]=None
    reversal_exit_edge: Optional[float]=None
    breakeven_trigger_pct: Optional[float]=None
    breakeven_exit_pct: Optional[float]=None
    profit_lock_enabled: Optional[bool]=None
    capital_shield_enabled: Optional[bool]=None
    scalp_max_loss_pct: Optional[float]=None
    pump_max_loss_pct: Optional[float]=None
    perp_max_loss_pct: Optional[float]=None
    scratch_after_minutes: Optional[float]=None
    scratch_loss_pct: Optional[float]=None
    scratch_min_peak_pct: Optional[float]=None
    global_equity_guard_pct: Optional[float]=None
    capital_shield_min_liquidity: Optional[float]=None
    capital_shield_min_market_quality: Optional[float]=None
    max_spot_position_liquidity_pct: Optional[float]=None
    max_spot_abs_m5_pct: Optional[float]=None

    sniper_enabled: Optional[bool]=None
    min_sniper_score: Optional[float]=None
    sniper_scan_interval_sec: Optional[int]=None
    sniper_min_buy_pressure: Optional[float]=None
    sniper_min_volume_accel: Optional[float]=None
    sniper_min_m5_pct: Optional[float]=None
    sniper_max_m5_pct: Optional[float]=None
    sniper_max_age_minutes: Optional[float]=None
    sniper_max_positions: Optional[int]=None
    sniper_risk_multiplier: Optional[float]=None
    sniper_max_loss_pct: Optional[float]=None
    sniper_scratch_minutes: Optional[float]=None
    sniper_scratch_loss_pct: Optional[float]=None
    sniper_max_hold_minutes: Optional[float]=None

    realtime_pulse_enabled: Optional[bool]=None
    pulse_min_score: Optional[float]=None
    pulse_min_events_5s: Optional[int]=None
    pulse_min_buy_pressure_5s: Optional[float]=None
    pulse_min_unique_buyers_10s: Optional[int]=None
    pulse_max_m5_pct: Optional[float]=None
    pulse_cooldown_sec: Optional[float]=None
    pulse_eval_timeout_sec: Optional[float]=None
    realtime_exit_enabled: Optional[bool]=None
    realtime_exit_min_interval_ms: Optional[float]=None
    realtime_exit_rest_fallback_ms: Optional[float]=None
    pulse_max_trade_subscriptions: Optional[int]=None
    pulse_subscription_ttl_sec: Optional[float]=None

    adaptive_pulse_enabled: Optional[bool]=None
    pulse_diag_window_sec: Optional[int]=None
    pulse_adapt_interval_sec: Optional[int]=None
    pulse_adapt_min_observations: Optional[int]=None
    pulse_score_floor: Optional[float]=None
    pulse_events_floor: Optional[int]=None
    pulse_buy_pressure_floor: Optional[float]=None
    pulse_unique_buyers_floor: Optional[int]=None
    pulse_score_ceiling: Optional[float]=None
    pulse_buy_pressure_ceiling: Optional[float]=None
    pulse_unique_buyers_ceiling: Optional[int]=None
    pulse_recovery_score: Optional[float]=None
    pulse_recovery_events_5s: Optional[int]=None
    pulse_recovery_buy_pressure: Optional[float]=None
    pulse_recovery_unique_buyers: Optional[int]=None
    pulse_confirmation_required: Optional[int]=None
    pulse_confirmation_window_sec: Optional[float]=None
    pulse_confirmation_min_gap_ms: Optional[float]=None

    edge_governor_enabled: Optional[bool]=None
    governor_min_samples: Optional[int]=None
    governor_recent_trades: Optional[int]=None
    governor_probation_risk_multiplier: Optional[float]=None
    governor_caution_risk_multiplier: Optional[float]=None
    governor_min_profit_factor: Optional[float]=None
    governor_pause_minutes: Optional[int]=None
    governor_loss_streak_limit: Optional[int]=None
    governor_loss_streak_pause_minutes: Optional[int]=None
    survival_daily_loss_pct: Optional[float]=None
    survival_combined_loss_pct: Optional[float]=None
    recovery_spot_liquidity_position_cap_pct: Optional[float]=None
    recovery_sniper_loss_cap_pct: Optional[float]=None
    recovery_scalp_loss_cap_pct: Optional[float]=None
    recovery_pump_loss_cap_pct: Optional[float]=None
    recovery_perp_loss_cap_pct: Optional[float]=None

    selective_entry_router_enabled: Optional[bool]=None
    router_soft_market_quality_floor: Optional[float]=None
    router_soft_liquidity_floor: Optional[float]=None
    router_min_signal_margin: Optional[float]=None
    router_min_buy_pressure: Optional[float]=None
    router_max_soft_m5_pct: Optional[float]=None
    router_soft_risk_multiplier: Optional[float]=None
    router_max_soft_entries_per_hour: Optional[int]=None

    launch_sniper_enabled: Optional[bool]=None
    launch_max_active_watch: Optional[int]=None
    launch_watch_ttl_sec: Optional[float]=None
    launch_entry_max_age_sec: Optional[float]=None
    launch_min_age_sec: Optional[float]=None
    launch_min_score: Optional[float]=None
    launch_min_events_2s: Optional[int]=None
    launch_min_unique_buyers_2s: Optional[int]=None
    launch_min_buy_pressure_2s: Optional[float]=None
    launch_min_buy_sol_2s: Optional[float]=None
    launch_max_top_buyer_share_pct: Optional[float]=None
    launch_max_mcap_multiple: Optional[float]=None
    launch_min_mcap_multiple: Optional[float]=None
    launch_creator_window_sec: Optional[float]=None
    launch_creator_max_tokens: Optional[int]=None
    launch_duplicate_symbol_max: Optional[int]=None
    launch_confirmation_required: Optional[int]=None
    launch_confirmation_window_sec: Optional[float]=None
    launch_confirmation_gap_ms: Optional[float]=None
    launch_max_positions: Optional[int]=None
    launch_max_position_pct: Optional[float]=None
    launch_max_entries_per_hour: Optional[int]=None
    launch_raw_stop_pct: Optional[float]=None
    launch_max_net_loss_pct: Optional[float]=None
    launch_scratch_after_sec: Optional[float]=None
    launch_scratch_peak_pct: Optional[float]=None
    launch_scratch_loss_pct: Optional[float]=None
    launch_max_hold_sec: Optional[float]=None
    launch_flow_reversal_after_sec: Optional[float]=None
    launch_flow_reversal_pressure: Optional[float]=None

    daily_profit_target_pct: Optional[float]=None
    daily_profit_secure_buffer_pct: Optional[float]=None
    daily_de_risk_start_pct: Optional[float]=None
    daily_de_risk_multiplier: Optional[float]=None
    daily_target_lock_enabled: Optional[bool]=None
    no_martingale: Optional[bool]=None
    max_total_open_risk_pct: Optional[float]=None

    position_watch_interval_sec: Optional[int]=None
    stale_position_price_sec: Optional[int]=None
    readiness_min_trades: Optional[int]=None
    readiness_min_pf: Optional[float]=None
    readiness_min_shadow_hours: Optional[float]=None
    readiness_max_drawdown_pct: Optional[float]=None
    readiness_max_mc_below_start_pct: Optional[float]=None
    readiness_min_wf_robust: Optional[int]=None
    readiness_max_exec_bps: Optional[float]=None
    min_liquidity: Optional[float]=None
    min_market_risk: Optional[float]=None
    min_spot_market_quality: Optional[float]=None
    execution_cost_pct: Optional[float]=None
    cooldown_minutes: Optional[int]=None

class WatchIn(BaseModel):
    mint:str

@app.get("/")
def root():
    return {"name":"NOVA Trader Ultimate","version":APP_VERSION,
            "operating_mode":operating_mode(),"live_execution_locked":LIVE_EXECUTION_LOCKED,
            "mode":"PAPER + SHADOW / LIVE HARD-LOCKED","docs":"/docs"}

@app.get("/health")
def health():
    h=source_health()
    return {"ok":h["status"]!="CRITICAL","version":APP_VERSION,"system":h,
            "operating_mode":operating_mode(),"live_execution_locked":LIVE_EXECUTION_LOCKED,
            "last_refresh":runtime["last_refresh"],"last_error":runtime["last_error"]}

@app.get("/api/dashboard")
def dashboard():
    with SessionLocal() as s:
        trades=s.scalars(select(Trade).order_by(Trade.id.desc()).limit(50)).all()
    return {
        "version":APP_VERSION,
        "mode":operating_mode(),
        "live_execution_locked":LIVE_EXECUTION_LOCKED,
        "bot_enabled":b("bot_enabled"),"killed":b("killed"),
        "pause_until":runtime["pause_until"].isoformat() if runtime["pause_until"] else None,
        "last_refresh":runtime["last_refresh"],"last_error":runtime["last_error"],
        "velocity_source_ok":runtime["velocity_source_ok"],
        "last_velocity_refresh":runtime["last_velocity_refresh"],
        "metrics":{**metrics(),**today_guard_status(),"equity_guard":global_equity_guard_status()},
        "daily_target":daily_target_status(),
        "risk_status":{
            "open_risk_pct":round(open_risk_pct(),3),
            "max_total_open_risk_pct":f("max_total_open_risk_pct"),
            "target_risk_multiplier":daily_target_risk_multiplier(),
            "no_martingale":b("no_martingale")
        },
        "launch_sniper":launch_status(),
        "pulse_intelligence":pulse_diag_summary(),
        "entry_router":{
            "enabled":b("selective_entry_router_enabled"),
            "soft_entries_last_hour":router_soft_entry_count(),
            "soft_entries_total":runtime["router_soft_entries"],
            "max_soft_entries_per_hour":i("router_max_soft_entries_per_hour"),
            "last_entry":runtime["router_last_entry"],
            "states":{
                "READY":sum(1 for c in runtime.get("candidates",[])[:20] if (c.get("entry_router") or {}).get("state")=="READY"),
                "SOFT_PASS":sum(1 for c in runtime.get("candidates",[])[:20] if (c.get("entry_router") or {}).get("state")=="SOFT_PASS"),
                "BLOCKED":sum(1 for c in runtime.get("candidates",[])[:20] if (c.get("entry_router") or {}).get("state")=="BLOCKED")
            }
        },
        "edge_governor":{
            "enabled":b("edge_governor_enabled"),
            "strategies":{s:strategy_governor_status(s) for s in
                ["LAUNCH_SNIPER","SNIPER_LONG","SCALP_LONG","PUMP_LONG","PERP_LONG","PERP_SHORT"]}
        },
        "survival_guard":survival_guard_status(),
        "realtime_exit":{
            "enabled":b("realtime_exit_enabled"),
            "mode":"EVENT_DRIVEN" if runtime["pulse_stream_connected"] else "4S_FALLBACK",
            "checks":runtime["realtime_exit_checks"],
            "direct_marks":runtime["realtime_exit_direct_marks"],
            "rest_checks":runtime["realtime_exit_rest_checks"],
            "last_event_age_sec":round(time.time()-runtime["realtime_exit_last_event"],3) if runtime["realtime_exit_last_event"] else None,
            "armed_positions":len(runtime["realtime_exit_refs"]),
            "error":runtime["realtime_exit_error"]
        },
        "realtime_pulse":{
            "enabled":b("realtime_pulse_enabled"),
            "connected":runtime["pulse_stream_connected"],
            "mode":runtime["pulse_stream_mode"],
            "events_total":runtime["pulse_events_total"],
            "entries":runtime["pulse_entries"],
            "subscribed_tokens":len(runtime["pulse_subscribed"]),
            "last_event_age_sec":round(time.time()-runtime["pulse_last_event"],3) if runtime["pulse_last_event"] else None,
            "hot":[{
                "mint":mint,"score":v.get("score"),"events_2s":v.get("events_2s"),
                "events_5s":v.get("events_5s"),"buy_pressure_5s":v.get("buy_pressure_5s"),
                "unique_buyers_10s":v.get("unique_buyers_10s"),"buy_sol_5s":v.get("buy_sol_5s"),
                "acceleration_score":v.get("acceleration_score")
            } for mint,v in sorted(runtime["pulse_hot"].items(),
                key=lambda kv:nz(kv[1].get("score")),reverse=True)[:8]]
        },
        "sniper":{
            "enabled":b("sniper_enabled"),
            "alive":runtime["sniper_scanner_alive"],
            "interval_sec":i("sniper_scan_interval_sec"),
            "last_scan_age_sec":round(time.time()-runtime["last_sniper_scan"],1) if runtime["last_sniper_scan"] else None,
            "entries":runtime["sniper_entries"],
            "candidates":[{
                "mint":c.get("mint"),"symbol":c.get("symbol"),
                "sniper_score":c.get("sniper_score"),"m5":c.get("m5"),
                "buy_pressure":c.get("buy_pressure"),"volume_accel":c.get("volume_accel"),
                "liquidity":c.get("liquidity"),"age_minutes":c.get("age_minutes")
            } for c in runtime.get("sniper_candidates",[])[:5]],
            "error":runtime["sniper_scan_error"]
        },
        "fast_watcher":{
            "alive":runtime["position_watcher_alive"],
            "interval_sec":i("position_watch_interval_sec"),
            "open_positions":metrics()["open_positions"],
            "last_watch_age_sec":round(time.time()-runtime["last_position_watch"],1) if runtime["last_position_watch"] else None,
            "error":runtime["position_watch_error"]
        },
        "equity_curve":equity_curve(120),
        "adaptive":adaptive_status(),
        "research_status":snapshot_stats(),
        "portfolio":portfolio_status(),
        "execution":execution_stats(),
        "system_health":source_health(),
        "security":token_security_status(),
        "readiness":live_readiness(),
        "decisions":recent_decisions(20),
        "events":system_events(20),
        "positions":positions_with_marks(),
        "candidates":runtime["candidates"][:30],
        "trades":[{
            "id":t.id,"symbol":t.symbol,"strategy":t.strategy,"side":strategy_side(t.strategy),"pnl":t.pnl,
            "pnl_pct":t.pnl_pct,"reason":t.reason,"closed_at":t.closed_at.isoformat()
        } for t in trades],
        "settings":{
            **{k:float(getv(k)) for k in [
                "risk_pct","max_position_pct","max_positions","stop_loss_pct","daily_loss_limit_pct",
                "min_pump_score","min_scalp_score","min_long_score","min_short_score","perp_leverage",
                "min_direction_edge","min_perp_oi_usd","max_abs_funding_rate",
                "optimizer_min_samples","max_adaptive_shift","strategy_pause_min_samples",
                "strategy_pause_minutes","drawdown_soft_cut_pct","drawdown_hard_cut_pct",
                "snapshot_interval_sec","snapshot_retention_days","replay_hold_minutes",
                "replay_take_profit_pct","research_days","monte_carlo_runs",
                "max_total_exposure_pct","max_strategy_exposure_pct","max_direction_exposure_pct",
                "correlation_threshold","correlation_lookback_points","min_portfolio_weight",
                "spot_fee_bps","perp_fee_bps","base_spread_bps","slippage_floor_bps",
                "impact_coefficient_bps","simulated_latency_ms","max_execution_cost_bps",
                "max_data_age_sec","min_token_security_score","security_scan_ttl_sec","security_scan_top_n",
                "min_route_quality","spot_max_hold_minutes","perp_max_hold_minutes","reversal_exit_edge",
                "breakeven_trigger_pct","breakeven_exit_pct","position_watch_interval_sec","stale_position_price_sec","readiness_min_trades","readiness_min_pf",
                "readiness_min_shadow_hours","readiness_max_drawdown_pct","readiness_max_mc_below_start_pct",
                "readiness_min_wf_robust","readiness_max_exec_bps",
                "scalp_max_loss_pct","pump_max_loss_pct","perp_max_loss_pct","scratch_after_minutes",
                "scratch_loss_pct","scratch_min_peak_pct","global_equity_guard_pct",
                "capital_shield_min_liquidity","capital_shield_min_market_quality",
                "max_spot_position_liquidity_pct","max_spot_abs_m5_pct",
                "min_sniper_score","sniper_scan_interval_sec","sniper_min_buy_pressure",
                "sniper_min_volume_accel","sniper_min_m5_pct","sniper_max_m5_pct",
                "sniper_max_age_minutes","sniper_max_positions","sniper_risk_multiplier",
                "sniper_max_loss_pct","sniper_scratch_minutes","sniper_scratch_loss_pct",
                "sniper_max_hold_minutes","pulse_min_score","pulse_min_events_5s",
                "pulse_min_buy_pressure_5s","pulse_min_unique_buyers_10s",
                "pulse_max_m5_pct","pulse_cooldown_sec","pulse_eval_timeout_sec",
                "realtime_exit_min_interval_ms","realtime_exit_rest_fallback_ms",
                "pulse_max_trade_subscriptions","pulse_subscription_ttl_sec",
                "pulse_diag_window_sec","pulse_adapt_interval_sec","pulse_adapt_min_observations",
                "pulse_score_floor","pulse_events_floor","pulse_buy_pressure_floor",
                "pulse_unique_buyers_floor","pulse_score_ceiling","pulse_buy_pressure_ceiling",
                "pulse_unique_buyers_ceiling","pulse_recovery_score","pulse_recovery_events_5s",
                "pulse_recovery_buy_pressure","pulse_recovery_unique_buyers",
                "pulse_confirmation_required","pulse_confirmation_window_sec","pulse_confirmation_min_gap_ms",
                "governor_min_samples","governor_recent_trades","governor_probation_risk_multiplier",
                "governor_caution_risk_multiplier","governor_min_profit_factor","governor_pause_minutes",
                "governor_loss_streak_limit","governor_loss_streak_pause_minutes",
                "survival_daily_loss_pct","survival_combined_loss_pct",
                "recovery_spot_liquidity_position_cap_pct","recovery_sniper_loss_cap_pct",
                "recovery_scalp_loss_cap_pct","recovery_pump_loss_cap_pct","recovery_perp_loss_cap_pct",
                "router_soft_market_quality_floor","router_soft_liquidity_floor",
                "router_min_signal_margin","router_min_buy_pressure","router_max_soft_m5_pct",
                "router_soft_risk_multiplier","router_max_soft_entries_per_hour",
                "launch_max_active_watch","launch_watch_ttl_sec","launch_entry_max_age_sec",
                "launch_min_age_sec","launch_min_score","launch_min_events_2s",
                "launch_min_unique_buyers_2s","launch_min_buy_pressure_2s","launch_min_buy_sol_2s",
                "launch_max_top_buyer_share_pct","launch_max_mcap_multiple","launch_min_mcap_multiple",
                "launch_creator_window_sec","launch_creator_max_tokens","launch_duplicate_symbol_max",
                "launch_confirmation_required","launch_confirmation_window_sec","launch_confirmation_gap_ms",
                "launch_max_positions","launch_max_position_pct","launch_max_entries_per_hour",
                "launch_raw_stop_pct","launch_max_net_loss_pct","launch_scratch_after_sec",
                "launch_scratch_peak_pct","launch_scratch_loss_pct","launch_max_hold_sec",
                "launch_flow_reversal_after_sec","launch_flow_reversal_pressure",
                "daily_profit_target_pct","daily_profit_secure_buffer_pct",
                "daily_de_risk_start_pct","daily_de_risk_multiplier","max_total_open_risk_pct",
                "min_liquidity","min_market_risk","min_spot_market_quality","execution_cost_pct","cooldown_minutes"
            ]},
            "block_extreme_volatility":b("block_extreme_volatility"),
            "adaptive_enabled":b("adaptive_enabled"),
            "portfolio_brain_enabled":b("portfolio_brain_enabled"),
            "execution_simulator_enabled":b("execution_simulator_enabled"),
            "security_required_shadow":b("security_required_shadow"),
            "security_hard_block_shadow":b("security_hard_block_shadow"),
            "profit_lock_enabled":b("profit_lock_enabled"),
            "capital_shield_enabled":b("capital_shield_enabled"),
            "sniper_enabled":b("sniper_enabled"),
            "realtime_pulse_enabled":b("realtime_pulse_enabled"),
            "realtime_exit_enabled":b("realtime_exit_enabled"),
            "adaptive_pulse_enabled":b("adaptive_pulse_enabled"),
            "edge_governor_enabled":b("edge_governor_enabled"),
            "selective_entry_router_enabled":b("selective_entry_router_enabled"),
            "launch_sniper_enabled":b("launch_sniper_enabled"),
            "daily_target_lock_enabled":b("daily_target_lock_enabled"),
            "no_martingale":b("no_martingale"),
            "operating_mode":operating_mode()
        }
    }

@app.get("/api/readiness")
def readiness():
    return live_readiness()

@app.get("/api/system")
def system_status():
    return {"health":source_health(),"events":system_events(50),"mode":operating_mode(),
            "live_execution_locked":LIVE_EXECUTION_LOCKED}

@app.get("/api/security")
def security():
    return token_security_status()

@app.post("/api/security/rescan/{mint}")
async def security_rescan(mint:str, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    c=next((x for x in runtime["candidates"] if x.get("mint")==mint),None)
    if not c:raise HTTPException(404,"Candidate not found")
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Trader-Ultimate/6.6"}) as client:
        result=await scan_token_security(client,c,force=True)
    c["security"]=result
    return {"mint":mint,"symbol":c.get("symbol"),"security":result}

@app.get("/api/decisions")
def decisions():
    return recent_decisions(100)

@app.get("/api/full-status")
def full_status():
    return {
        "version":APP_VERSION,"mode":operating_mode(),"live_execution_locked":LIVE_EXECUTION_LOCKED,
        "health":source_health(),"readiness":live_readiness(),"metrics":metrics(),
        "adaptive":adaptive_status(),"portfolio":portfolio_status(),"execution":execution_stats(),
        "research_status":snapshot_stats(),"security":token_security_status()
    }

@app.post("/api/mode/{mode}")
def set_mode(mode:str, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    requested=mode.upper().strip()
    if requested=="LIVE":
        record_event("WARN","LIVE_BLOCKED","Attempt to select LIVE mode was blocked")
        raise HTTPException(403,"LIVE execution is hard-locked in NOVA V5.0")
    if requested not in ("PAPER","SHADOW"):
        raise HTTPException(400,"Mode must be PAPER or SHADOW")
    previous=operating_mode()
    if requested=="SHADOW" and previous!="SHADOW":
        setv("shadow_started_at",datetime.now(timezone.utc).isoformat())
    setv("operating_mode",requested)
    record_event("INFO","MODE_CHANGE",f"Operating mode changed {previous} -> {requested}")
    return {"ok":True,"mode":requested,"live_execution_locked":LIVE_EXECUTION_LOCKED}

@app.get("/api/execution")
def execution():
    return execution_stats()

@app.post("/api/execution/estimate")
def execution_estimate(data:WatchIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    c=next((x for x in runtime["candidates"] if x.get("mint")==data.mint),None)
    if not c:raise HTTPException(404,"Candidate not found")
    strategy="PERP_SHORT" if c.get("perp_eligible") and c.get("direction")=="SHORT" else (
        "PERP_LONG" if c.get("perp_eligible") else "SCALP_LONG"
    )
    notional=max(10.0,planned_collateral(c,strategy)*strategy_leverage(strategy))
    return {"symbol":c.get("symbol"),"strategy":strategy,"notional_usd":notional,
            "estimate":execution_cost_estimate(c,strategy,notional)}

@app.get("/api/portfolio")
def portfolio():
    return portfolio_status()

@app.post("/api/research/run")
def run_research(x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    return research_report()

@app.get("/api/research/status")
def research_status():
    return snapshot_stats()

@app.get("/api/optimizer")
def optimizer():
    return adaptive_status()

@app.post("/api/control/{action}")
def control(action:str, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    if action=="start":
        setv("killed","false");setv("bot_enabled","true")
        record_event("INFO","BOT_START","Auto Trader started",{"mode":operating_mode()},dedupe_sec=5)
    elif action=="stop":
        setv("bot_enabled","false")
        record_event("INFO","BOT_STOP","Auto Trader stopped",{},dedupe_sec=5)
    elif action=="kill":
        setv("bot_enabled","false");setv("killed","true")
        record_event("WARN","KILL_SWITCH","Kill switch activated",{},dedupe_sec=5)
    elif action=="reset-paper":
        with SessionLocal() as s:
            s.query(PositionFeature).delete();s.query(TradeFeature).delete();s.query(ExecutionEvent).delete();s.query(Position).delete();s.query(Trade).delete();s.query(EquityPoint).delete();s.commit()
        setv("cash",getv("start_balance"));setv("bot_enabled","false");setv("killed","false")
        runtime["cooldowns"].clear();runtime["strategy_pauses"].clear();runtime["last_portfolio_block"]=None;runtime["last_execution_block"]=None;runtime["execution_blocks"]=0;runtime["position_price_seen"].clear();runtime["sniper_entries"]=0;runtime["sniper_candidates"]=[];runtime["pulse_entries"]=0;runtime["pulse_diag_events"].clear();runtime["pulse_diag_dedupe"].clear();runtime["pulse_recent_best"].clear();runtime["pulse_confirmations"].clear();runtime["governor_pauses"].clear();runtime["governor_last_reason"].clear();runtime["router_soft_entry_times"].clear();runtime["router_last_entry"]=None;runtime["router_soft_entries"]=0;runtime["launch_watch"].clear();runtime["launch_marks"].clear();runtime["launch_confirmations"].clear();runtime["launch_evaluating"].clear();runtime["launch_creator_history"].clear();runtime["launch_symbol_history"].clear();runtime["launch_entries"]=0;runtime["launch_entry_times"].clear();runtime["launch_new_tokens"]=0;runtime["launch_trades_seen"]=0;runtime["launch_diag"].clear();runtime["launch_best"]=None;runtime["launch_last_event"]=0;runtime["launch_last_entry"]=None;runtime["pulse_adaptive_shifts"]={"score":0.0,"events":0,"pressure":0.0,"buyers":0};runtime["pulse_adaptive_actions"].clear();runtime["pulse_last_perf_trade_id"]=0;runtime["realtime_exit_checks"]=0;runtime["realtime_exit_direct_marks"]=0;runtime["realtime_exit_rest_checks"]=0;runtime["realtime_exit_refs"].clear();runtime["daily_target_locked"]=False;runtime["daily_target_lock_time"]=None;runtime["pause_until"]=None
        record_event("INFO","PAPER_RESET","Paper account reset to start balance",{"start_balance":f("start_balance")},dedupe_sec=5)
    else: raise HTTPException(400,"Unknown action")
    return {"ok":True,"action":action}

@app.post("/api/settings")
def settings(data:SettingsIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    vals=data.model_dump(exclude_none=True)
    if "risk_pct" in vals and not (0.05<=vals["risk_pct"]<=2.0):
        raise HTTPException(400,"risk_pct must be between 0.05 and 2.0")
    if "perp_leverage" in vals and not (1.0<=vals["perp_leverage"]<=2.0):
        raise HTTPException(400,"perp_leverage must be between 1x and 2x")
    if "max_positions" in vals and not (1<=vals["max_positions"]<=10):
        raise HTTPException(400,"max_positions must be 1..10")
    if "stop_loss_pct" in vals and not (0.5<=vals["stop_loss_pct"]<=15):
        raise HTTPException(400,"stop_loss_pct must be 0.5..15")
    if "correlation_threshold" in vals and not (0.3<=vals["correlation_threshold"]<=0.99):
        raise HTTPException(400,"correlation_threshold must be 0.30..0.99")
    if "launch_min_score" in vals and not (65<=vals["launch_min_score"]<=90):
        raise HTTPException(400,"launch_min_score must be 65..90")
    if "launch_min_events_2s" in vals and not (2<=vals["launch_min_events_2s"]<=10):
        raise HTTPException(400,"launch_min_events_2s must be 2..10")
    if "launch_min_unique_buyers_2s" in vals and not (1<=vals["launch_min_unique_buyers_2s"]<=6):
        raise HTTPException(400,"launch_min_unique_buyers_2s must be 1..6")
    if "launch_max_position_pct" in vals and not (0.10<=vals["launch_max_position_pct"]<=1.0):
        raise HTTPException(400,"launch_max_position_pct must be 0.10..1.0")
    if "launch_max_entries_per_hour" in vals and not (1<=vals["launch_max_entries_per_hour"]<=10):
        raise HTTPException(400,"launch_max_entries_per_hour must be 1..10")
    if "router_soft_market_quality_floor" in vals and not (58<=vals["router_soft_market_quality_floor"]<=70):
        raise HTTPException(400,"router_soft_market_quality_floor must be 58..70")
    if "router_soft_liquidity_floor" in vals and not (25000<=vals["router_soft_liquidity_floor"]<=50000):
        raise HTTPException(400,"router_soft_liquidity_floor must be 25000..50000")
    if "router_soft_risk_multiplier" in vals and not (0.10<=vals["router_soft_risk_multiplier"]<=0.75):
        raise HTTPException(400,"router_soft_risk_multiplier must be 0.10..0.75")
    if "router_max_soft_entries_per_hour" in vals and not (1<=vals["router_max_soft_entries_per_hour"]<=6):
        raise HTTPException(400,"router_max_soft_entries_per_hour must be 1..6")
    if "pulse_score_floor" in vals and not (60<=vals["pulse_score_floor"]<=80):
        raise HTTPException(400,"pulse_score_floor must be 60..80")
    if "pulse_buy_pressure_floor" in vals and not (55<=vals["pulse_buy_pressure_floor"]<=75):
        raise HTTPException(400,"pulse_buy_pressure_floor must be 55..75")
    if "pulse_adapt_min_observations" in vals and not (20<=vals["pulse_adapt_min_observations"]<=500):
        raise HTTPException(400,"pulse_adapt_min_observations must be 20..500")
    if "pulse_min_score" in vals and not (50<=vals["pulse_min_score"]<=95):
        raise HTTPException(400,"pulse_min_score must be 50..95")
    if "pulse_min_events_5s" in vals and not (2<=vals["pulse_min_events_5s"]<=30):
        raise HTTPException(400,"pulse_min_events_5s must be 2..30")
    if "daily_profit_target_pct" in vals and not (1<=vals["daily_profit_target_pct"]<=25):
        raise HTTPException(400,"daily_profit_target_pct must be 1..25")
    if "daily_de_risk_start_pct" in vals and not (0.5<=vals["daily_de_risk_start_pct"]<=20):
        raise HTTPException(400,"daily_de_risk_start_pct must be 0.5..20")
    if "daily_de_risk_multiplier" in vals and not (0.1<=vals["daily_de_risk_multiplier"]<=1.0):
        raise HTTPException(400,"daily_de_risk_multiplier must be 0.1..1.0")
    if "max_total_open_risk_pct" in vals and not (0.5<=vals["max_total_open_risk_pct"]<=10):
        raise HTTPException(400,"max_total_open_risk_pct must be 0.5..10")
    if "sniper_scan_interval_sec" in vals and not (5<=vals["sniper_scan_interval_sec"]<=30):
        raise HTTPException(400,"sniper_scan_interval_sec must be 5..30")
    if "sniper_risk_multiplier" in vals and not (0.1<=vals["sniper_risk_multiplier"]<=1.0):
        raise HTTPException(400,"sniper_risk_multiplier must be 0.1..1.0")
    if "sniper_max_positions" in vals and not (1<=vals["sniper_max_positions"]<=2):
        raise HTTPException(400,"sniper_max_positions must be 1..2")
    if "position_watch_interval_sec" in vals and not (3<=vals["position_watch_interval_sec"]<=30):
        raise HTTPException(400,"position_watch_interval_sec must be 3..30")
    if "stale_position_price_sec" in vals and not (15<=vals["stale_position_price_sec"]<=180):
        raise HTTPException(400,"stale_position_price_sec must be 15..180")
    for k,v in vals.items():setv(k,v)
    record_event("INFO","SETTINGS_UPDATE","Risk/strategy settings updated",{"keys":list(vals.keys())},dedupe_sec=5)
    return {"ok":True,"updated":list(vals.keys())}

@app.post("/api/watch")
def watch(data:WatchIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    mint=data.mint.strip()
    if len(mint)<30:raise HTTPException(400,"Invalid mint")
    vals=[x for x in getv("watchlist","").split(",") if x]
    if mint not in vals:vals.append(mint)
    setv("watchlist",",".join(vals[-30:]))
    return {"ok":True}

@app.post("/api/watch/remove")
def watch_remove(data:WatchIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    mint=data.mint.strip()
    vals=[x for x in getv("watchlist","").split(",") if x and x!=mint]
    setv("watchlist",",".join(vals))
    return {"ok":True,"removed":mint}
