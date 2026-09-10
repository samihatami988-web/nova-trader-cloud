import os, asyncio, math, time, random
from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, String, Float, Integer, Boolean, DateTime, Text, select, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

APP_VERSION = "4.5.0"
DEX = "https://api.dexscreener.com"
VELOCITY_DATA = "https://data.velocity.exchange"

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
    "min_liquidity": "20000",
    "min_market_risk": "70",
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
    "cooldowns": {},
    "strategy_pauses": {},
    "pause_until": None,
    "loop_alive": False,
}

def auth(x_nova_key: Optional[str]):
    if not x_nova_key or x_nova_key != ADMIN_KEY:
        raise HTTPException(401, "Invalid NOVA admin key")

def nz(v, d=0.0):
    try: return float(v or d)
    except: return d

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
    m5, h1 = nz(pc.get("m5")), nz(pc.get("h1"))
    bull_momentum = max(0, min(100, 50 + m5 * 5.0 + h1 * 0.65))
    bear_momentum = max(0, min(100, 50 - m5 * 5.0 - h1 * 0.65))

    liq = nz((p.get("liquidity") or {}).get("usd"))
    liquidity_score = max(0, min(100, 20 + 22 * math.log10(max(liq, 1) / 1000)))

    created = nz(p.get("pairCreatedAt"))
    age_min = (time.time()*1000 - created)/60000 if created else 999999
    age_score = 100 if 10 <= age_min <= 1440 else 80 if 5 <= age_min <= 4320 else 55 if age_min <= 10080 else 35

    fdv = nz(p.get("marketCap") or p.get("fdv"))
    ratio = liq / max(fdv, 1)
    ratio_score = max(0, min(100, ratio * 700))

    boost = min(100, source_boost)
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
        "pump_score": max(0,min(100,pump)),
        "scalp_score": max(0,min(100,scalp)),
        "long_score": max(0,min(100,long_score)),
        "short_score": max(0,min(100,short_score)),
        "market_risk": max(0,min(100,market_risk)),
        "buy_pressure": round(pressure,1),
        "volume_accel": round(vol_accel,1),
        "m5": m5, "h1": h1, "liquidity": liq, "market_cap": fdv,
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
            ret=directional_return_pct(p,price)
            value=paper_position_value(p,price)
            out.append({
                "id":p.id,"mint":p.mint,"symbol":p.symbol,"name":p.name,
                "strategy":p.strategy,"side":strategy_side(p.strategy),
                "leverage":strategy_leverage(p.strategy),
                "entry":p.entry_price,"price":price,
                "return_pct":ret,"remaining_cost":p.remaining_cost,
                "market_value":value,"locked_pnl":p.locked_pnl,
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
    fee=f("execution_cost_pct")*2
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
            ret=gross-fee*leverage
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
    strategies=["PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
    replays={}
    walks={}
    for s in strategies:
        replays[s]=replay_strategy(rows,s,base_threshold(s))
        walks[s]=walk_forward(rows,s)
    return {
        "version":APP_VERSION,"data":snapshot_stats(),
        "replay":replays,"walk_forward":walks,"monte_carlo":monte_carlo(),
        "note":"Replay uses snapshots recorded by NOVA after V4.4 deployment; it is not tick-level exchange backtesting."
    }


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
        "RISK_ON":{"PUMP_LONG":1.00,"SCALP_LONG":0.95,"PERP_LONG":1.00,"PERP_SHORT":0.50},
        "RISK_OFF":{"PUMP_LONG":0.45,"SCALP_LONG":0.65,"PERP_LONG":0.50,"PERP_SHORT":1.00},
        "CHOP":{"PUMP_LONG":0.60,"SCALP_LONG":1.00,"PERP_LONG":0.72,"PERP_SHORT":0.72},
        "HIGH_VOL":{"PUMP_LONG":0.35,"SCALP_LONG":0.50,"PERP_LONG":0.55,"PERP_SHORT":0.55},
        "WARMUP":{"PUMP_LONG":0.70,"SCALP_LONG":0.70,"PERP_LONG":0.70,"PERP_SHORT":0.70},
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
    strategies=["PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
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
    if strategy=="PUMP_LONG":return nz(c.get("pump_score"))
    if strategy=="SCALP_LONG":return nz(c.get("scalp_score"))
    if strategy=="PERP_LONG":return nz(c.get("long_score"))
    if strategy=="PERP_SHORT":return nz(c.get("short_score"))
    return max(nz(c.get("long_score")),nz(c.get("short_score")),nz(c.get("pump_score")),nz(c.get("scalp_score")))

def base_threshold(strategy):
    return {
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
    strategies=["PUMP_LONG","SCALP_LONG","PERP_LONG","PERP_SHORT"]
    return {
        "enabled":b("adaptive_enabled"),
        "risk_multiplier":risk_multiplier(),
        "health":{s:strategy_health(s) for s in strategies},
        "optimizers":{s:optimizer_report(s) for s in strategies}
    }

def gate(c,strategy=None):
    if b("killed"): return False,"kill switch"
    if not b("bot_enabled"): return False,"bot stopped"
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]: return False,"loss pause"
    if strategy:
        spu=runtime["strategy_pauses"].get(strategy)
        if spu and datetime.now(timezone.utc)<spu:return False,"strategy adaptive pause"
    if c.get("perp_eligible"):
        if c.get("data_source")=="VELOCITY" and c.get("open_interest_usd",0) < f("min_perp_oi_usd"):
            return False,"low perp open interest"
        if c.get("data_source")=="VELOCITY" and c.get("direction_edge",0) < f("min_direction_edge"):
            return False,"weak directional edge"
        if abs(c.get("funding_rate",0)) > f("max_abs_funding_rate"):
            return False,"extreme funding"
        if b("block_extreme_volatility") and c.get("volatility_regime")=="EXTREME":
            return False,"extreme volatility"
    else:
        if c["liquidity"] < f("min_liquidity"): return False,"low liquidity"
    if c["market_risk"] < f("min_market_risk"): return False,"market risk gate"
    if strategy and strategy_side(strategy)=="SHORT" and not c.get("perp_eligible"):
        return False,"short unavailable for spot-only token"
    with SessionLocal() as s:
        if s.scalar(select(Position).where(Position.mint==c["mint"])): return False,"already open"
        if len(s.scalars(select(Position)).all()) >= i("max_positions"): return False,"max positions"
    cd=runtime["cooldowns"].get(c["mint"])
    if cd and time.time()<cd:return False,"cooldown"
    start=f("start_balance")
    if today_realized() <= -(start*f("daily_loss_limit_pct")/100): return False,"daily loss limit"
    if strategy:
        pok,preason,pdetail=portfolio_gate(c,strategy)
        if not pok:
            runtime["last_portfolio_block"]={
                "time":datetime.now(timezone.utc).isoformat(),
                "symbol":c.get("symbol"),"strategy":strategy,"reason":preason,"detail":pdetail
            }
            return False,preason
    return True,"ok"

def open_position(c,strategy):
    ok,reason=gate(c,strategy)
    if not ok:return False,reason
    m=metrics()
    leverage=max(1.0,min(2.0,strategy_leverage(strategy)))
    rm=risk_multiplier()
    pw=portfolio_weight(strategy)
    risk_budget=m["equity"]*f("risk_pct")/100*rm*pw
    collateral=risk_budget/max((f("stop_loss_pct")/100)*leverage,0.001)
    collateral=min(collateral,m["equity"]*f("max_position_pct")/100,f("cash"))
    if collateral<5:return False,"position too small"
    open_fee=collateral*leverage*f("execution_cost_pct")/100
    cash_need=collateral+open_fee
    if cash_need>f("cash"): return False,"cash"
    setv("cash",f("cash")-cash_need)
    now=datetime.now(timezone.utc)
    with SessionLocal() as s:
        pos=Position(
            mint=c["mint"],symbol=c["symbol"],name=c["name"],strategy=strategy,
            entry_price=c["price"],last_price=c["price"],peak_price=c["price"],
            initial_notional=collateral,remaining_cost=collateral,locked_pnl=-open_fee,
            opened_at=now
        )
        s.add(pos);s.flush()
        s.add(PositionFeature(
            position_id=pos.id,strategy=strategy,entry_score=strategy_entry_score(strategy,c),
            long_score=nz(c.get("long_score")),short_score=nz(c.get("short_score")),
            pump_score=nz(c.get("pump_score")),scalp_score=nz(c.get("scalp_score")),
            market_risk=nz(c.get("market_risk")),direction_edge=nz(c.get("direction_edge")),
            funding_rate=nz(c.get("funding_rate")),open_interest_usd=nz(c.get("open_interest_usd")),
            volatility_regime=str(c.get("volatility_regime") or "UNKNOWN"),created_at=now
        ))
        s.commit()
    return True,"opened"

def partial_sell(p, c, fraction):
    fraction=min(fraction,p.remaining_cost/max(p.initial_notional,1e-9))
    cost_basis=min(p.initial_notional*fraction,p.remaining_cost)
    if cost_basis<=0:return
    side=strategy_side(p.strategy)
    leverage=strategy_leverage(p.strategy)
    raw=directional_raw_return(p.entry_price,c["price"],side)
    gross_return=max(0.0,cost_basis*(1+raw*leverage))
    fee=cost_basis*leverage*f("execution_cost_pct")/100
    net=max(0.0,gross_return-fee)
    pnl=net-cost_basis
    setv("cash",f("cash")+net)
    p.remaining_cost-=cost_basis
    p.locked_pnl+=pnl

def close_position(p,c,reason):
    side=strategy_side(p.strategy)
    leverage=strategy_leverage(p.strategy)
    raw=directional_raw_return(p.entry_price,c["price"],side)
    gross_return=max(0.0,p.remaining_cost*(1+raw*leverage))
    fee=p.remaining_cost*leverage*f("execution_cost_pct")/100
    net=max(0.0,gross_return-fee)
    rem_pnl=net-p.remaining_cost
    total=p.locked_pnl+rem_pnl
    setv("cash",f("cash")+net)
    pnl_pct=total/max(p.initial_notional,1e-9)*100
    closed=datetime.now(timezone.utc)
    with SessionLocal() as s:
        obj=s.get(Position,p.id)
        if not obj:return
        pf=s.scalar(select(PositionFeature).where(PositionFeature.position_id==p.id))
        tr=Trade(mint=p.mint,symbol=p.symbol,strategy=p.strategy,pnl=total,pnl_pct=pnl_pct,
                 reason=reason,opened_at=p.opened_at,closed_at=closed)
        s.add(tr);s.flush()
        if pf:
            s.add(TradeFeature(
                trade_id=tr.id,strategy=p.strategy,entry_score=pf.entry_score,
                market_risk=pf.market_risk,direction_edge=pf.direction_edge,
                funding_rate=pf.funding_rate,open_interest_usd=pf.open_interest_usd,
                volatility_regime=pf.volatility_regime,pnl=total,pnl_pct=pnl_pct,closed_at=closed
            ))
            s.delete(pf)
        s.delete(obj);s.commit()
    runtime["cooldowns"][p.mint]=time.time()+i("cooldown_minutes")*60
    maybe_pause_strategy(p.strategy)
    if consecutive_losses()>=i("max_consecutive_losses"):
        runtime["pause_until"]=datetime.now(timezone.utc)+timedelta(minutes=i("loss_pause_minutes"))

def manage_positions():
    cands={x["mint"]:x for x in runtime["candidates"]}
    with SessionLocal() as s:
        pos=s.scalars(select(Position)).all()
        for p in pos:s.expunge(p)
    for p in pos:
        c=cands.get(p.mint)
        if not c:continue
        with SessionLocal() as s:
            obj=s.get(Position,p.id)
            if not obj:continue
            side=strategy_side(obj.strategy)
            obj.last_price=c["price"]
            if side=="SHORT":
                obj.peak_price=min(obj.peak_price,c["price"])
            else:
                obj.peak_price=max(obj.peak_price,c["price"])

            ret=directional_return_pct(obj,c["price"])
            peak=directional_return_pct(obj,obj.peak_price)
            pull=ret-peak
            prevliq=runtime["prev_liq"].get(obj.mint,c["liquidity"])
            reason=None

            if ret<=-f("stop_loss_pct"):
                reason="STOP_LOSS"
            elif not str(obj.strategy).startswith("PERP_") and prevliq>0 and c["liquidity"]<prevliq*.65:
                reason="LIQUIDITY_DROP"
            elif side=="LONG" and c["buy_pressure"]<24 and ret>0:
                reason="MOMENTUM_EXIT"
            elif side=="SHORT" and c["buy_pressure"]>76 and ret>0:
                reason="SHORT_SQUEEZE_EXIT"
            else:
                if ret>=8 and not obj.tp1: partial_sell(obj,c,.15);obj.tp1=True
                if ret>=15 and not obj.tp2: partial_sell(obj,c,.20);obj.tp2=True
                if ret>=25 and not obj.tp3: partial_sell(obj,c,.25);obj.tp3=True
                trail=None
                if peak>=60:trail=-14
                elif peak>=35:trail=-11
                elif peak>=20:trail=-8
                elif peak>=8:trail=-5
                if trail is not None and pull<=trail:reason="TRAILING_EXIT"
            s.commit()
            s.expunge(obj)
        if reason:close_position(obj,c,reason)

def choose_entry():
    if not b("bot_enabled") or b("killed"):return
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]:return

    opportunities=[]
    for c in runtime["candidates"]:
        if c.get("perp_eligible"):
            if c.get("data_source")=="VELOCITY":
                if c.get("direction")=="LONG" and c["long_score"]>=effective_threshold("PERP_LONG",c):
                    opportunities.append((c["long_score"]+c.get("direction_edge",0)*.25,c,"PERP_LONG"))
                elif c.get("direction")=="SHORT" and c["short_score"]>=effective_threshold("PERP_SHORT",c):
                    opportunities.append((c["short_score"]+c.get("direction_edge",0)*.25,c,"PERP_SHORT"))
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

    weighted=[]
    for score,c,strategy in opportunities:
        pw=portfolio_weight(strategy)
        weighted.append((score*max(pw,0.01),score,pw,c,strategy))
    weighted.sort(key=lambda x:x[0],reverse=True)
    for weighted_score,score,pw,c,strategy in weighted:
        ok,_=gate(c,strategy)
        if ok:
            open_position(c,strategy)
            return

async def engine_loop():
    runtime["loop_alive"]=True
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Trader-Paper/4.5"}) as client:
        addresses=[];boosts={};last_discovery=0
        while True:
            try:
                now=time.time()
                if now-last_discovery>60 or not addresses:
                    addresses,boosts=await discover(client);last_discovery=now
                spot_pairs=await fetch_pairs(client,addresses,boosts)
                velocity_raw=await fetch_velocity_markets(client)
                if velocity_raw:
                    perp_pairs=[perp_intelligence(v) for v in velocity_raw]
                else:
                    perp_pairs=await fetch_legacy_perp_pairs(client)

                merged={c["mint"]:c for c in spot_pairs}
                for c in perp_pairs:merged[c["mint"]]=c
                pairs=list(merged.values())
                pairs.sort(key=lambda x:max(x["pump_score"],x["scalp_score"],x["long_score"],x["short_score"]),reverse=True)
                if pairs:
                    runtime["candidates"]=pairs
                    runtime["last_refresh"]=datetime.now(timezone.utc).isoformat()
                    manage_positions()
                    choose_entry()
                    record_equity_snapshot()
                    record_market_snapshots(pairs)
                    for c in pairs:runtime["prev_liq"][c["mint"]]=c.get("liquidity",0)
                runtime["last_error"]=None if pairs else runtime["last_error"]
            except Exception as e:
                runtime["last_error"]=str(e)
            await asyncio.sleep(15)

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine_loop())

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
    min_liquidity: Optional[float]=None
    min_market_risk: Optional[float]=None
    execution_cost_pct: Optional[float]=None
    cooldown_minutes: Optional[int]=None

class WatchIn(BaseModel):
    mint:str

@app.get("/")
def root():
    return {"name":"NOVA Trader Cloud","version":APP_VERSION,"mode":"V4.5 PORTFOLIO BRAIN + CORRELATION GUARD / PAPER ONLY","docs":"/docs"}

@app.get("/health")
def health():
    return {"ok":True,"loop_alive":runtime["loop_alive"],"last_refresh":runtime["last_refresh"],
            "velocity_source_ok":runtime["velocity_source_ok"],"last_velocity_refresh":runtime["last_velocity_refresh"],
            "error":runtime["last_error"]}

@app.get("/api/dashboard")
def dashboard():
    with SessionLocal() as s:
        trades=s.scalars(select(Trade).order_by(Trade.id.desc()).limit(50)).all()
    return {
        "version":APP_VERSION,
        "mode":"PAPER",
        "bot_enabled":b("bot_enabled"),"killed":b("killed"),
        "pause_until":runtime["pause_until"].isoformat() if runtime["pause_until"] else None,
        "last_refresh":runtime["last_refresh"],"last_error":runtime["last_error"],
        "velocity_source_ok":runtime["velocity_source_ok"],
        "last_velocity_refresh":runtime["last_velocity_refresh"],
        "metrics":metrics(),
        "equity_curve":equity_curve(120),
        "adaptive":adaptive_status(),
        "research_status":snapshot_stats(),
        "portfolio":portfolio_status(),
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
                "min_liquidity","min_market_risk","execution_cost_pct","cooldown_minutes"
            ]},
            "block_extreme_volatility":b("block_extreme_volatility"),
            "adaptive_enabled":b("adaptive_enabled"),
            "portfolio_brain_enabled":b("portfolio_brain_enabled")
        }
    }

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
    elif action=="stop":
        setv("bot_enabled","false")
    elif action=="kill":
        setv("bot_enabled","false");setv("killed","true")
    elif action=="reset-paper":
        with SessionLocal() as s:
            s.query(PositionFeature).delete();s.query(TradeFeature).delete();s.query(Position).delete();s.query(Trade).delete();s.query(EquityPoint).delete();s.commit()
        setv("cash",getv("start_balance"));setv("bot_enabled","false");setv("killed","false")
        runtime["cooldowns"].clear();runtime["strategy_pauses"].clear();runtime["last_portfolio_block"]=None;runtime["pause_until"]=None
    else: raise HTTPException(400,"Unknown action")
    return {"ok":True,"action":action}

@app.post("/api/settings")
def settings(data:SettingsIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    for k,v in data.model_dump(exclude_none=True).items():setv(k,v)
    return {"ok":True}

@app.post("/api/watch")
def watch(data:WatchIn, x_nova_key:Optional[str]=Header(None)):
    auth(x_nova_key)
    mint=data.mint.strip()
    if len(mint)<30:raise HTTPException(400,"Invalid mint")
    vals=[x for x in getv("watchlist","").split(",") if x]
    if mint not in vals:vals.append(mint)
    setv("watchlist",",".join(vals[-30:]))
    return {"ok":True}
