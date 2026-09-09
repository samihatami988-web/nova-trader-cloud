import os, asyncio, math, time
from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, String, Float, Integer, Boolean, DateTime, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

APP_VERSION = "4.1.0"
DEX = "https://api.dexscreener.com"

# Paper-perp universe. Short entries are only allowed for explicitly eligible
# markets. These mints are used only as public price/signal proxies.
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
    "cooldowns": {},
    "pause_until": None,
    "loop_alive": False,
}

def auth(x_nova_key: Optional[str]):
    if not x_nova_key or x_nova_key != ADMIN_KEY:
        raise HTTPException(401, "Invalid NOVA admin key")

def nz(v, d=0.0):
    try: return float(v or d)
    except: return d

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
                    **sc
                })
        except Exception as e:
            runtime["last_error"]=f"pairs: {e}"
    pairs.sort(key=lambda x:max(x["pump_score"],x["scalp_score"]),reverse=True)
    return pairs[:60]

async def fetch_perp_pairs(client):
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
    return {
        "cash":cash,"equity":equity,"pnl":pnl,"pnl_pct":pnl/start*100 if start else 0,
        "trades":len(trades),"wins":len(wins),"losses":len(losses),
        "win_rate":winrate,"profit_factor":profit_factor,
        "open_positions":len(positions),
        "long":side_stats(long_trades),"short":side_stats(short_trades)
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

def gate(c,strategy=None):
    if b("killed"): return False,"kill switch"
    if not b("bot_enabled"): return False,"bot stopped"
    if runtime["pause_until"] and datetime.now(timezone.utc)<runtime["pause_until"]: return False,"loss pause"
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
    return True,"ok"

def open_position(c,strategy):
    ok,reason=gate(c,strategy)
    if not ok:return False,reason
    m=metrics()
    leverage=max(1.0,min(2.0,strategy_leverage(strategy)))
    risk_budget=m["equity"]*f("risk_pct")/100
    collateral=risk_budget/max((f("stop_loss_pct")/100)*leverage,0.001)
    collateral=min(collateral,m["equity"]*f("max_position_pct")/100,f("cash"))
    if collateral<5:return False,"position too small"
    open_fee=collateral*leverage*f("execution_cost_pct")/100
    cash_need=collateral+open_fee
    if cash_need>f("cash"): return False,"cash"
    setv("cash",f("cash")-cash_need)
    now=datetime.now(timezone.utc)
    with SessionLocal() as s:
        s.add(Position(
            mint=c["mint"],symbol=c["symbol"],name=c["name"],strategy=strategy,
            entry_price=c["price"],last_price=c["price"],peak_price=c["price"],
            initial_notional=collateral,remaining_cost=collateral,locked_pnl=-open_fee,
            opened_at=now
        ));s.commit()
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
        s.add(Trade(mint=p.mint,symbol=p.symbol,strategy=p.strategy,pnl=total,pnl_pct=pnl_pct,
                    reason=reason,opened_at=p.opened_at,closed_at=closed))
        s.delete(obj);s.commit()
    runtime["cooldowns"][p.mint]=time.time()+i("cooldown_minutes")*60
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
            if c["long_score"]>=f("min_long_score"):
                opportunities.append((c["long_score"],c,"PERP_LONG"))
            if c["short_score"]>=f("min_short_score"):
                opportunities.append((c["short_score"],c,"PERP_SHORT"))
        else:
            if c["pump_score"]>=f("min_pump_score"):
                opportunities.append((c["pump_score"],c,"PUMP_LONG"))
            if c["scalp_score"]>=f("min_scalp_score"):
                opportunities.append((c["scalp_score"],c,"SCALP_LONG"))

    opportunities.sort(key=lambda x:x[0],reverse=True)
    for score,c,strategy in opportunities:
        ok,_=gate(c,strategy)
        if ok:
            open_position(c,strategy)
            return

async def engine_loop():
    runtime["loop_alive"]=True
    async with httpx.AsyncClient(headers={"User-Agent":"NOVA-Trader-Paper/4.1"}) as client:
        addresses=[];boosts={};last_discovery=0
        while True:
            try:
                now=time.time()
                if now-last_discovery>60 or not addresses:
                    addresses,boosts=await discover(client);last_discovery=now
                spot_pairs=await fetch_pairs(client,addresses,boosts)
                perp_pairs=await fetch_perp_pairs(client)
                merged={c["mint"]:c for c in spot_pairs}
                for c in perp_pairs:merged[c["mint"]]=c
                pairs=list(merged.values())
                pairs.sort(key=lambda x:max(x["pump_score"],x["scalp_score"],x["long_score"],x["short_score"]),reverse=True)
                if pairs:
                    runtime["candidates"]=pairs
                    runtime["last_refresh"]=datetime.now(timezone.utc).isoformat()
                    manage_positions()
                    choose_entry()
                    for c in pairs:runtime["prev_liq"][c["mint"]]=c["liquidity"]
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
    min_liquidity: Optional[float]=None
    min_market_risk: Optional[float]=None
    execution_cost_pct: Optional[float]=None
    cooldown_minutes: Optional[int]=None

class WatchIn(BaseModel):
    mint:str

@app.get("/")
def root():
    return {"name":"NOVA Trader Cloud","version":APP_VERSION,"mode":"LONG + SHORT PAPER ONLY","docs":"/docs"}

@app.get("/health")
def health():
    return {"ok":True,"loop_alive":runtime["loop_alive"],"last_refresh":runtime["last_refresh"],"error":runtime["last_error"]}

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
        "metrics":metrics(),
        "positions":positions_with_marks(),
        "candidates":runtime["candidates"][:30],
        "trades":[{
            "id":t.id,"symbol":t.symbol,"strategy":t.strategy,"side":strategy_side(t.strategy),"pnl":t.pnl,
            "pnl_pct":t.pnl_pct,"reason":t.reason,"closed_at":t.closed_at.isoformat()
        } for t in trades],
        "settings":{k:float(getv(k)) for k in [
            "risk_pct","max_position_pct","max_positions","stop_loss_pct","daily_loss_limit_pct",
            "min_pump_score","min_scalp_score","min_long_score","min_short_score","perp_leverage","min_liquidity","min_market_risk","execution_cost_pct","cooldown_minutes"
        ]}
    }

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
            s.query(Position).delete();s.query(Trade).delete();s.commit()
        setv("cash",getv("start_balance"));setv("bot_enabled","false");setv("killed","false")
        runtime["cooldowns"].clear();runtime["pause_until"]=None
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
