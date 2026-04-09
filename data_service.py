"""
data_service.py  —  Tushare 数据拉取 & 涨幅计算
Token 从 st.secrets["TUSHARE_TOKEN"] 读取，不硬编码
"""
import tushare as ts
import streamlit as st
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import db

# ── Tushare 初始化 ────────────────────────────────────────
_pro = None

def get_pro():
    global _pro
    if _pro is None:
        token = st.secrets["TUSHARE_TOKEN"]
        ts.set_token(token)
        _pro = ts.pro_api()
    return _pro


# ── 代码标准化 ────────────────────────────────────────────
def normalize_code(raw: str) -> str | None:
    raw = raw.strip().upper()
    if not raw:
        return None
    # 同花顺格式：SH600519 / SZ000001
    if (raw.startswith("SH") or raw.startswith("SZ")) and len(raw) == 8:
        prefix = raw[:2]
        code6  = raw[2:]
        if code6.isdigit():
            return f"{code6}.{prefix}"
    # 已是标准格式：600519.SH / 000001.SZ
    if "." in raw:
        parts = raw.split(".")
        if len(parts) == 2 and len(parts[0]) == 6 and parts[0].isdigit():
            if parts[1] in ("SH", "SZ", "BJ"):
                return raw
            if parts[1] == "SS":
                return f"{parts[0]}.SH"
    # 纯6位数字
    if len(raw) == 6 and raw.isdigit():
        if raw.startswith("6") or raw.startswith("688") or raw.startswith("689"):
            return f"{raw}.SH"
        if raw.startswith("0") or raw.startswith("3"):
            return f"{raw}.SZ"
        if raw.startswith("8") or raw.startswith("4"):
            return f"{raw}.BJ"
    return None


def parse_codes(text: str) -> tuple[list[str], list[str]]:
    valid, invalid = [], []
    seen = set()
    for line in text.strip().splitlines():
        for part in line.replace(",", " ").replace("\t", " ").split():
            code = normalize_code(part)
            if code and code not in seen:
                valid.append(code)
                seen.add(code)
            elif not code:
                invalid.append(part)
    return valid, invalid


# ── 交易日历 ──────────────────────────────────────────────
def ensure_calendar():
    pro = get_pro()
    if db.calendar_count() < 200:
        this_year = datetime.today().year
        df = pro.trade_cal(
            exchange="SSE",
            start_date="20200101",
            end_date=f"{this_year + 1}1231"
        )
        records = [(row["cal_date"], 1 if row["is_open"] == 1 else 0)
                   for _, row in df.iterrows()]
        db.upsert_calendar(records)


# ── 价格数据 ──────────────────────────────────────────────
def fetch_price_range(code: str, start: str, end: str):
    """拉取价格并写入缓存，已有的跳过"""
    existing = {r["trade_date"] for r in db.get_prices(code, start, end)}
    trade_days = db.get_trade_days(start, end)
    if not set(trade_days) - existing:
        return  # 全部已缓存

    pro = get_pro()
    df = pro.daily(
        ts_code=code, start_date=start, end_date=end,
        fields="ts_code,trade_date,open,high,close"
    )
    if df is None or df.empty:
        return

    records = [
        (code, row["trade_date"], row["open"], row["high"], row["close"])
        for _, row in df.iterrows()
    ]
    db.upsert_prices_batch(records)


def get_stock_name(code: str) -> str:
    try:
        pro = get_pro()
        df = pro.stock_basic(ts_code=code, fields="ts_code,name")
        if not df.empty:
            return df.iloc[0]["name"]
    except Exception:
        pass
    return code


# ── 到期日计算 ────────────────────────────────────────────
def calc_expiry_date(buy_date: str, months: int) -> str:
    """自然月到期日，顺延至最近交易日"""
    dt = datetime.strptime(buy_date, "%Y%m%d")
    expiry = dt + relativedelta(months=months)
    expiry_str = expiry.strftime("%Y%m%d")
    look_ahead = (expiry + timedelta(days=10)).strftime("%Y%m%d")
    days = db.get_trade_days(expiry_str, look_ahead)
    return days[0] if days else expiry_str


def nth_trade_day_after(buy_date: str, n: int) -> str | None:
    """buy_date 之后第 n 个交易日"""
    look = (datetime.strptime(buy_date, "%Y%m%d") + timedelta(days=30)).strftime("%Y%m%d")
    days = db.get_trade_days(buy_date, look)
    # days[0] == buy_date 本身，所以第n个交易日是 days[n]
    return days[n] if len(days) > n else None


# ── 涨幅计算 ──────────────────────────────────────────────
def calc_metrics(sel: dict, today_str: str) -> dict:
    code      = sel["code"]
    buy_date  = sel["buy_date"]
    buy_price = sel["buy_price"]

    if not buy_date or not buy_price or buy_price == 0:
        return {}

    far_end = min(
        (datetime.strptime(buy_date, "%Y%m%d") + relativedelta(months=4)).strftime("%Y%m%d"),
        today_str
    )
    prices = db.get_prices(code, buy_date, far_end)
    if not prices:
        return {}

    price_map      = {p["trade_date"]: p for p in prices}
    all_trade_days = [p["trade_date"] for p in prices]
    result         = {}

    def pct(price):
        return round((price / buy_price - 1) * 100, 2)

    def compute(interval_days, target_date):
        """给定区间交易日列表，计算收盘涨幅和最高涨幅"""
        status = "进行中" if target_date > today_str else "已完成"
        days = [d for d in interval_days if d <= today_str] if status == "进行中" else interval_days
        if not days:
            return (None, status), (None, status)
        close = price_map[days[-1]]["close"]
        highs = [price_map[d]["high"] for d in days]
        return (pct(close), status), (pct(max(highs)), status)

    # 5日、10日（固定交易日）
    for n, label in [(5, "5日"), (10, "10日")]:
        target = nth_trade_day_after(buy_date, n)
        if target is None:
            result[f"{label}涨幅"] = result[f"{label}最高涨幅"] = (None, "未到期")
            continue
        interval = [d for d in all_trade_days if d > buy_date and d <= target]
        result[f"{label}涨幅"], result[f"{label}最高涨幅"] = compute(interval, target)

    # 1月、2月、3月（自然月）
    for m, label in [(1, "1月"), (2, "2月"), (3, "3月")]:
        expiry   = calc_expiry_date(buy_date, m)
        interval = [d for d in all_trade_days if d > buy_date and d <= expiry]
        result[f"{label}涨幅"], result[f"{label}最高涨幅"] = compute(interval, expiry)

    return result


# ── 批量录入 ──────────────────────────────────────────────
def fetch_selection_data(select_date: str, codes: list[str], note: str = "") -> list[dict]:
    ensure_calendar()
    pro   = get_pro()
    buy_date = db.next_trade_day(select_date.replace("-", ""))
    if not buy_date:
        return [{"code": c, "status": "error", "msg": "找不到下一交易日"} for c in codes]

    results = []
    for code in codes:
        try:
            name = get_stock_name(code)
            df = pro.daily(
                ts_code=code, start_date=buy_date, end_date=buy_date,
                fields="ts_code,trade_date,open,high,close"
            )
            if df is None or df.empty:
                results.append({"code": code, "name": name,
                                 "status": "error", "msg": "无买入日价格数据"})
                continue

            row       = df.iloc[0]
            buy_price = float(row["open"])
            db.upsert_price(code, buy_date, row["open"], row["high"], row["close"])

            # 拉后续3个月价格
            far = (datetime.strptime(buy_date, "%Y%m%d")
                   + relativedelta(months=3, days=10)).strftime("%Y%m%d")
            fetch_price_range(code, buy_date, far)

            select_date_str = select_date.replace("-", "")
            if db.is_duplicate(select_date_str, code, note):
                results.append({
                    "code": code, "name": name, "status": "skip",
                    "msg": "已存在相同选股日+代码+备注，已跳过"
                })
            else:
                db.insert_selection(
                    select_date_str, buy_date,
                    code, name, buy_price, note
                )
                results.append({
                    "code": code, "name": name, "status": "ok",
                    "buy_date": buy_date, "buy_price": buy_price
                })
        except Exception as e:
            results.append({"code": code, "status": "error", "msg": str(e)})

    return results


def refresh_prices() -> int:
    """更新所有进行中股票的最新价格"""
    ensure_calendar()
    today = datetime.today().strftime("%Y%m%d")
    sels  = db.get_all_selections()
    updated = 0
    for sel in sels:
        if not sel["buy_date"]:
            continue
        far = (datetime.strptime(sel["buy_date"], "%Y%m%d")
               + relativedelta(months=3, days=10)).strftime("%Y%m%d")
        end = min(far, today)
        if sel["buy_date"] <= today:
            fetch_price_range(sel["code"], sel["buy_date"], end)
            updated += 1
    return updated


def calc_all_metrics(sels: list[dict], today_str: str) -> list[dict]:
    """
    批量计算所有涨幅。
    用 get_prices_multi 一次查询拿所有价格，再用 get_trade_days 拿交易日历。
    总共只有 2 次数据库连接。
    """
    if not sels:
        return []

    valid = [s for s in sels if s.get("buy_date")]
    if not valid:
        return [{} for _ in sels]

    min_date = min(s["buy_date"] for s in valid)
    max_buy  = max(s["buy_date"] for s in valid)
    far_end  = min(
        (datetime.strptime(max_buy, "%Y%m%d") + relativedelta(months=4, days=10)).strftime("%Y%m%d"),
        today_str
    )

    # 1次查询：所有交易日
    all_trade_days = db.get_trade_days(min_date, far_end)

    # 1次查询：所有股票价格
    all_codes = list({s["code"] for s in valid})
    price_by_code = db.get_prices_multi(all_codes, min_date, far_end)

    results = []
    for sel in sels:
        code      = sel.get("code")
        buy_date  = sel.get("buy_date")
        buy_price = sel.get("buy_price")

        if not buy_date or not buy_price or buy_price == 0:
            results.append({})
            continue

        price_map = price_by_code.get(code, {})
        if not price_map:
            results.append({})
            continue

        sel_days = [d for d in all_trade_days if d >= buy_date and d in price_map]

        def pct(price, bp=buy_price):
            return round((price / bp - 1) * 100, 2)

        def compute(interval_days, target_date):
            status = "进行中" if target_date > today_str else "已完成"
            days = [d for d in interval_days if d <= today_str] if status == "进行中" else list(interval_days)
            if not days:
                return (None, status), (None, status)
            close = price_map[days[-1]]["close"]
            highs = [price_map[d]["high"] for d in days]
            return (pct(close), status), (pct(max(highs)), status)

        result = {}

        for n, label in [(5, "5日"), (10, "10日")]:
            after = [d for d in sel_days if d > buy_date]
            target = after[n - 1] if len(after) >= n else None
            if target is None:
                result[f"{label}涨幅"] = result[f"{label}最高涨幅"] = (None, "未到期")
                continue
            interval = [d for d in sel_days if d > buy_date and d <= target]
            result[f"{label}涨幅"], result[f"{label}最高涨幅"] = compute(interval, target)

        for m, label in [(1, "1月"), (2, "2月"), (3, "3月")]:
            expiry   = calc_expiry_date(buy_date, m)
            interval = [d for d in sel_days if d > buy_date and d <= expiry]
            result[f"{label}涨幅"], result[f"{label}最高涨幅"] = compute(interval, expiry)

        results.append(result)

    return results
