"""
db.py - PostgreSQL (Supabase) 版本
每次操作独立连接，用完立即关闭，不用连接池
"""
import psycopg2
import psycopg2.extras
import streamlit as st


def get_conn():
    cfg = st.secrets["database"]
    return psycopg2.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 5432)),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        sslmode="prefer",
        connect_timeout=10,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def init_db():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS selections (
                    id          SERIAL PRIMARY KEY,
                    select_date TEXT NOT NULL,
                    buy_date    TEXT,
                    code        TEXT NOT NULL,
                    name        TEXT,
                    buy_price   REAL,
                    note        TEXT,
                    created_at  TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS price_cache (
                    id          SERIAL PRIMARY KEY,
                    code        TEXT NOT NULL,
                    trade_date  TEXT NOT NULL,
                    open_price  REAL,
                    high_price  REAL,
                    close_price REAL,
                    UNIQUE(code, trade_date)
                );
                CREATE TABLE IF NOT EXISTS trade_calendar (
                    trade_date  TEXT PRIMARY KEY,
                    is_open     INTEGER
                );
            """)
        conn.commit()


def upsert_price(code, trade_date, open_, high, close):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO price_cache(code, trade_date, open_price, high_price, close_price)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (code, trade_date) DO UPDATE
                  SET open_price=EXCLUDED.open_price,
                      high_price=EXCLUDED.high_price,
                      close_price=EXCLUDED.close_price
            """, (code, trade_date, float(open_), float(high), float(close)))
        conn.commit()


def upsert_prices_batch(records: list[tuple]):
    if not records:
        return
    records = [(r[0], r[1], float(r[2]), float(r[3]), float(r[4])) for r in records]
    with get_conn() as conn:
        with conn.cursor() as c:
            psycopg2.extras.execute_values(c, """
                INSERT INTO price_cache(code, trade_date, open_price, high_price, close_price)
                VALUES %s
                ON CONFLICT (code, trade_date) DO UPDATE
                  SET open_price=EXCLUDED.open_price,
                      high_price=EXCLUDED.high_price,
                      close_price=EXCLUDED.close_price
            """, records)
        conn.commit()


def get_prices_multi(codes: list[str], start: str, end: str) -> dict[str, dict]:
    """一次查询拿所有股票的价格，返回 {code: {trade_date: row}}"""
    if not codes:
        return {}
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT code, trade_date, open_price AS open,
                       high_price AS high, close_price AS close
                FROM price_cache
                WHERE code = ANY(%s) AND trade_date>=%s AND trade_date<=%s
                ORDER BY code, trade_date
            """, (codes, start, end))
            rows = c.fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        d = dict(r)
        result.setdefault(d["code"], {})[d["trade_date"]] = d
    return result


def get_prices(code, start, end):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date, open_price AS open,
                       high_price AS high, close_price AS close
                FROM price_cache
                WHERE code=%s AND trade_date>=%s AND trade_date<=%s
                ORDER BY trade_date
            """, (code, start, end))
            rows = c.fetchall()
    return [dict(r) for r in rows]


def upsert_calendar(records: list[tuple]):
    if not records:
        return
    with get_conn() as conn:
        with conn.cursor() as c:
            psycopg2.extras.execute_values(c, """
                INSERT INTO trade_calendar(trade_date, is_open)
                VALUES %s ON CONFLICT (trade_date) DO NOTHING
            """, records)
        conn.commit()


def get_trade_days(start, end):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE is_open=1 AND trade_date>=%s AND trade_date<=%s
                ORDER BY trade_date
            """, (start, end))
            rows = c.fetchall()
    return [r["trade_date"] for r in rows]


def next_trade_day(date_str):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE is_open=1 AND trade_date>%s
                ORDER BY trade_date LIMIT 1
            """, (date_str,))
            row = c.fetchone()
    return row["trade_date"] if row else None


def calendar_count():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) AS cnt FROM trade_calendar")
            row = c.fetchone()
    return row["cnt"]


def is_duplicate(select_date, code, note):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id FROM selections
                WHERE select_date=%s AND code=%s AND note=%s LIMIT 1
            """, (select_date, code, note or ""))
            row = c.fetchone()
    return row is not None


def insert_selection(select_date, buy_date, code, name, buy_price, note):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO selections(select_date,buy_date,code,name,buy_price,note)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (select_date, buy_date, code, name,
                  float(buy_price) if buy_price else None, note or ""))
        conn.commit()


def get_all_selections():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM selections ORDER BY select_date DESC, code")
            rows = c.fetchall()
    return [dict(r) for r in rows]


def get_select_dates():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT select_date, COUNT(*) AS cnt
                FROM selections GROUP BY select_date
                ORDER BY select_date DESC
            """)
            rows = c.fetchall()
    return [dict(r) for r in rows]


def delete_selection(sel_id):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM selections WHERE id=%s", (sel_id,))
        conn.commit()


def delete_by_date(select_date):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM selections WHERE select_date=%s", (select_date,))
            deleted = c.rowcount
        conn.commit()
    return deleted
