"""
db.py  —  PostgreSQL (Supabase) 版本
使用连接池避免频繁建立新连接导致超限
"""
import psycopg2
import psycopg2.extras
import psycopg2.pool
import streamlit as st


# ── 连接池（单例，缓存在 st.cache_resource） ──────────────
@st.cache_resource
def _get_pool():
    cfg = st.secrets["database"]
    return psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host=cfg["host"],
        port=int(cfg.get("port", 5432)),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        sslmode="prefer",
        options="-c default_transaction_isolation='read committed'",
    )


def get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def release_conn(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        pass


# ── 建表（幂等） ──────────────────────────────────────────
def init_db():
    conn = get_conn()
    try:
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
    finally:
        release_conn(conn)


# ── 价格缓存 ──────────────────────────────────────────────
def upsert_price(code, trade_date, open_, high, close):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO price_cache(code, trade_date, open_price, high_price, close_price)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (code, trade_date) DO UPDATE
                  SET open_price  = EXCLUDED.open_price,
                      high_price  = EXCLUDED.high_price,
                      close_price = EXCLUDED.close_price
            """, (code, trade_date, float(open_), float(high), float(close)))
        conn.commit()
    finally:
        release_conn(conn)


def upsert_prices_batch(records: list[tuple]):
    if not records:
        return
    records = [(r[0], r[1], float(r[2]), float(r[3]), float(r[4])) for r in records]
    conn = get_conn()
    try:
        with conn.cursor() as c:
            psycopg2.extras.execute_values(c, """
                INSERT INTO price_cache(code, trade_date, open_price, high_price, close_price)
                VALUES %s
                ON CONFLICT (code, trade_date) DO UPDATE
                  SET open_price  = EXCLUDED.open_price,
                      high_price  = EXCLUDED.high_price,
                      close_price = EXCLUDED.close_price
            """, records)
        conn.commit()
    finally:
        release_conn(conn)


def get_prices(code, start, end):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date, open_price AS open, high_price AS high, close_price AS close
                FROM price_cache
                WHERE code=%s AND trade_date>=%s AND trade_date<=%s
                ORDER BY trade_date
            """, (code, start, end))
            rows = c.fetchall()
        return [dict(r) for r in rows]
    finally:
        release_conn(conn)


# ── 交易日历 ──────────────────────────────────────────────
def upsert_calendar(records: list[tuple]):
    if not records:
        return
    conn = get_conn()
    try:
        with conn.cursor() as c:
            psycopg2.extras.execute_values(c, """
                INSERT INTO trade_calendar(trade_date, is_open)
                VALUES %s
                ON CONFLICT (trade_date) DO NOTHING
            """, records)
        conn.commit()
    finally:
        release_conn(conn)


def get_trade_days(start, end):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE is_open=1 AND trade_date>=%s AND trade_date<=%s
                ORDER BY trade_date
            """, (start, end))
            rows = c.fetchall()
        return [r["trade_date"] for r in rows]
    finally:
        release_conn(conn)


def next_trade_day(date_str):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE is_open=1 AND trade_date>%s
                ORDER BY trade_date LIMIT 1
            """, (date_str,))
            row = c.fetchone()
        return row["trade_date"] if row else None
    finally:
        release_conn(conn)


def calendar_count():
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) AS cnt FROM trade_calendar")
            row = c.fetchone()
        return row["cnt"]
    finally:
        release_conn(conn)


# ── 选股记录 ──────────────────────────────────────────────
def is_duplicate(select_date, code, note):
    """同选股日 + 同代码 + 同备注 → True（跳过）"""
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT id FROM selections
                WHERE select_date=%s AND code=%s AND note=%s
                LIMIT 1
            """, (select_date, code, note or ""))
            row = c.fetchone()
        return row is not None
    finally:
        release_conn(conn)


def insert_selection(select_date, buy_date, code, name, buy_price, note):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO selections(select_date, buy_date, code, name, buy_price, note)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (select_date, buy_date, code, name,
                  float(buy_price) if buy_price else None, note or ""))
        conn.commit()
    finally:
        release_conn(conn)


def get_all_selections():
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT * FROM selections ORDER BY select_date DESC, code")
            rows = c.fetchall()
        return [dict(r) for r in rows]
    finally:
        release_conn(conn)


def get_select_dates():
    """返回所有选股日及各日数量，供批量删除使用"""
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT select_date, COUNT(*) AS cnt
                FROM selections
                GROUP BY select_date
                ORDER BY select_date DESC
            """)
            rows = c.fetchall()
        return [dict(r) for r in rows]
    finally:
        release_conn(conn)


def delete_selection(sel_id):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM selections WHERE id=%s", (sel_id,))
        conn.commit()
    finally:
        release_conn(conn)


def delete_by_date(select_date):
    """删除某个选股日的所有记录，返回删除数量"""
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM selections WHERE select_date=%s", (select_date,))
            deleted = c.rowcount
        conn.commit()
        return deleted
    finally:
        release_conn(conn)
