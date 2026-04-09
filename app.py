import streamlit as st
import pandas as pd
from datetime import datetime, date
from itertools import groupby
import db
import data_service as ds
import excel_export as ex

st.set_page_config(
    page_title="选股追踪系统",
    page_icon="📈",
    layout="wide",
)

st.markdown("""
<style>
thead tr th { background-color: #1F3864 !important; color: white !important; }
</style>
""", unsafe_allow_html=True)

# ── 初始化数据库（幂等） ───────────────────────────────────
@st.cache_resource
def _init():
    db.init_db()

_init()

PERIODS = ["5日", "10日", "1月", "2月", "3月"]


# ── 格式化工具 ────────────────────────────────────────────
def fmt_pct(tup):
    if not isinstance(tup, tuple) or tup[0] is None:
        return "—"
    val, status = tup
    prefix = "▶ " if status == "进行中" else ""
    return f"{prefix}{val:+.2f}%"


# ════════════════════════════════════════════════════════
# 页面标题
# ════════════════════════════════════════════════════════
st.title("📈 选股追踪系统")
st.caption("买入价 = 选股日下一交易日开盘价 ｜ ▶ = 进行中（截至今日）｜ 红涨绿跌")

# ── 全局数据（页面只查一次，避免连接池耗尽） ────────────
all_selections  = db.get_all_selections()
all_date_options = db.get_select_dates()

tab1, tab2, tab3 = st.tabs(["📥 录入选股", "📊 持仓看板", "📋 统计分析"])


# ════════════════════════════════════════════════════════
# Tab 1：录入选股
# ════════════════════════════════════════════════════════
with tab1:
    st.subheader("录入选股记录")

    col1, col2 = st.columns([1, 2])
    with col1:
        select_date = st.date_input(
            "选股日期", value=date.today(), max_value=date.today()
        )
        note = st.text_input("备注标签（可选）", placeholder="趋势突破、低估值…")

    with col2:
        codes_input = st.text_area(
            "股票代码（每行一个）",
            height=160,
            placeholder="SH600519\nSZ000001\n300750\n600036.SH",
            help="支持同花顺格式 SH600519 / 纯6位 600519 / 标准格式 600519.SH"
        )

    if st.button("✅ 提交并拉取数据", type="primary"):
        if not codes_input.strip():
            st.warning("请输入至少一个股票代码")
        else:
            valid, invalid = ds.parse_codes(codes_input)
            if invalid:
                st.warning(f"无法识别，已跳过：{', '.join(invalid)}")
            if valid:
                with st.spinner(f"正在拉取 {len(valid)} 只股票数据，请稍候…"):
                    results = ds.fetch_selection_data(
                        select_date.strftime("%Y-%m-%d"), valid, note
                    )
                ok   = [r for r in results if r["status"] == "ok"]
                err  = [r for r in results if r["status"] == "error"]
                skip = [r for r in results if r["status"] == "skip"]
                if ok:
                    st.success(
                        f"✅ 成功录入 {len(ok)} 只：" +
                        "、".join(f"{r['name']}({r['code']})" for r in ok)
                    )
                if skip:
                    st.info(
                        f"⏭️ 跳过 {len(skip)} 只（选股日+代码+备注完全相同）：" +
                        "、".join(f"{r['name']}({r['code']})" for r in skip)
                    )
                for r in err:
                    st.error(f"❌ {r['code']}：{r['msg']}")

    # 已录入记录管理
    st.divider()
    st.subheader("已录入记录")
    sels = all_selections
    if sels:
        df_m = pd.DataFrame(sels)[
            ["id", "select_date", "buy_date", "code", "name", "buy_price", "note"]
        ]
        df_m.columns = ["ID", "选股日", "买入日", "代码", "名称", "买入价", "备注"]
        st.dataframe(df_m, use_container_width=True, hide_index=True)

        st.divider()
        dc1, dc2 = st.columns(2)

        with dc1:
            st.markdown("**按选股日批量删除**")
            date_options = all_date_options
            date_labels  = [f"{r['select_date']}（{r['cnt']} 只）" for r in date_options]
            if date_labels:
                chosen_label = st.selectbox("选择要删除的选股日", date_labels, key="del_date")
                chosen_date  = date_options[date_labels.index(chosen_label)]["select_date"]
                if st.button("🗑️ 删除该日所有记录", type="primary"):
                    n = db.delete_by_date(chosen_date)
                    st.success(f"已删除 {chosen_date} 的 {n} 条记录")
                    st.rerun()

        with dc2:
            st.markdown("**按 ID 删除单条记录**")
            del_id = st.number_input("输入记录 ID", min_value=1, step=1)
            if st.button("🗑️ 删除该条记录"):
                db.delete_selection(int(del_id))
                st.success(f"已删除 ID={del_id}")
                st.rerun()
    else:
        st.info("暂无记录，请先录入选股")


# ════════════════════════════════════════════════════════
# Tab 2：持仓看板
# ════════════════════════════════════════════════════════
with tab2:
    st.subheader("持仓看板")

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        f_start = st.date_input("选股日 从", value=None, key="fs")
    with fc2:
        f_end   = st.date_input("选股日 至", value=None, key="fe")
    with fc3:
        st.write("")
        st.write("")
        do_refresh = st.button("🔄 更新数据", type="primary")

    if do_refresh:
        with st.spinner("正在从 Tushare 拉取最新价格…"):
            n = ds.refresh_prices()
        st.success(f"已更新 {n} 条记录的价格数据")
        st.rerun()

    sels = list(all_selections)  # 使用全局缓存
    if f_start:
        sels = [s for s in sels if s["select_date"] >= f_start.strftime("%Y%m%d")]
    if f_end:
        sels = [s for s in sels if s["select_date"] <= f_end.strftime("%Y%m%d")]

    if not sels:
        st.info("暂无数据，请先在「录入选股」页面录入")
    else:
        today_str = datetime.today().strftime("%Y%m%d")

        # 构建展示行
        display_rows = []
        raw_rows     = []   # 保留原始tuple，供Excel用
        for sel in sels:
            metrics = ds.calc_metrics(sel, today_str)
            base = {
                "代码":   sel["code"],
                "名称":   sel["name"] or "",
                "选股日": sel["select_date"],
                "买入日": sel["buy_date"] or "",
                "买入价": f"{sel['buy_price']:.2f}" if sel["buy_price"] else "—",
                "备注":   sel["note"] or "",
            }
            raw = {
                "股票代码":   sel["code"],
                "股票名称":   sel["name"] or "",
                "选股日":     sel["select_date"],
                "买入日":     sel["buy_date"] or "",
                "买入价(元)": sel["buy_price"],
                "备注":       sel["note"] or "",
            }
            for p in PERIODS:
                tup = metrics.get(f"{p}涨幅",     (None, ""))
                htup= metrics.get(f"{p}最高涨幅", (None, ""))
                base[f"{p}涨幅"]     = fmt_pct(tup)
                base[f"{p}最高涨幅"] = fmt_pct(htup)
                raw[f"{p}涨幅"]      = tup
                raw[f"{p}最高涨幅"]  = htup
            display_rows.append(base)
            raw_rows.append(raw)

        display_cols = (
            ["代码", "名称", "选股日", "买入日", "买入价"]
            + [f"{p}涨幅"     for p in PERIODS]
            + [f"{p}最高涨幅" for p in PERIODS]
            + ["备注"]
        )
        st.dataframe(
            pd.DataFrame(display_rows)[display_cols],
            use_container_width=True,
            hide_index=True,
            height=580,
        )

        # Excel 下载
        st.divider()
        if st.button("📥 生成 Excel"):
            xlsx = ex.build_excel(raw_rows)
            st.download_button(
                label="⬇️ 下载 Excel",
                data=xlsx,
                file_name=f"选股追踪_{datetime.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


# ════════════════════════════════════════════════════════
# Tab 3：统计分析
# ════════════════════════════════════════════════════════
with tab3:
    st.subheader("统计分析")

    sels = all_selections
    if not sels:
        st.info("暂无数据")
    else:
        today_str = datetime.today().strftime("%Y%m%d")
        all_m = []
        for sel in sels:
            m = ds.calc_metrics(sel, today_str)
            m["select_date"] = sel["select_date"]
            all_m.append(m)

        def _stats(subset):
            r = {}
            for p in PERIODS:
                vals  = [m[f"{p}涨幅"][0]     for m in subset
                         if isinstance(m.get(f"{p}涨幅"), tuple)
                         and m[f"{p}涨幅"][0] is not None]
                highs = [m[f"{p}最高涨幅"][0] for m in subset
                         if isinstance(m.get(f"{p}最高涨幅"), tuple)
                         and m[f"{p}最高涨幅"][0] is not None]
                r[f"{p}均涨幅"] = f"{sum(vals)/len(vals):+.2f}%"  if vals  else "—"
                r[f"{p}均最高"] = f"{sum(highs)/len(highs):+.2f}%" if highs else "—"
                r[f"{p}胜率"]   = f"{sum(1 for v in vals if v>0)/len(vals)*100:.0f}%" if vals else "—"
            return r

        # 全局摘要
        st.markdown("### 全部选股汇总")
        total = _stats(all_m)
        cols  = st.columns(5)
        for col, p in zip(cols, PERIODS):
            col.metric(f"{p} 均涨幅", total[f"{p}均涨幅"])
        st.markdown("#### 各周期胜率")
        cols2 = st.columns(5)
        for col, p in zip(cols2, PERIODS):
            col.metric(f"{p} 胜率", total[f"{p}胜率"])

        st.divider()

        # 按选股日分组
        st.markdown("### 按选股日分组")
        stat_rows = []
        for dk, grp in groupby(
            sorted(all_m, key=lambda x: x["select_date"]),
            key=lambda x: x["select_date"]
        ):
            g = list(grp)
            r = {"选股日": dk, "股票数": len(g)}
            r.update(_stats(g))
            stat_rows.append(r)

        show_cols = (["选股日", "股票数"]
                     + [f"{p}均涨幅" for p in PERIODS]
                     + [f"{p}胜率"   for p in PERIODS])
        st.dataframe(
            pd.DataFrame(stat_rows)[show_cols],
            use_container_width=True,
            hide_index=True,
        )
