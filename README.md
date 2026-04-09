# 选股追踪系统

主观选股效果追踪工具。支持多周期涨幅分析、历史记录永久保存、Excel导出。

**技术栈**：Streamlit Cloud（免费）+ Supabase PostgreSQL（免费）+ Tushare

---

## 文件结构

```
stock_tracker/
├── app.py                  # Streamlit 主程序
├── db.py                   # PostgreSQL 数据库操作
├── data_service.py         # Tushare 数据拉取 & 涨幅计算
├── excel_export.py         # Excel 导出
├── requirements.txt        # Python 依赖
├── secrets.toml.example    # Secrets 配置模板（参考用）
└── .gitignore              # 防止 secrets 泄露
```

---

## 部署步骤

### 第一步：上传代码到 GitHub

1. 打开你的 GitHub 仓库（stock-tracker）
2. 把以上所有文件上传到仓库根目录
3. **确认 `.streamlit/secrets.toml` 没有被上传**（.gitignore 已保护）

### 第二步：在 Streamlit Cloud 部署

1. 打开 [share.streamlit.io](https://share.streamlit.io)
2. 点 **New app**
3. 填写：
   - Repository：选择你的 `stock-tracker` 仓库
   - Branch：`main`
   - Main file path：`app.py`
4. 点 **Advanced settings** → 找到 **Secrets** 输入框
5. 把以下内容填入（替换为你的真实信息）：

```toml
TUSHARE_TOKEN = "你的真实tushare_token"

[database]
host     = "db.xxxxxxxxxx.supabase.co"
port     = 5432
dbname   = "postgres"
user     = "postgres"
password = "你的supabase数据库密码"
```

6. 点 **Deploy**，等待约1-2分钟完成部署

### 第三步：验证

部署完成后访问分配的网址，进入「录入选股」Tab，
录入一只股票测试，若成功拉取数据说明部署正常。

---

## 涨幅计算规则

| 周期 | 口径         | 说明               |
|------|--------------|--------------------|
| 5日  | 固定5个交易日 | A股一周            |
| 10日 | 固定10个交易日| A股两周            |
| 1月  | 自然月        | 买入日+1月顺延交易日|
| 2月  | 自然月        | 同上               |
| 3月  | 自然月        | 同上               |

- **买入价**：选股日下一交易日开盘价
- **收盘涨幅**：到期日收盘价 / 买入价 - 1
- **最高涨幅**：区间最高价 / 买入价 - 1
- **▶ 进行中**：显示截至今日的当前值

---

## 安全说明

- Tushare Token 和数据库密码**只存在 Streamlit Cloud Secrets**，不进入任何代码文件
- `.gitignore` 已屏蔽本地 secrets 文件，不会被意外提交
- 即使仓库公开，Token 也完全安全
