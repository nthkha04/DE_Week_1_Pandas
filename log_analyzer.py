"""
Web Log Analyzer — functional pipeline
raw log → parse → filter → top IPs → SQLite → báo cáo text
"""
import re
import json
import sqlite3
import logging
import sys
from pathlib import Path
from typing import Optional
import pandas as pd
from pydantic import BaseModel, field_validator, ValidationError

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("log_analyzer.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── Pydantic Schema ───────────────────────────────────────────
class LogRecord(BaseModel):
    ip:        str
    timestamp: str
    method:    str
    path:      str
    status:    int
    size:      Optional[int] = None

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: int) -> int:
        if not (100 <= v <= 599):
            raise ValueError(f"Status code lạ: {v}")
        return v

    @field_validator("method")
    @classmethod
    def valid_method(cls, v: str) -> str:
        allowed = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
        if v not in allowed:
            raise ValueError(f"HTTP method lạ: {v}")
        return v


# ── Parse ─────────────────────────────────────────────────────
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) (?P<size>\S+)'
)


def parse_apache_log(line: str) -> Optional[dict]:
    """
    Parse 1 dòng Apache log → dict.
    Trả None nếu không match pattern.

    Tại sao Optional[dict]?
    → Log thực tế có dòng comment, dòng trống, format lạ.
      Trả None thay vì raise = không crash pipeline.
    """
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    d = match.groupdict()
    d["status"] = int(d["status"])
    d["size"]   = int(d["size"]) if d["size"] != "-" else None
    return d


# ── Load + Validate ───────────────────────────────────────────
def load_logs(log_path: str,
              dead_letter_path: str = "dead_letter.jsonl") -> pd.DataFrame:
    """
    Đọc file log, parse từng dòng, validate Pydantic.
    Record lỗi → dead_letter.jsonl.

    Tại sao jsonl không phải csv?
    → Append từng dòng hiệu quả hơn csv khi file log lớn.
      jsonl = mỗi dòng là 1 JSON object độc lập.
    """
    logger.info(f"[LOAD_LOGS] Đọc: {log_path}")

    if not Path(log_path).exists():
        raise FileNotFoundError(f"Không tìm thấy file log: {log_path}")

    valid_records = []
    dead_letters  = []

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue

            parsed = parse_apache_log(line)
            if parsed is None:
                dead_letters.append({
                    "raw":    line.strip(),
                    "_error": "parse_failed"
                })
                continue

            try:
                LogRecord(**parsed)
                valid_records.append(parsed)
            except ValidationError as e:
                parsed["_error"] = str(e.errors()[0]["msg"])
                dead_letters.append(parsed)

    if dead_letters:
        with open(dead_letter_path, "w", encoding="utf-8") as f:
            for rec in dead_letters:
                f.write(json.dumps(rec) + "\n")
        logger.warning(
            f"[LOAD_LOGS] {len(dead_letters)} lỗi → {dead_letter_path}"
        )

    logger.info(f"[LOAD_LOGS] Hợp lệ: {len(valid_records):,} records")
    return pd.DataFrame(valid_records) if valid_records else pd.DataFrame()


# ── Filter ────────────────────────────────────────────────────
def filter_errors(df: pd.DataFrame) -> pd.DataFrame:
    """Giữ lại chỉ các request lỗi 4xx và 5xx."""
    if df.empty or "status" not in df.columns:
        logger.warning("[FILTER] DataFrame rỗng hoặc thiếu cột status")
        return pd.DataFrame()

    df_errors = df[df["status"] >= 400].copy()
    logger.info(f"[FILTER] 4xx/5xx: {len(df_errors):,} records")
    return df_errors


# ── Aggregate ─────────────────────────────────────────────────
def top_ips(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Top N IP theo số request lỗi."""
    if df.empty:
        logger.warning("[TOP_IPS] DataFrame rỗng")
        return pd.DataFrame()

    df_top = (
        df.groupby("ip")
        .agg(error_count=("status", "count"))
        .reset_index()
        .sort_values("error_count", ascending=False)
        .head(n)
    )
    logger.info(f"[TOP_IPS] Top {n} IPs tính xong")
    return df_top


# ── SQLite ────────────────────────────────────────────────────
def save_to_sqlite(df: pd.DataFrame,
                   db_path: str = "logs.db",
                   table: str = "web_logs") -> None:
    """
    Lưu DataFrame vào SQLite.

    Tại sao SQLite không phải Parquet ở đây?
    → Sau khi lưu ta cần query thống kê bằng SQL ngay.
      SQLite = file-based database, không cần server,
      phù hợp local analysis.
    """
    if df.empty:
        logger.warning("[SQLITE] DataFrame rỗng, bỏ qua")
        return

    logger.info(f"[SQLITE] Ghi vào {db_path} / table={table}")
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table, conn, if_exists="replace", index=False)
        conn.close()
        logger.info(f"[SQLITE] ✅ {len(df):,} records")
    except Exception as e:
        logger.error(f"[SQLITE] Lỗi: {e}")
        raise


# ── Query + Report ────────────────────────────────────────────
def query_stats(db_path: str = "logs.db") -> str:
    """Query thống kê từ SQLite, trả về báo cáo text."""
    try:
        conn = sqlite3.connect(db_path)

        df_status = pd.read_sql("""
            SELECT status, COUNT(*) as count
            FROM web_logs
            GROUP BY status
            ORDER BY count DESC
        """, conn)

        df_top = pd.read_sql("""
            SELECT ip, COUNT(*) as error_count
            FROM web_logs
            WHERE status >= 400
            GROUP BY ip
            ORDER BY error_count DESC
            LIMIT 5
        """, conn)

        df_methods = pd.read_sql("""
            SELECT method, COUNT(*) as count
            FROM web_logs
            GROUP BY method
            ORDER BY count DESC
        """, conn)

        conn.close()

        report = "\n".join([
            "=" * 40,
            "BÁO CÁO WEB LOG",
            "=" * 40,
            "\n── Requests theo Status Code ──",
            df_status.to_string(index=False),
            "\n── Top 5 IP lỗi nhiều nhất ──",
            df_top.to_string(index=False),
            "\n── Requests theo HTTP Method ──",
            df_methods.to_string(index=False),
            "=" * 40,
        ])
        return report

    except Exception as e:
        logger.error(f"[QUERY] Lỗi: {e}")
        raise


# ── Sample log generator ──────────────────────────────────────
def generate_sample_log(path: str = "sample.log",
                        n_lines: int = 500) -> None:
    """Tạo file log mẫu để test pipeline khi không có log thực."""
    import random
    from datetime import datetime, timedelta

    ips      = ["192.168.1.1", "10.0.0.5", "172.16.0.3", "8.8.8.8", "1.1.1.1"]
    methods  = ["GET", "POST", "PUT", "DELETE"]
    paths    = ["/api/v1/orders", "/health", "/login",
                "/static/app.js", "/admin"]
    statuses = [200, 200, 200, 301, 400, 401, 403, 404, 500, 502]

    base_time = datetime(2024, 1, 1)
    lines = []
    for i in range(n_lines):
        ip     = random.choice(ips)
        ts     = (base_time + timedelta(seconds=i * 10)).strftime(
                     "%d/%b/%Y:%H:%M:%S +0000")
        method = random.choice(methods)
        paths   = random.choice(paths)
        status = random.choice(statuses)
        size   = random.randint(200, 5000) if status != 204 else "-"
        lines.append(
            f'{ip} - - [{ts}] "{method} {paths} HTTP/1.1" {status} {size}'
        )

    with open(path, "w") as f:
        f.write("\n".join(lines))
    logger.info(f"[SAMPLE] Tạo {n_lines} dòng → {path}")


# ── Main pipeline ─────────────────────────────────────────────
def run_log_pipeline(log_path: str = "sample.log") -> None:
    logger.info("=" * 40)
    logger.info("LOG ANALYZER BẮT ĐẦU")
    logger.info("=" * 40)

    # Tạo sample log nếu chưa có
    if not Path(log_path).exists():
        generate_sample_log(log_path)

    # Extract + Validate
    df_logs = load_logs(log_path)
    if df_logs.empty:
        logger.error("[PIPELINE] Không có record hợp lệ, dừng pipeline")
        return

    # Filter 4xx/5xx
    df_errors = filter_errors(df_logs)

    # Top IPs
    df_top = top_ips(df_errors, n=10)
    if not df_top.empty:
        df_top.to_parquet("top_ips.parquet", index=False)
        logger.info("[TOP_IPS] Đã ghi top_ips.parquet")
        print("\n── Top 10 IPs lỗi ──")
        print(df_top.to_string(index=False))

    # Lưu toàn bộ logs vào SQLite
    save_to_sqlite(df_logs)

    # Query + báo cáo
    report = query_stats()
    print(report)

    with open("report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    logger.info("[REPORT] Đã ghi report.txt")

    logger.info("LOG ANALYZER HOÀN THÀNH ✅")


if __name__ == "__main__":
    run_log_pipeline()