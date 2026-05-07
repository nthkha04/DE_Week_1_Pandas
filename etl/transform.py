import pandas as pd
import logging
from pydantic import BaseModel, field_validator, ValidationError
from typing import Optional

logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────
class OrderSchema(BaseModel):
    order_id:                 str
    customer_id:              str
    order_status:             str
    order_purchase_timestamp: str
    customer_state:           str
    price:                    float
    freight_value:            float
    payment_value:            Optional[float] = None
    product_id:               Optional[str]   = None

    @field_validator("price", "freight_value")
    @classmethod
    def non_negative(cls, v):
        if v < 0:
            raise ValueError(f"Không được âm: {v}")
        return v

    @field_validator("order_status")
    @classmethod
    def valid_status(cls, v):
        allowed = {
            "delivered", "shipped", "canceled",
            "unavailable", "processing",
            "invoiced", "approved", "created",
        }
        if v not in allowed:
            raise ValueError(f"Status lạ: {v}")
        return v


# ── Validate ──────────────────────────────────────────────────
def validate(df: pd.DataFrame,
             required_cols: list,
             error_path: str = "errors.csv") -> pd.DataFrame:
    """Pydantic validate từng record. Lỗi → errors.csv, không crash pipeline."""
    logger.info("[VALIDATE] Bắt đầu...")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Thiếu columns: {missing}")

    validate_cols = required_cols + ["payment_value", "product_id"]
    validate_cols = [c for c in validate_cols if c in df.columns]

    valid_idx, errors = [], []

    for record in df[validate_cols].to_dict("records"):
        row_dict = {k: (None if pd.isna(v) else v)
                    for k, v in record.items()}
        try:
            OrderSchema(**row_dict)
            valid_idx.append(record.get("order_id"))
        except ValidationError as e:
            row_dict["_error"] = str(e.errors()[0]["msg"])
            errors.append(row_dict)

    # fix: track by position thay vì order_id để tránh mất index
    valid_mask = []
    error_set  = {r["order_id"] for r in errors if "_error" in r}

    df_valid = df[~df["order_id"].isin(error_set)].reset_index(drop=True)

    if errors:
        pd.DataFrame(errors).to_csv(error_path, index=False)
        logger.warning(f"[VALIDATE] {len(errors)} lỗi → {error_path}")

    logger.info(
        f"[VALIDATE] Hợp lệ: {len(df_valid):,}/{len(df):,} "
        f"({len(df_valid)/len(df)*100:.1f}%)"
    )
    return df_valid


# ── Transform ─────────────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Dedup + fillna + parse datetime + tạo year_month."""
    logger.info(f"[TRANSFORM] Input: {df.shape}")
    try:
        before = len(df)
        df = df.drop_duplicates(subset=["order_id", "product_id"]).copy()
        logger.info(f"[TRANSFORM] Dedup xóa: {before - len(df):,} dòng")

        df["price"]         = df["price"].fillna(0.0)
        df["freight_value"] = df["freight_value"].fillna(0.0)

        df["order_purchase_timestamp"] = pd.to_datetime(
            df["order_purchase_timestamp"], errors="coerce"
        )

        # Log NaT sau parse để không bị silent data loss
        nat_count = df["order_purchase_timestamp"].isna().sum()
        if nat_count:
            logger.warning(f"[TRANSFORM] {nat_count:,} dòng NaT sau parse datetime")

        df["year_month"] = df["order_purchase_timestamp"].dt.to_period("M")

        logger.info(f"[TRANSFORM] Output: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"[TRANSFORM] Lỗi: {e}")
        raise


# ── Aggregations ──────────────────────────────────────────────
def calc_top_states(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Top N tiểu bang theo tổng doanh thu."""
    df_states = (
        df.groupby("customer_state")
        .agg(
            total_orders  = ("order_id", "nunique"),
            total_revenue = ("price",    "sum"),
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(top_n)
    )
    logger.info(f"[TOP_STATES] shape: {df_states.shape}")
    return df_states


def calc_revenue_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Doanh thu + % tăng trưởng theo tháng."""
    df_monthly = (
        df.groupby("year_month")
        .agg(
            total_orders  = ("order_id", "nunique"),
            total_revenue = ("price",    "sum"),
        )
        .reset_index()
        .sort_values("year_month")
    )
    df_monthly["revenue_growth_pct"] = (
        df_monthly["total_revenue"]
        .pct_change().mul(100).round(2).fillna(0)
    )
    df_monthly["year_month"] = df_monthly["year_month"].astype(str)
    logger.info(f"[GROWTH] {len(df_monthly)} tháng")
    return df_monthly


def flag_outliers(df: pd.DataFrame, col: str, n_std: float = 3.0) -> pd.DataFrame:
    """Đánh dấu outlier bằng ±N std."""
    mean = df[col].mean()
    std  = df[col].std()
    df   = df.copy()
    df[f"{col}_is_outlier"] = (df[col] - mean).abs() > n_std * std
    logger.info(f"[OUTLIER] '{col}': {df[f'{col}_is_outlier'].sum():,} outliers")
    return df