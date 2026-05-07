import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def load(df: pd.DataFrame, out_path: str) -> None:
    """Ghi DataFrame ra Parquet."""
    logger.info(f"[LOAD] Ghi: {out_path}")
    try:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False, engine="pyarrow")
        kb = Path(out_path).stat().st_size / 1024
        logger.info(f"[LOAD] ✅ {out_path} ({len(df):,} rows, {kb:.1f} KB)")
    except Exception as e:
        logger.error(f"[LOAD] Lỗi: {e}")
        raise