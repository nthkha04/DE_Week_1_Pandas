import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract(path: str) -> pd.DataFrame:
    """Đọc CSV thô vào DataFrame. Không transform gì ở đây."""
    logger.info(f"[EXTRACT] Đọc file: {path}")
    try:
        df = pd.read_csv(path, low_memory=False)
        logger.info(f"[EXTRACT] Xong — shape: {df.shape}")
        return df
    except FileNotFoundError:
        logger.error(f"[EXTRACT] Không tìm thấy: {path}")
        raise