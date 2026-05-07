import logging
import sys
from etl import (
    extract, validate, transform,
    calc_top_states, calc_revenue_growth,
    flag_outliers, load,
)
from etl.config import PIPELINE_CONFIG

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            PIPELINE_CONFIG["log_path"], encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run_pipeline(config: dict) -> None:
    logger.info("=" * 50)
    logger.info("PIPELINE BẮT ĐẦU")
    logger.info("=" * 50)

    df_raw   = extract(config["input_path"])
    df_valid = validate(
        df_raw,
        required_cols=config["required_cols"],
        error_path=config["error_path"],
    )
    df_clean = transform(df_valid)
    df_clean = flag_outliers(df_clean, col="price")

    df_states  = calc_top_states(df_clean)
    df_monthly = calc_revenue_growth(df_clean)

    load(df_states,  config["output_top_states"])
    load(df_monthly, config["output_monthly"])

    logger.info("PIPELINE HOÀN THÀNH ✅")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_pipeline(PIPELINE_CONFIG)