PIPELINE_CONFIG = {
    "input_path":         "olist_sales.csv",
    "output_top_states":  "top_states_revenue.parquet",
    "output_monthly":     "monthly_growth.parquet",
    "error_path":         "errors.csv",
    "log_path":           "pipeline.log",
    "required_cols": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "customer_state",
        "price", "freight_value",
    ],
}