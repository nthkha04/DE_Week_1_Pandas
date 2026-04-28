Hôm nay (Thứ 2 - Tuần 1): ETL Pandas cơ bản - Làm sạch có chủ đích & Xử lý Data Grain

① Tool/Kỹ thuật hôm nay giải quyết bài toán gì cụ thể?
Sử dụng Pandas để xây dựng pipeline ETL cơ bản cho file 56MB.

Ở bước Transform 1: Dùng .drop_duplicates(), .dropna() và .fillna() để làm sạch dữ liệu rác.

Ở bước Transform 2: Dùng .groupby().agg() để gộp nhóm đa chiều, giải quyết bài toán đếm đơn hàng thực tế trong tệp dữ liệu bán lẻ có quan hệ 1-N (1 đơn có nhiều items).

② Có cách nào khác làm điều tương tự không?
Có thể load file thô thẳng vào Database (PostgreSQL) rồi dùng SQL để làm sạch (dùng COALESCE để xử lý NULL) và tổng hợp (dùng COUNT(DISTINCT)). Hoặc viết script bằng PySpark nếu data ở mức Big Data.

③ Trade-off (đánh đổi) & Bài học cốt lõi là gì?

Bài học Cleaning: Không bao giờ dùng .dropna() mù quáng cho cả bảng vì sẽ làm mất dữ liệu oan. Phải làm sạch có chủ đích: chỉ drop dòng rỗng ở các cột Key (mã đơn), và fill số 0 ở các cột Metric (doanh thu).

Bài học Data Grain (Độ hạt dữ liệu): Dữ liệu thô thường bị nhân bản dòng nếu khách mua nhiều món. Dùng .count() sẽ đếm sai số lượng đơn (gây lạm phát dữ liệu). Bắt buộc dùng .nunique() để đếm duy nhất mã đơn.

Bài học Cạm bẫy Aggregation (Hệ quả của Data JOIN): Khi dataset là một bảng đã được gộp sẵn (denormalized), phải cẩn trọng với hàm sum(). Nếu tính tổng trên cột đại diện cho cả đơn hàng (payment_value), doanh thu sẽ bị nhân khống lên nhiều lần do các dòng lặp. Phải tính sum() trên cột giá trị đơn lẻ của từng item (price). Luôn kiểm tra ý nghĩa của từng cột trước khi gom nhóm.

Trade-off của Pandas: Code cực nhanh, thao tác ma trận rất gọn, nhưng kiến trúc In-memory bắt buộc load tất cả lên RAM. Với file 56MB thì chạy tốt, nhưng nếu file là 50GB thì cách làm này sẽ gây sập RAM (OOM - Out of Memory).

## Thứ 2 — Tuần 1 (Day 1 - ETL Pipeline hoàn chỉnh)

### Đã làm:
- Build ETL pipeline functional style end-to-end
- Pydantic validate từng record, dead-letter → errors.csv
- Output: top_states_revenue.parquet + monthly_growth.parquet

### Bài học:
- payment_value sai data grain vì bị nhân N lần (item-level dataset) -> use dùng sum(price) từng item
- nunique(order_id) thay vì count() để đếm đơn hàng thực ( order lặp lại khi mua nhiều sản phẩm)
- validate() không crash pipeline — record lỗi ghi ra errors.csv
- Period (unsupported types) phải convert sang str trước khi ghi Parquet 