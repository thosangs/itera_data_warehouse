SET NOCOUNT ON;

DECLARE @customers_src BIGINT = (SELECT COUNT(*) FROM source.customers);
DECLARE @customers_dw BIGINT = (SELECT COUNT(*) FROM dw.customers WHERE is_active = 1);
DECLARE @orders_src BIGINT = (SELECT COUNT(*) FROM source.orders);
DECLARE @orders_dw BIGINT = (SELECT COUNT(*) FROM dw.orders WHERE is_active = 1);

INSERT INTO metadata.universe_compare_log(table_name, source_count, dw_count, source_checksum, dw_checksum, status)
VALUES
('customers', @customers_src, @customers_dw, @customers_src, @customers_dw, CASE WHEN @customers_src = @customers_dw THEN 'OK' ELSE 'MISMATCH' END),
('orders', @orders_src, @orders_dw, @orders_src, @orders_dw, CASE WHEN @orders_src = @orders_dw THEN 'OK' ELSE 'MISMATCH' END);


