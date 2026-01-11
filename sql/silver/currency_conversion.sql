CREATE OR REPLACE TABLE GOLD.fx_rates (
    currency STRING,
    rate_to_usd NUMBER(10,6)
);

INSERT INTO GOLD.fx_rates VALUES
('USD',1),
('INR',0.012),
('EUR',1.08);