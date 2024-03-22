-- Drop table if it already exists to prevent conflicts
DROP TABLE IF EXISTS team_1.prod_data.store_data_with_retention_rate_version_2;

-- Create a table to store data with retention rates
CREATE TABLE team_1.prod_data.store_data_with_retention_rate_version_2 AS

-- Subquery to calculate retention rates for each store
WITH store_data AS (
    SELECT DISTINCT
        SUBSTRING(idstore, 3, 30) AS IDSTORE,
        store_commercial_type,
        store_management_type_name,
        store_country_code,
        store_city,
        CASE
            WHEN store_country_code = 'FR' AND (STORE_CITY = 'PARIS' OR STORE_RETAIL_TYPE_LABEL = 'PARIS') THEN 'IDF'
            WHEN store_country_code = 'FR' AND (STORE_CITY <> 'PARIS' AND STORE_RETAIL_TYPE_LABEL <> 'PARIS') THEN 'FR PROVINCE'
            WHEN store_country_code = 'GB' AND (STORE_CITY = 'LONDON' OR STORE_RETAIL_TYPE_LABEL = 'LONDON') THEN 'LONDON'
            WHEN store_country_code = 'GB' AND (STORE_CITY <> 'LONDON' AND STORE_RETAIL_TYPE_LABEL <> 'LONDON') THEN 'GB PROVINCE'
            ELSE '-'
        END AS STORE_GEOGRAPHICAL_AREA
    FROM
        master_sales_item_customer_store
    WHERE
        YEAR(dayofsale) <> '2025'
        AND idcustomer <> '1-0'
        AND season_code IN ('E23', 'H22', 'E22', 'H21')
),

all_distinct_store_customers AS (
    SELECT
        a.*,
        b.season_code
    FROM
        (
            SELECT DISTINCT
                SUBSTRING(idstore, 3, 30) AS IDSTORE,
                idcustomer
            FROM
                master_sales_item_customer_store
            WHERE
                YEAR(dayofsale) <> '2025'
                AND idcustomer <> '1-0'
                AND season_code IN ('E23', 'H22', 'E22', 'H21')
        ) a
    CROSS JOIN
        (
            SELECT DISTINCT
                season_code
            FROM
                master_sales_item_customer_store
            WHERE
                season_code IN ('E23', 'H22', 'E22', 'H21')
        ) b
),

pivoted_store_customer AS (
    SELECT DISTINCT
        SUBSTRING(a.idstore, 3, 30) AS IDSTORE,
        e23.idcustomer AS e23_idcustomer,
        h22.idcustomer AS h22_idcustomer,
        e22.idcustomer AS e22_idcustomer,
        h21.idcustomer AS h21_idcustomer
    FROM
        all_distinct_store_customers AS a
    LEFT JOIN
        (
            SELECT DISTINCT
                SUBSTRING(idstore, 3, 30) AS IDSTORE,
                idcustomer
            FROM
                master_sales_item_customer_store
            WHERE
                season_code = 'E22'
        ) E22 ON a.idstore = e22.idstore AND a.idcustomer = e22.idcustomer
    LEFT JOIN
        (
            SELECT DISTINCT
                SUBSTRING(idstore, 3, 30) AS IDSTORE,
                idcustomer
            FROM
                master_sales_item_customer_store
            WHERE
                season_code = 'E23'
        ) e23 ON a.idstore = e23.idstore AND a.idcustomer = e23.idcustomer
    LEFT JOIN
        (
            SELECT DISTINCT
                SUBSTRING(idstore, 3, 30) AS IDSTORE,
                idcustomer
            FROM
                master_sales_item_customer_store
            WHERE
                season_code = 'H22'
        ) h22 ON a.idstore = h22.idstore AND a.idcustomer = h22.idcustomer
    LEFT JOIN
        (
            SELECT DISTINCT
                SUBSTRING(idstore, 3, 30) AS IDSTORE,
                idcustomer
            FROM
                master_sales_item_customer_store
            WHERE
                season_code = 'H21'
        ) h21 ON a.idstore = h21.idstore AND a.idcustomer = h21.idcustomer
),

retention_rates AS (
    SELECT
        idstore,
        COUNT(DISTINCT e22_IDCUSTOMER) AS e22_IDCUSTOMERS,
        COUNT(DISTINCT h22_idcustomer) AS h22_idcustomers,
        COUNT(DISTINCT e23_idcustomer) AS e23_idcustomers,
        COUNT(DISTINCT CASE
            WHEN h21_idcustomer IS NOT NULL AND e22_idcustomer IS NOT NULL THEN e22_idcustomer
        END) AS e22_retained_customers,
        COUNT(DISTINCT CASE
            WHEN e22_idcustomer IS NOT NULL AND h22_idcustomer IS NOT NULL THEN h22_idcustomer
        END) AS h22_retained_customers,
        COUNT(DISTINCT CASE
            WHEN h22_idcustomer IS NOT NULL AND e23_idcustomer IS NOT NULL THEN e23_idcustomer
        END) AS e23_retained_customers,
        CASE
            WHEN COUNT(DISTINCT h22_idcustomer) > 0 THEN (COUNT(DISTINCT CASE
                WHEN h22_idcustomer IS NOT NULL THEN e23_idcustomer
            END) * 100.0) / COUNT(DISTINCT h22_idcustomer)
            ELSE 0
        END AS e23_retention_rate,
        CASE
            WHEN COUNT(DISTINCT e22_idcustomer) > 0 THEN (COUNT(DISTINCT CASE
                WHEN e22_idcustomer IS NOT NULL THEN h22_idcustomer
            END) * 100.0) / COUNT(DISTINCT e22_idcustomer)
            ELSE 0
        END AS h22_retention_rate,
        CASE
            WHEN COUNT(DISTINCT h21_idcustomer) > 0 THEN (COUNT(DISTINCT CASE
                WHEN h21_idcustomer IS NOT NULL THEN e22_idcustomer
            END) * 100.0) / COUNT(DISTINCT h21_idcustomer)
            ELSE 0
        END AS e22_retention_rate
    FROM
        pivoted_store_customer
    GROUP BY
        idstore
),

-- Combine store data with retention rates
SELECT
    a.*,
    NVL(e22_idcustomers, 0) AS e22_idcustomers,
    NVL(h22_idcustomers, 0) AS h22_idcustomers,
    NVL(e23_idcustomers, 0) AS e23_idcustomers,
    NVL(e22_retained_customers, 0) AS e22_retained_customers,
    NVL(h22_retained_customers, 0) AS h22_retained_customers,
    NVL(e23_retained_customers, 0) AS e23_retained_customers,
    NVL(e22_retention_rate, 0) AS e22_retention_rate,
    NVL(h22_retention_rate, 0) AS h22_retention_rate,
    NVL(e23_retention_rate, 0) AS e23_retention_rate
FROM
    store_data a
LEFT JOIN
    retention_rates b ON a.idstore = b.idstore;

-- Display store data with retention rates
SELECT DISTINCT
    STORE_COMMERCIAL_TYPE,
    STORE_MANAGEMENT_TYPE_NAME,
    STORE_COUNTRY_CODE,
    STORE_CITY,
    STORE_GEOGRAPHICAL_AREA,
    IDSTORE,
    E22_IDCUSTOMERS,
    H22_IDCUSTOMERS,
    E23_IDCUSTOMERS,
    E22_RETAINED_CUSTOMERS,
    H22_RETAINED_CUSTOMERS,
    E23_RETAINED_CUSTOMERS,
    E22_RETENTION_RATE,
    H22_RETENTION_RATE,
    E23_RETENTION_RATE
FROM
    team_1.prod_data.store_data_with_retention_rate_version_2
ORDER BY
    IDSTORE;
