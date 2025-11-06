{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là Nguồn, Kho/Region, IndustryCode, và ngày ETL
        unique_key=['data_date', 'source', 'magcode', 'region', 'industrycode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    StockBalances_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__StockBalances') }}),
    MPLmagaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }}),
    magaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__magaz') }}),
    MPLmagazMPLConfig_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagazMPLConfig') }}),
    MPL_WarehouseStorageQuota_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPL_WarehouseStorageQuota') }}),

    -- 0.1 Filtered Stock Balances (Lọc theo ngày ETL)
    FilteredStockBalances AS (
        SELECT J.*, g.Region, g.int_regio, g.ManagerGroup
        FROM StockBalances_Source J
        JOIN MPLmagaz_Source g ON J.Warehouse = g.magcode AND g.Division = 101 AND g.int_regio IS NOT NULL AND g.Region IS NOT NULL AND g.ManagerGroup = 'MPL'
        -- Lọc Incremental
        {% if is_incremental() %}
            -- Lấy tồn kho của ngày ETL
            WHERE J.Date = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    ),

    -- 1. CTE: Tương đương với #temptable (Tính tồn kho trung bình theo ngày)
    temptable AS (
        SELECT 
            gb.Region, 
            TRIM(gb.Warehouse) AS magcode, 
            P.IndustryCode, 
            SUM(gb.Quantity) AS Quantity, 
            SUM(gb.Quantity * COALESCE(TRY_CAST(P.UserField_01 AS DOUBLE), 0.0)) AS Volumns,
            P.UserYesNo_05,
            P.Assortment,
            gb.int_regio
        FROM (
            SELECT 
                X.Warehouse, X.Region, X.ItemCode, 
                AVG(X.Qty) AS Quantity, -- Lấy Qty trung bình
                X.int_regio
            FROM (
                SELECT 
                    J.Warehouse, g.Region, J.ItemCode, 
                    SUM(J.Quantity) AS Qty, -- Tồn kho cho mỗi ItemCode tại một kho
                    g.int_regio
                FROM FilteredStockBalances J
                JOIN MPLmagaz_Source g ON J.Warehouse = g.magcode 
                LEFT JOIN MDataItems_Source I ON J.ItemCode = I.ItemCode
                WHERE J.Warehouse IS NOT NULL -- Giữ lại logic này nếu cần
                GROUP BY J.Warehouse, J.ItemCode, g.Region, g.int_regio
            ) X
            GROUP BY X.Warehouse, X.ItemCode, X.Region, X.int_regio
        ) GB
        JOIN MDataItems_Source P ON GB.ItemCode = P.ItemCode
        WHERE
            COALESCE(P.IndustryCode, 'SAMPLE') <> 'SAMPLE'
        GROUP BY 
            gb.Region, gb.Warehouse, P.IndustryCode, P.UserYesNo_05, P.Assortment, gb.int_regio
    ),

    -- 3. CTE: Truy vấn SELECT đầu tiên (Dữ liệu Tồn kho)
    Result1 AS (
        SELECT 
            'StockData' AS Source, U.Region, TRIM(U.magcode) AS magcode, U.IndustryCode, 
            U.Quantity, U.Volumns,
            CAST(NULL AS DOUBLE) AS Acreage, CAST(NULL AS DOUBLE) AS MaxSizeVolumn,
            CAST(NULL AS DOUBLE) AS MagazVolumn, CAST(NULL AS INTEGER) AS SYear,
            CAST(NULL AS DOUBLE) AS VolumnTarget,
            U.int_regio,
            U.UserYesNo_05 AS IsItemOn_Cover,
            (CASE 
                WHEN U.Assortment <= 350 THEN 'Product'
                WHEN (U.Assortment = 500 OR (U.Assortment = 900 AND U.IndustryCode IS NOT NULL)) THEN 'Part'
                ELSE ''
            END) AS optionview_Cover,
            U.magcode AS Warehouse_Cover
        FROM temptable U
    ),

    -- 4. CTE: Truy vấn SELECT thứ hai (Cấu hình kho)
    Result2 AS (
        SELECT 
            'WarehouseConfig' AS Source,
            g.Region, g.magcode, CAST(NULL AS VARCHAR) AS IndustryCode,
            CAST(NULL AS DOUBLE) AS Quantity, CAST(NULL AS DOUBLE) AS Volumns,
            SUM(COALESCE(XS.Acreage, G.Acreage)) AS Acreage, 
            SUM(COALESCE(XS.MaxSizeVolumn, G.MaxSizeVolumn)) AS MaxSizeVolumn,
            SUM(COALESCE(XS.MagazVolumn, 0)) AS MagazVolumn,
            CAST(NULL AS INTEGER) AS SYear, CAST(NULL AS DOUBLE) AS VolumnTarget,
            g.int_regio, CAST(NULL AS BOOLEAN) AS IsItemOn_Cover, CAST(NULL AS VARCHAR) AS optionview_Cover,
            g.magcode AS Warehouse_Cover
        FROM MPLmagaz_Source g
        JOIN magaz_Source gx ON g.magcode = gx.magcode
        -- T-SQL OUTER APPLY (XS)
        LEFT JOIN LATERAL (
            SELECT 
                X.Acreage, X.MaxSizeVolumn, X.MagazVolumn, X.Magcode,
                ROW_NUMBER() OVER(PARTITION BY X.Magcode ORDER BY X.FromDate DESC) as rn
            FROM MPLmagazMPLConfig_Source X 
            WHERE X.Active = TRUE
        ) XS ON g.magcode = XS.Magcode AND XS.rn = 1
        WHERE g.Division = 101 AND g.ManagerGroup ='MPL' AND g.Region IS NOT NULL
          AND (COALESCE(XS.Acreage, G.Acreage) + COALESCE(XS.MaxSizeVolumn, G.MaxSizeVolumn)) <> 0
        GROUP BY g.Region, g.magcode, g.int_regio
    ),

    -- 5. CTE: Truy vấn SELECT thứ ba (Hạn ngạch lưu trữ)
    Result3 AS (
        SELECT 
            'StorageQuota' AS Source, U.Region, CAST(NULL AS VARCHAR) AS magcode, 
            U.IndustryCode, CAST(NULL AS DOUBLE) AS Quantity, CAST(NULL AS DOUBLE) AS Volumns,
            CAST(NULL AS DOUBLE) AS Acreage, CAST(NULL AS DOUBLE) AS MaxSizeVolumn,
            CAST(NULL AS DOUBLE) AS MagazVolumn, U.SYear, U.VolumnTarget,
            CAST(NULL AS VARCHAR) AS int_regio, CAST(NULL AS BOOLEAN) AS IsItemOn_Cover,
            CAST(NULL AS VARCHAR) AS optionview_Cover, CAST(NULL AS VARCHAR) AS Warehouse_Cover
        FROM MPL_WarehouseStorageQuota_Source U
        -- Lọc Incremental (Lấy dữ liệu của năm ETL)
        {% if is_incremental() %}
            WHERE U.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
        {% endif %}
    )

-- 6. Gộp tất cả kết quả
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM Result1 T
UNION ALL
SELECT 
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date, CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.* FROM Result2 T
UNION ALL
SELECT 
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date, CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.* FROM Result3 T
ORDER BY Source, magcode, IndustryCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}