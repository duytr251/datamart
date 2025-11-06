{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là Năm, Tháng, Ngành hàng, và Kho
        unique_key=['data_date', 'year', 'month', 'industrycode', 'magcode_cover'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    gbkmut_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__gbkmut') }}),
    StockBalances_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__StockBalances') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__Items') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHIndustry') }}),
    SHSaleChannelCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHSaleChannelCostcenter') }}),
    SHSaleChannel_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHSaleChannel') }}),

    -- 0.1 Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT *
        FROM gbkmut_Source g
        WHERE SUBSTR(COALESCE(g.bud_vers,''), 1, 4) <> 'DBBH'
            AND g.aantal <> 0
            AND g.transtype = 'N'
            AND SUBSTR(TRIM(g.reknr), 1, 3) = '156'
        
        -- Lọc Incremental (Chỉ lấy giao dịch của tháng ETL hiện tại)
        {% if is_incremental() %}
            AND g.bkjrcode = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            AND TRIM(g.periode) = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS VARCHAR)
        {% endif %}
    ),

    -- 1. CTE: Tương đương với @tmp (Dữ liệu thô cho StockPosition, cần toàn bộ lịch sử)
    tmp AS (
        -- *** LƯU Ý: Phải sử dụng StockBalances_Source (toàn bộ lịch sử) ***
        SELECT 
            IND.IndustryCode,
            sb.Date,
            SUM(sb.Quantity) AS Quantity,
            YEAR(sb.Date) * 100 + MONTH(sb.Date) AS MONTHCAL
        FROM StockBalances_Source sb
        JOIN Items_Source i ON i.ItemCode = sb.ItemCode
        JOIN SHIndustry_Source IND ON i.Assortment BETWEEN IND.ItemGroupMin AND IND.ItemGroupMax
        WHERE 
            sb.Quantity <> 0
        GROUP BY 
            IND.IndustryCode, sb.Date
    ),

    -- 2. CTE: Tương đương với @StockPosition (Tồn kho lũy kế)
    StockPosition AS (
        SELECT 
            IndustryCode,
            MONTHCAL,
            SUM(Quantity) OVER (
                PARTITION BY IndustryCode 
                ORDER BY MONTHCAL
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS TotalQuantity,
            CAST(SUBSTR(CAST(MONTHCAL AS VARCHAR), 5, 2) AS INTEGER) AS MONTH
        FROM (
            SELECT 
                IndustryCode, 
                MONTHCAL, 
                SUM(Quantity) AS Quantity
            FROM tmp
            GROUP BY IndustryCode, MONTHCAL
        ) monthly_tmp
    ),

    -- 3. CTE: Tương đương với Subquery (P) (Kết quả chính)
    P AS (
        SELECT 
            Ind.IndustryCode,
            Ind.IndustryName,
            g.bkjrcode AS YEAR,
            COALESCE(TRY_CAST(NULLIF(TRIM(g.periode), '') AS INTEGER), 0) AS MONTH,
            ABS(SUM(CASE WHEN g.transsubtype = 'B' THEN g.aantal ELSE 0 END)) AS QuantityExp,
            st.TotalQuantity,
            SUM(CASE WHEN g.transsubtype = 'A' THEN g.aantal ELSE 0 END) AS QuantityImp,
            
            SUM(CASE WHEN g.transsubtype IN ('A','H') AND g.bkstnr_sub IS NOT NULL THEN ABS(g.aantal) * COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) ELSE 0 END) AS M3ImpNotIBT,
            SUM(CASE WHEN g.transsubtype IN ('A') AND g.bkstnr_sub IS NULL THEN ABS(g.aantal) * COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) ELSE 0 END) AS M3ImpIBT,
            SUM(CASE WHEN g.transsubtype = 'B' THEN ABS(g.aantal) * COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) ELSE 0 END) AS M3ExpNotIBT,
            SUM(CASE WHEN g.transsubtype = 'B' AND g.bkstnr_sub IS NULL THEN ABS(g.aantal) * COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) ELSE 0 END) AS M3ExpIBT,
            
            SC.ChannelCode AS ChannelCode_Cover,
            Ind.IndustryCode AS IndustryCode_Cover,
            g.warehouse AS Magcode_Cover
            
        FROM FilteredMaster g
        JOIN SHSaleChannelCostcenter_Source SCC ON g.kstplcode = SCC.CostCenter
        JOIN SHSaleChannel_Source SC ON SC.ChannelCode = SCC.ChannelCode
        JOIN Items_Source i ON i.ItemCode = g.artcode
        JOIN SHIndustry_Source Ind ON i.Assortment BETWEEN Ind.ItemGroupMin AND Ind.ItemGroupMax
        LEFT JOIN StockPosition st 
            ON st.IndustryCode = Ind.IndustryCode 
            AND (g.bkjrcode * 100 + COALESCE(TRY_CAST(NULLIF(TRIM(g.periode), '') AS INTEGER), 0)) = st.MONTHCAL
        GROUP BY 1, 2, 3, 4, 6, 7, 13, 14, 15
    )

-- 4. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    P.IndustryCode,
    P.IndustryName,
    P.YEAR,
    P.MONTH,
    P.QuantityExp,
    P.TotalQuantity,
    P.QuantityImp,
    P.M3ImpNotIBT,
    P.M3ImpIBT,
    P.M3ExpNotIBT,
    P.M3ExpIBT,
    COALESCE(P.QuantityExp / NULLIF(P.TotalQuantity, 0) * 100, 0) AS RatioExp,
    COALESCE(P.QuantityImp / NULLIF(P.QuantityExp, 0) * 100, 0) AS RatioImpExp,
    P.M3ImpNotIBT + P.M3ImpIBT AS TotalM3Imp,
    P.M3ExpNotIBT + P.M3ExpIBT AS TotalM3Exp,
    (P.M3ImpNotIBT + P.M3ImpIBT) - (P.M3ExpNotIBT + P.M3ExpIBT) AS DiffenceM3,
    COALESCE((P.M3ImpNotIBT + P.M3ImpIBT) / NULLIF((P.M3ExpNotIBT + P.M3ExpIBT), 0) * 100, 0) AS RatioM3ImpExp,
    
    P.ChannelCode_Cover,
    P.IndustryCode_Cover,
    P.Magcode_Cover
FROM P
ORDER BY P.YEAR, P.MONTH, P.IndustryCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}