{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày tháng (SaleMonth/Year), CostCenter, và ItemCode
        unique_key=['data_date', 'salemonth', 'saleyear', 'costcenter', 'itemcode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    BISaleByMonth_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__BISaleByMonth') }}),
    SHSaleCostCenterFull_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHSaleCostCenterFull') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__items') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHIndustry') }}),
    ItemAssortment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemAssortment') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemClasses') }}),

    -- Lọc Incremental cho bảng SaleByMonth
    FilteredSaleData AS (
        SELECT 
            S.*,
            -- Thêm cột Division từ logic cũ
            (CASE cc.Region 
                WHEN 'MB' THEN 101 
                WHEN 'MT' THEN 200 
                WHEN 'MN' THEN 302 
            END) AS Division
        FROM BISaleByMonth_Source S
        LEFT JOIN SHSaleCostCenterFull_Source cc ON S.CostCenter = cc.CostCenter
        WHERE
            S.CostCenter <> 'SHMN'
            
            -- Lọc Incremental (Chỉ lấy dữ liệu của tháng/năm mới nhất)
            {% if is_incremental() %}
                -- Giả định ngày ETL chạy hàng tháng, chỉ cần lấy tháng/năm của ngày ETL
                AND S.SaleMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
                AND S.SaleYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            {% endif %}
    ),

    -- 1. CTE: Tương đương với [View_BISaleByMonth_TrueCC]
    View_BISaleByMonth_TrueCC AS (
        SELECT 
            S.SBMID, 
            S.ItemCode, 
            S.InvDebNr, 
            S.OrdDebNr, 
            S.CostCenter, 
            S.PersonID, 
            S.SaleMonth, 
            S.SaleYear, 
            S.SaleQuantity, 
            S.SaleAmount,
            (S.CostAmount + COALESCE(S.CostAmountPromotion, 0)) AS CostAmount, 
            S.CostAmount AS CostAmountFake, 
            COALESCE(S.CostAmountPromotion, 0) AS CostAmountPromotion,
            S.Division
        FROM 
            FilteredSaleData S
    )

-- 2. SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    v.Division,
    v.CostCenter,
    c.ReportName AS ReportName_STT,
    c.ChannelCode,
    v.ItemCode,
    i.Description_0,
    CAST(i.Assortment AS VARCHAR(50)) AS Assortment,
    ia.Description AS Assortment_Description,
    si.IndustryCode,
    si.IndustryName,
    i.Class_01,
    ic.Description AS Class_01_Description,
    CASE 
        WHEN i.UserYesNo_05 = TRUE THEN 'OFF'
        ELSE i.UserField_06
    END AS thuoc_tinh,
    
    -- Lấy MAX UserField_01 (chỉ cần MAX vì GROUP BY ItemCode)
    COALESCE(TRY_CAST(MAX(i.UserField_01) AS DOUBLE), 0.0) AS UserField_01,
    
    SUM(v.SaleQuantity) AS SaleQuantity,
    SUM(CAST(v.SaleAmount AS DOUBLE)) AS SaleAmount,
    
    -- M3 calculation
    SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * v.SaleQuantity) AS M3,
    
    -- AmountM3 calculation
    CASE 
        WHEN SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * v.SaleQuantity) = 0 THEN 0 
        ELSE SUM(CAST(v.SaleAmount AS DOUBLE)) / SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * v.SaleQuantity)
    END AS AmountM3,
    v.SaleMonth, -- Giữ lại SaleMonth để lọc và làm key
    v.SaleYear -- Giữ lại SaleYear để lọc và làm key
    
FROM 
    View_BISaleByMonth_TrueCC v
INNER JOIN SHSaleCostCenterFull_Source c ON v.CostCenter = c.CostCenter
INNER JOIN Items_Source i ON v.ItemCode = i.ItemCode
INNER JOIN SHIndustry_Source si ON i.Assortment BETWEEN si.ItemGroupMin AND si.ItemGroupMax
INNER JOIN ItemAssortment_Source ia ON i.Assortment = ia.Assortment
INNER JOIN ItemClasses_Source ic ON i.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
WHERE 
    i.Assortment < 500
    
GROUP BY 
    v.Division,
    v.CostCenter,
    v.SaleMonth,
    v.SaleYear,
    v.ItemCode,
    c.ReportName,
    c.ChannelCode,
    i.Description_0,
    i.UserField_06,
    i.UserYesNo_05,
    i.Class_01,
    i.Assortment,
    si.IndustryCode, 
    si.IndustryName,
    ic.Description,
    ia.Description
ORDER BY 
    i.Assortment, ReportName_STT, v.Division, v.CostCenter, c.ChannelCode, v.ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}