{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày tháng (SMonth/SYear), ItemCode, CostCenter, và LocationType
        unique_key=['data_date', 'smonth', 'syear', 'itemcode', 'costcenter', 'locationtype'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    SH_WH_StorageCosts_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SH_WH_StorageCosts') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHCostcenter') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__ItemClasses') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHIndustry') }}),


    -- 1. CTE: Tương đương với biến bảng @TblLocationType (Sử dụng VALUES)
    TblLocationType AS (
        SELECT *
        FROM (
            VALUES
                (CAST('TQ_MB' AS VARCHAR), CAST('CPLK MPLTQ - MB' AS VARCHAR), CAST('1_TQ_MB' AS VARCHAR), CAST('1) Miền bắc' AS VARCHAR)),
                (CAST('TQ_MT' AS VARCHAR), CAST('CPLK MPLTQ - MT' AS VARCHAR), CAST('2_TQ_MT' AS VARCHAR), CAST('2) Miền trung' AS VARCHAR)),
                (CAST('TQ_MN' AS VARCHAR), CAST('CPLK MPLTQ - MN' AS VARCHAR), CAST('3_TQ_MN' AS VARCHAR), CAST('5) CPLK TQ_MN' AS VARCHAR)),
                (CAST('trieukhuc' AS VARCHAR), CAST('CPLK Triều khúc' AS VARCHAR), CAST('4_trieukhuc' AS VARCHAR), CAST('3) Triều khúc' AS VARCHAR)),
                (CAST('hoakhanh' AS VARCHAR), CAST('CPLK Hòa khánh' AS VARCHAR), CAST('5_hoakhanh' AS VARCHAR), CAST('4) Hòa khánh' AS VARCHAR)),
                (CAST('datamn' AS VARCHAR), CAST('CPLK Data - MN' AS VARCHAR), CAST('6_datamn' AS VARCHAR), CAST('6) CPLK DataMN' AS VARCHAR))
        ) AS t (LocationType, DescriptName, OrderName, OtherName)
    ),

    -- 2. Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT T.*
        FROM SH_WH_StorageCosts_Source T
        WHERE 
            (T.Cost * COALESCE(T.Rate, 1)) <> 0
            
            -- Lọc Incremental (Chỉ lấy dữ liệu của tháng/năm mới nhất)
            {% if is_incremental() %}
                -- Giả định ngày ETL chạy hàng tháng, chỉ cần lấy tháng/năm của ngày ETL
                AND T.SMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
                AND T.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            {% endif %}
    )

-- 3. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    T.SMonth,
    T.SYear,
    T.LocationType,
    T.ItemCode,
    T.Cost,
    TRIM(T.CostCenter) AS CostCenter,
    T.SFQuantity,
    T.SaleQuantity,
    T.LYQuantity,
    T.LYSFQuantity,
    T.VolByClass,
    T.Rate,
    T.Cost * COALESCE(T.Rate, 1) AS CostByItem,
    T.SyncTime,
    I.ItemName,
    I.Class_01,
    I.ItemClassName,
    I.IndustryCode,
    I.IndustryName,
    I.UserField_01,
    I.UserYesNo_05,
    I.Class_07,
    I.Class_09,
    I.UserField_05,
    sc.ChannelCode,
    sc.Region,
    J.DescriptName,
    J.OrderName,
    J.OtherName,
    CASE WHEN sc.ChannelCode = 'GT' THEN 'CC GT' ELSE 'CC còn lại' END AS CCTemp,
    
    I.IndustryCode AS IndustryCode_Cover,
    CAST(T.SYear AS VARCHAR) || '-' || LPAD(CAST(T.SMonth AS VARCHAR), 2, '0') || '-01' AS Date_Cover
    
FROM FilteredMaster T
LEFT JOIN SHCostcenter_Source sc ON sc.CostCenter = T.CostCenter
LEFT JOIN TblLocationType J ON J.LocationType = T.LocationType
-- T-SQL OUTER APPLY (I) - Lấy thông tin Item
LEFT JOIN LATERAL (
    SELECT 
        I1.ItemCode, i1.ItemName, i1.Class_01, IC.Description AS ItemClassName, 
        i1.IndustryCode, SI.IndustryName, i1.UserField_01, i1.UserYesNo_05,
        i1.Class_07, i1.Class_09, i1.UserField_05
    FROM MDataItems_Source i1
    LEFT JOIN ItemClasses_Source IC ON i1.Class_01 = IC.ItemClassCode AND IC.ClassID = 1
    LEFT JOIN SHIndustry_Source SI ON SI.IndustryCode = i1.IndustryCode
    WHERE I1.ItemCode = T.ItemCode
) I ON TRUE
ORDER BY 
    T.ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}