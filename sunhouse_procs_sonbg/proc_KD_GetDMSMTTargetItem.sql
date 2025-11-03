{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày tháng (SMonth/SYear), CostCenter, cmp_wwn, và PGCode/UserName
        unique_key=['data_date', 'smonth', 'syear', 'costcenter', 'cmp_wwn', 'pgcode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    DMSMTTargetItem_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSMTTargetItem') }}),
    DMSMTSaleOutTargetAmount_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSMTSaleOutTargetAmount') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }}),
    Cicmpy_Consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__Cicmpy_Consolidated') }}),
    SHStatesCountry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHStatesCountry') }}),
    SHMarAgency_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHMarAgency') }}),

    -- Lọc Incremental cho cả hai bảng Master
    FilteredTargetItem AS (
        SELECT D.*
        FROM DMSMTTargetItem_Source D
        WHERE D.TargetQuantity > 0
        
        {% if is_incremental() %}
            -- Giả định lọc theo SMonth/SYear của ngày ETL
            AND D.SMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            AND D.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
        {% endif %}
    ),
    
    FilteredTargetAmount AS (
        SELECT D.*
        FROM DMSMTSaleOutTargetAmount_Source D
        WHERE D.IndustryCode <> 'ALL'
            AND D.TargetAmount > 0
        
        {% if is_incremental() %}
            -- Giả định lọc theo SMonth/SYear của ngày ETL
            AND D.SMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            AND D.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
        {% endif %}
    ),

    -- 1. CTE: Tương đương với Subquery (X)
    X AS (
        -- Nhánh logic Item Target
        SELECT 
            D.CostCenter,
            D.cmp_wwn,
            TRIM(D.UserName) AS UserName,
            D.SMonth,
            D.SYear,
            D.Approval,
            D.Itemcode,
            D.TargetQuantity,
            '' AS IndustryCode,
            0 AS TargetAmount,
            '' AS TargetType,
            NULL AS AgencyId
        FROM FilteredTargetItem D
        
        UNION ALL
        
        -- Nhánh logic Amount Target
        SELECT 
            D.CostCenter,
            D.cmp_wwn,
            TRIM(D.PGCode) AS UserName,
            D.SMonth,
            D.SYear,
            D.Approval,
            '' AS Itemcode,
            0 AS TargetQuantity,
            D.IndustryCode,
            D.TargetAmount,
            D.TargetType,
            D.AgencyId
        FROM FilteredTargetAmount D
    )

-- 2. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    X.CostCenter,
    X.cmp_wwn,
    C.cmp_name,
    TRIM(C.debcode) AS debcode,
    MA.AgencyName,
    MA.PhoneNumber,
    MA.Id AS AgencyId,
    TRIM(X.UserName) AS PGCode,
    A.FullName,
    X.SMonth,
    X.SYear,
    A.Region,
    X.Approval,
    X.ItemCode,
    X.IndustryCode,
    X.TargetQuantity,
    X.TargetAmount,
    X.TargetType,
    C.textfield11 AS ChainCode,
    -- dateCustom (Ngày đầu tiên của tháng mục tiêu)
    PARSE_DATETIME(
        CAST(X.SYear AS VARCHAR) || '-' || CAST(X.SMonth AS VARCHAR) || '-1',
        'yyyy-MM-d'
    ) AS dateCustom
FROM X
LEFT JOIN AppUsers_Source A ON TRIM(X.UserName) = TRIM(A.UserName)
LEFT JOIN Cicmpy_Consolidated_Source C ON X.cmp_wwn = C.cmp_wwn AND C.Division = 101
LEFT JOIN SHStatesCountry_Source SH ON SH.Id = C.SHProvinceId AND SH.StatesLevel = 0
LEFT JOIN SHMarAgency_Source MA ON MA.Id = X.AgencyId
ORDER BY 
    X.UserName

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}