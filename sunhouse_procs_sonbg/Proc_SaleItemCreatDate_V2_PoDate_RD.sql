{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng các cột định danh (Industry, Class, ItemCode) cộng với ngày chạy (data_date) làm key
        unique_key=['data_date', 'industrycode', 'classgroup', 'itemcode'],
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    -- Sử dụng các alias ngắn gọn cho CTE
    SHBaseSaleBIData_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHBaseSaleBIData') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHCostcenter') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MDataItems') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemClasses') }}),
    GroupItemClass_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__GroupItemClass') }}),
    gbkmut_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }}),
    ItemAssortment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__ItemAssortment') }}),

    -- 1. CTE cho #SHBaseSaleBIData
    SHBaseSaleBIData_Base AS (
        SELECT
            B.SaleIndustry AS IndustryCode,
            COALESCE(CAST(GI."Group" AS VARCHAR), B.SaleIndustry) AS ClassGroup,
            I.ItemCode,
            I.ItemName AS Description_0,
            I.Class_01,
            I.UserField_06,
            I.Class_07,
            I.UserField_07,
            I.FirsStockDate AS StartDate,
            ic.Description AS ICName,
            B.SaleDate -- Thêm SaleDate để lọc incremental
        FROM
            SHBaseSaleBIData_Source B
        INNER JOIN SHCostcenter_Source C
            ON B.Costcenter = C.CostCenter
            AND C.CostCenterType = 'KD'
            AND COALESCE(C.ChannelCode, '') <> 'TL'
            AND C.Active = TRUE
        INNER JOIN MDataItems_Source I ON I.ItemCode = B.artcode
        LEFT JOIN ItemClasses_Source ic ON I.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
        LEFT JOIN GroupItemClass_Source GI ON COALESCE(GI.Class_01, '') = COALESCE(I.Class_01, '')
        WHERE
            B.SaleIndustry IS NOT NULL
            -- Lọc Incremental cho bảng này
            {% if is_incremental() %}
                AND B.SaleDate = CAST('{{ var("etl_date") }}' AS DATE) -- Giả định SaleDate là cột ngày giao dịch
            {% endif %}
    ),
    
    -- 1.1 Aggregation for SHBaseSaleBIData
    SHBaseSaleBIData_Agg AS (
        SELECT
            SaleIndustry AS IndustryCode,
            ClassGroup,
            ItemCode,
            Description_0,
            Class_01,
            UserField_06,
            Class_07,
            UserField_07,
            StartDate,
            ICName,
            SUM(SaleQuantity) AS SaleQty,
            SUM(SaleAmount) AS SaleAmount
        FROM
            SHBaseSaleBIData_Base
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    ),


    -- 2. CTE cho #IBT
    IBT_Base AS (
        SELECT
            i.IndustryCode AS IndustryCode,
            COALESCE(CAST(GI."Group" AS VARCHAR), i.IndustryCode) AS ClassGroup,
            I.ItemCode,
            I.ItemName AS Description_0,
            I.Class_01,
            I.UserField_06,
            I.Class_07,
            I.UserField_07,
            I.FirsStockDate AS StartDate,
            ic.Description AS ICName,
            g.aantal AS SaleQty,
            g.aantal * I.CostPriceStandard AS SaleAmount,
            g.afldat AS TransactionDate -- Thêm afldat để lọc incremental
        FROM
            gbkmut_Source g
        INNER JOIN MDataItems_Source I ON I.ItemCode = g.artcode
        JOIN ItemAssortment_Source ia ON ia.Assortment = I.Assortment
        LEFT JOIN ItemClasses_Source ic ON I.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
        LEFT JOIN GroupItemClass_Source GI ON COALESCE(GI.Class_01, '') = COALESCE(I.Class_01, '')
        WHERE
            g.transsubtype IN ('A', 'B')
            AND g.checked = 1
            AND g.blockitem = 0
            AND g.warehouse = 'BAMZ'
            AND g.transtype = 'B'
            AND g.bud_vers = 'MRP'
            AND g.IBTDeliveryNr IS NOT NULL
            -- Lọc Incremental cho bảng này
            {% if is_incremental() %}
                AND g.afldat = CAST('{{ var("etl_date") }}' AS DATE) -- Giả định afldat là cột ngày giao dịch
            {% endif %}
    ),
    
    -- 2.1 Aggregation for IBT
    IBT_Agg AS (
        SELECT
            IndustryCode,
            ClassGroup,
            ItemCode,
            Description_0,
            Class_01,
            UserField_06,
            Class_07,
            UserField_07,
            StartDate,
            ICName,
            SUM(SaleQty) AS SaleQty,
            SUM(SaleAmount) AS SaleAmount
        FROM
            IBT_Base
        WHERE
            -- Đảm bảo không trùng lặp với dữ liệu từ SHBaseSaleBIData
            NOT EXISTS (
                SELECT 1
                FROM SHBaseSaleBIData_Agg sh
                WHERE sh.ItemCode = IBT_Base.ItemCode
            )
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    )


-- 3. Gộp kết quả cuối cùng
SELECT
    -- Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM
    SHBaseSaleBIData_Agg T
UNION ALL
SELECT
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM
    IBT_Agg T

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}