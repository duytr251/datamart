{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng ItemCode và ngày ETL làm unique_key
        unique_key=['data_date', 'itemcode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__Items') }}),
    ItemAssortment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__itemassortment') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemClasses') }}),
    ItemAccounts_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__ItemAccounts') }}),
    Cicmpy_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__cicmpy') }}),
    SHStockControlItemProperties_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHStockControlItemProperties') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__SHIndustry') }}),
    ITWFReqItemDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgGextappdata__ITWFReqItemDetail') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'shg_extappdata', 'MDataItems') }}), -- Nguồn khác

    -- 1. Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT I.*
        FROM Items_Source I
        WHERE I."Type" NOT IN ('L','R','P')
        
        -- Lọc Incremental (Chỉ lấy các bản ghi đã được sửa đổi kể từ lần chạy trước)
        {% if is_incremental() %}
            AND I.ModifyDate >= (SELECT MAX(t.ModifyDate) FROM {{ this }} t WHERE t.ModifyDate IS NOT NULL)
        {% endif %}
    ),

    -- 2. CTE cho ItemAccounts (IAC)
    ItemAccountsRanked AS (
        SELECT 
            ia.ItemCode, ia.DeliveryTimeInDays, ia.PurchaseOrderSize, c.crdnr,
            ROW_NUMBER() OVER(PARTITION BY ia.ItemCode ORDER BY ia.ItemCode) as rn
        FROM ItemAccounts_Source ia
        INNER JOIN Cicmpy_Source C ON ia.AccountCode = CAST(C.cmp_wwn AS VARCHAR) 
    )

-- 3. SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    i.Assortment AS ItemGroup, 
    TRIM(ia.Description) AS ItemGroupName,
    i.Class_01, 
    TRIM(ic.Description) AS Class_01Name,
    i.Class_03, 
    TRIM(ic2.Description) AS Class_03Name,
    TRIM(I.ItemCode) AS ItemCode, 
    TRIM(I.Description_0) AS ItemName,
    i."Type",
    i.Condition AS StatusCode,
    CASE 
        WHEN TRIM(i.Condition) = 'A' THEN 'Actived'
        WHEN TRIM(i.Condition) = 'B' THEN 'Blocked'
        WHEN TRIM(i.Condition) = 'D' THEN 'Discontinued'
        WHEN TRIM(i.Condition) = 'E' THEN 'Inactived' 
        ELSE 'Not in Status' 
    END AS StatusName,
    ic4.Description AS MauSac,
    ic5.Description AS MuaVu,
    ic6.Description AS PhanKhuc,
    ic7.Description AS NguonGoc,
    i.TextDescription AS QuyCach,
    i.userfield_01 AS TheTich,
    i.userfield_02 AS Model,
    i.userfield_03 AS Barcode,
    i.userfield_04 AS PT_DongGoi,
    i.userfield_10 AS HSCode,
    i.Shelflife AS VongDoiSP,
    i.Warranty AS ThoihanBH,
    i.Netweight AS CanNang,
    i.Description_1 AS ItemName2,
    i.UserField_05 AS Ma_PTBH,
    COALESCE(i.userNumber_12, 0) AS Shipment,
    CASE 
        WHEN i.UserYesNo_02 = TRUE THEN 'Có' 
        ELSE 'Không'
    END AS BHTaiNha,
    I.UserField_06 AS SIOI,
    CASE 
        WHEN i.UserYesNo_05 = FALSE THEN 'ON' 
        ELSE 'OFF' 
    END AS ONOFF,
    COALESCE(I.UserNumber_06, 0) AS MOQ,
    (COALESCE(i.UserNumber_10, 0) + COALESCE(i.UserNumber_11, 0)) AS LeadTime,
    (CASE 
        WHEN TRIM(I.Class_07) IN ('VIET','SX','TN') THEN 5 
        WHEN COALESCE(IP.DeliveryTimeInDays, 0) = 0 AND TRIM(I.Class_07) NOT IN ('VIET','SX','TN') THEN 20 
        ELSE COALESCE(IP.DeliveryTimeInDays, 0) 
    END) AS DeliveryDay,
    IAC.PurchaseOrderSize AS POSize,
    i.CostPriceStandard,
    i.UserField_07,
    i.SalesVatCode,
    CASE 
        WHEN i.UserYesNo_03 = TRUE THEN 'Có' 
        ELSE 'Không'
    END AS ItemNotSH,
    si.IndustryCode,
    ic9.Description AS SaleChannel,
    ic8.Description AS TradeMark,
    COALESCE(ir.MethodLong, 0) AS ItemLong,
    COALESCE(ir.MethodWidth, 0) AS ItemWidth,
    COALESCE(ir.MethodHeight, 0) AS ItemHeight,
    IX.FirsStockDate,
    IX.FirsOutStockDate,
    i.ModifyDate -- Giữ lại ModifyDate cho logic incremental
FROM 
    FilteredMaster AS i
LEFT JOIN ItemAssortment_Source AS ia ON i.Assortment = ia.Assortment
LEFT JOIN ItemClasses_Source AS ic ON i.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
LEFT JOIN ItemClasses_Source AS ic2 ON i.Class_03 = ic2.ItemClassCode AND ic2.ClassID = 3
LEFT JOIN ItemClasses_Source AS ic4 ON i.Class_04 = ic4.ItemClassCode AND ic4.ClassID = 4
LEFT JOIN ItemClasses_Source AS ic5 ON i.Class_05 = ic5.ItemClassCode AND ic5.ClassID = 5
LEFT JOIN ItemClasses_Source AS ic6 ON i.Class_06 = ic6.ItemClassCode AND ic6.ClassID = 6
LEFT JOIN ItemClasses_Source AS ic7 ON i.Class_07 = ic7.ItemClassCode AND ic7.ClassID = 7
LEFT JOIN ItemClasses_Source AS ic8 ON i.Class_08 = ic8.ItemClassCode AND ic8.ClassID = 8
LEFT JOIN ItemClasses_Source AS ic9 ON i.Class_09 = ic9.ItemClassCode AND ic9.ClassID = 9

-- LEFT JOIN (IAC)
LEFT JOIN ItemAccountsRanked IAC ON I.ItemCode = IAC.ItemCode AND i.lev_crdnr = CAST(IAC.crdnr AS VARCHAR) AND IAC.rn = 1

LEFT JOIN (
    SELECT DISTINCT ItemCode, DeliveryTimeInDays 
    FROM SHStockControlItemProperties_Source 
    WHERE DeliveryTimeInDays <> 0
) IP ON I.ItemCode = IP.ItemCode

LEFT JOIN SHIndustry_Source si ON i.Assortment BETWEEN si.ItemGroupMin AND si.ItemGroupMax

-- LEFT JOIN LATERAL (ir)
LEFT JOIN LATERAL (
    SELECT 
        ir.MethodLong, ir.MethodWidth, ir.MethodHeight 
    FROM ITWFReqItemDetail_Source ir
    WHERE ir.ItemCode = i.ItemCode AND ir.Data = 101
    ORDER BY ir.CreatedDate DESC
    LIMIT 1
) ir ON TRUE

-- LEFT JOIN LATERAL (IX)
LEFT JOIN LATERAL (
    SELECT 
        IM.FirsStockDate, IM.FirsOutStockDate
    FROM MDataItems_Source IM
    WHERE IM.ItemCode = I.ItemCode
    ORDER BY IM.FirsStockDate DESC
    LIMIT 1
) IX ON TRUE

ORDER BY 
    i.Assortment, i.Class_01, i.Description_0

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}