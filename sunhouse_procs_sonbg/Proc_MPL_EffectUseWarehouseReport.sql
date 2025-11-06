{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key phức tạp: dựa trên ngày, kho, ItemCode (hoặc ClassName nếu tổng hợp) và nguồn dữ liệu
        unique_key=['data_date', 'date', 'warehouse', 'itemcode', 'datause_cover', 'typeview_cover'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    StockBalances_SHG_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__StockBalances') }}),
    StockBalances_DL_Source AS (SELECT * FROM {{ source('dp_src_exact102', 'stockbalances') }}),
    MPLmagaz_SHG_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }} WHERE Division = 101),
    MPLmagaz_DL_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact102__magaz') }} WHERE Division = 102),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__ItemClasses') }}),

    -- Lọc Incremental (Dùng chung cho tất cả các nhánh)
    DateFilter AS (
        SELECT 
            CAST('{{ var("etl_date") }}' AS DATE) AS FilterDate
        {% if is_incremental() %}
            -- Đối với Incremental, ta chỉ quan tâm đến dữ liệu của ngày ETL
        {% endif %}
    ),
    
    -- 1. Nhánh Detail - SHG (Gộp tồn đầu kỳ và tồn trong kỳ)
    Detail_SHG AS (
        SELECT 
            G.Warehouse, m.name AS WarehouseName, G.ItemCode, i.ItemName,
            CASE WHEN COALESCE(i.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END AS UserYesNo_05,
            m.int_regio, m.Region, i.IndustryCode, 
            COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) AS Volume,
            IC.Description AS ClassName,
            G.Date, SUM(G.Quantity) AS Quantity,
            'Detail' AS typeView_Cover, 'SHG' AS dataUse_Cover, i.Class_01 AS lstclass_Cover,
            (CASE WHEN i.Assortment <= 350 THEN 'Product' WHEN i.Assortment IN (500, 900) THEN 'Part' ELSE '' END) AS optionview_Cover,
            G.Warehouse AS Warehouse_Cover, i.IndustryCode AS IndustryCode_Cover,
            (CASE WHEN COALESCE(i.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END) AS Status_Cover
        FROM (
            SELECT G.Warehouse, G.ItemCode, MIN(G.Date) AS Date, SUM(Quantity) AS Quantity FROM StockBalances_SHG_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_SHG_Source m ON G.Warehouse = TRIM(m.magcode) WHERE (I.Assortment <= 350 OR (I.Assortment = 900 AND I.Class_01 = 'RO1')) AND m.magcode <> 'TRAN' AND M.ManagerGroup = 'MPL' AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY G.Warehouse, G.ItemCode
            UNION ALL
            SELECT G.Warehouse, G.ItemCode, G.Date, SUM(Quantity) AS Quantity FROM StockBalances_SHG_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_SHG_Source m ON G.Warehouse = TRIM(m.magcode) WHERE (I.Assortment <= 350 OR (I.Assortment = 900 AND I.Class_01 = 'RO1')) AND M.ManagerGroup = 'MPL' AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY G.Warehouse, G.ItemCode, G.Date
        ) G
        JOIN MDataItems_Source i ON i.ItemCode = G.ItemCode
        JOIN MPLmagaz_SHG_Source m ON TRIM(G.Warehouse) = TRIM(m.magcode)
        LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1
        GROUP BY 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    ),

    -- 2. Nhánh Detail - DL
    Detail_DL AS (
        SELECT 
            G.Warehouse, m.name AS WarehouseName, G.ItemCode, i.ItemName,
            CASE WHEN COALESCE(i.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END AS UserYesNo_05,
            m.int_regio, m.Region, i.IndustryCode, 
            COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) AS Volume,
            IC.Description AS ClassName,
            G.Date, SUM(G.Quantity) AS Quantity,
            'Detail' AS typeView_Cover, 'DL' AS dataUse_Cover, i.Class_01 AS lstclass_Cover,
            (CASE WHEN i.Assortment <= 350 THEN 'Product' WHEN i.Assortment IN (500, 900) THEN 'Part' ELSE '' END) AS optionview_Cover,
            G.Warehouse AS Warehouse_Cover, i.IndustryCode AS IndustryCode_Cover,
            (CASE WHEN COALESCE(i.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END) AS Status_Cover
        FROM (
            SELECT G.Warehouse, G.ItemCode, MIN(G.Date) AS Date, SUM(Quantity) AS Quantity FROM StockBalances_DL_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_DL_Source m ON G.Warehouse = TRIM(m.magcode) WHERE (I.Assortment <= 350 OR (I.Assortment = 900 AND I.Class_01 = 'RO1')) AND m.magcode <> 'TRAN' AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY G.Warehouse, G.ItemCode
            UNION ALL
            SELECT G.Warehouse, G.ItemCode, G.Date, SUM(Quantity) AS Quantity FROM StockBalances_DL_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_DL_Source m ON G.Warehouse = TRIM(m.magcode) WHERE (I.Assortment <= 350 OR (I.Assortment = 900 AND I.Class_01 = 'RO1')) AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY G.Warehouse, G.ItemCode, G.Date
        ) G
        JOIN MDataItems_Source i ON i.ItemCode = G.ItemCode
        JOIN MPLmagaz_SHG_Source m ON TRIM(G.Warehouse) = TRIM(m.magcode) AND m.Division = 102 -- Lỗi T-SQL gốc: join MPLmagaz_SHG với Div 102
        LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1
        GROUP BY 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    ),
    
    -- 3. Nhánh Sum - DL
    Sum_DL AS (
        SELECT 
            G.Warehouse, g.WarehouseName, g.int_regio, G.IndustryCode, m.Region, G.Date, SUM(G.Quantity) AS Quantity, 
            SUM(G.TotalVolumne) AS TotalVolumne, (CASE WHEN I.UserYesNo_05 = TRUE THEN 'OFF' ELSE 'ON' END) AS UserYesNo_05,
            G.ClassName, (CASE WHEN G.ClassName IS NOT NULL THEN 'SumByClass' ELSE 'SumByDay' END) AS typeView_Cover,
            'DL' AS dataUse_Cover, I.Class_01 AS lstclass_Cover,
            (CASE WHEN I.Assortment <= 350 THEN 'Product' WHEN I.Assortment IN (500, 900) THEN 'Part' ELSE '' END) AS optionview_Cover,
            G.Warehouse AS Warehouse_Cover, I.IndustryCode AS IndustryCode_Cover,
            (CASE WHEN COALESCE(I.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END) AS Status_Cover
        FROM (
            SELECT G.Warehouse, TRIM(M.naam) AS WarehouseName, m.int_regio, i.IndustryCode, MIN(G.Date) AS Date, SUM(Quantity) AS Quantity, ROUND(SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * G.Quantity), 0) AS TotalVolumne, IC.Description AS ClassName, I.ItemCode FROM StockBalances_DL_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_DL_Source m ON G.Warehouse = TRIM(m.magcode) AND m.Division = 102 LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1 WHERE m.magcode <> 'TRAN' AND m.Division = 102 AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY 1, 4, 2, 3, 8, 9
            UNION ALL
            SELECT G.Warehouse, TRIM(M.naam) AS WarehouseName, m.int_regio, i.IndustryCode, G.Date, SUM(Quantity) AS Quantity, ROUND(SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * G.Quantity), 0) AS TotalVolumne, IC.Description AS ClassName, I.ItemCode FROM StockBalances_DL_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_DL_Source m ON G.Warehouse = TRIM(m.magcode) AND m.Division = 102 LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1 WHERE G.Warehouse IS NOT NULL AND m.Division = 102 AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY 1, 4, 5, 2, 3, 8, 9
        ) G
        JOIN MPLmagaz_SHG_Source m ON G.Warehouse = m.magcode AND m.Division = 101 -- Lỗi T-SQL gốc: join MPLmagaz_SHG với Div 101
        JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode
        GROUP BY 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    ),

    -- 4. Nhánh Sum - SHG
    Sum_SHG AS (
        SELECT 
            G.Warehouse, g.WarehouseName, g.int_regio, G.IndustryCode, m.Region, G.Date, SUM(G.Quantity) AS Quantity, 
            SUM(G.TotalVolumne) AS TotalVolumne, (CASE WHEN I.UserYesNo_05 = TRUE THEN 'OFF' ELSE 'ON' END) AS UserYesNo_05,
            G.ClassName, (CASE WHEN G.ClassName IS NOT NULL THEN 'SumByClass' ELSE 'SumByDay' END) AS typeView_Cover,
            'SHG' AS dataUse_Cover, I.Class_01 AS lstclass_Cover,
            (CASE WHEN I.Assortment <= 350 THEN 'Product' WHEN I.Assortment IN (500, 900) THEN 'Part' ELSE '' END) AS optionview_Cover,
            G.Warehouse AS Warehouse_Cover, I.IndustryCode AS IndustryCode_Cover,
            (CASE WHEN COALESCE(I.UserYesNo_05, FALSE) = FALSE THEN 'ON' ELSE 'OFF' END) AS Status_Cover
        FROM (
            SELECT G.Warehouse, TRIM(M.name) AS WarehouseName, m.int_regio, i.IndustryCode, MIN(G.Date) AS Date, SUM(Quantity) AS Quantity, ROUND(SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * G.Quantity), 0) AS TotalVolumne, IC.Description AS ClassName, I.ItemCode FROM StockBalances_SHG_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_SHG_Source m ON G.Warehouse = TRIM(m.magcode) AND m.Division = 101 LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1 WHERE M.ManagerGroup = 'MPL' AND m.magcode <> 'TRAN' AND m.Division = 101 AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY 1, 4, 2, 3, 8, 9
            UNION ALL
            SELECT G.Warehouse, TRIM(M.name) AS WarehouseName, m.int_regio, i.IndustryCode, G.Date, SUM(Quantity) AS Quantity, ROUND(SUM(COALESCE(TRY_CAST(i.UserField_01 AS DOUBLE), 0.0) * G.Quantity), 0) AS TotalVolumne, IC.Description AS ClassName, I.ItemCode FROM StockBalances_SHG_Source G JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode JOIN MPLmagaz_SHG_Source m ON G.Warehouse = TRIM(m.magcode) AND m.Division = 101 LEFT JOIN ItemClasses_Source IC ON IC.ItemClassCode = I.Class_01 AND IC.ClassID = 1 WHERE m.ManagerGroup = 'MPL' AND G.Warehouse IS NOT NULL AND m.Division = 101 AND G.Date = (SELECT FilterDate FROM DateFilter) GROUP BY 1, 4, 5, 2, 3, 8, 9
        ) G
        JOIN MPLmagaz_SHG_Source m ON G.Warehouse = m.magcode AND m.Division = 101
        JOIN MDataItems_Source I ON G.ItemCode = I.ItemCode
        GROUP BY 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    )

-- 5. Gộp tất cả 4 nhánh logic
SELECT 
    -- 1. Cột Metadata Incremental
    (SELECT FilterDate FROM DateFilter) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    Warehouse, WarehouseName, ItemCode, ItemName, UserYesNo_05, int_regio, Region, 
    IndustryCode, Volume, ClassName, Date, Quantity, 
    CAST(NULL AS DOUBLE) AS TotalVolumne, -- TotalVolumne chỉ có giá trị khi TypeView là SumByDay/SumByClass
    typeView_Cover, dataUse_Cover, lstclass_Cover, optionview_Cover, 
    Warehouse_Cover, IndustryCode_Cover, Status_Cover
FROM Detail_SHG

UNION ALL

SELECT 
    (SELECT FilterDate FROM DateFilter) AS data_date, CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    Warehouse, WarehouseName, ItemCode, ItemName, UserYesNo_05, int_regio, Region, 
    IndustryCode, Volume, ClassName, Date, Quantity, 
    CAST(NULL AS DOUBLE) AS TotalVolumne,
    typeView_Cover, dataUse_Cover, lstclass_Cover, optionview_Cover, 
    Warehouse_Cover, IndustryCode_Cover, Status_Cover
FROM Detail_DL

UNION ALL

SELECT 
    (SELECT FilterDate FROM DateFilter) AS data_date, CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    Warehouse, WarehouseName, CAST(NULL AS VARCHAR) AS ItemCode, CAST(NULL AS VARCHAR) AS ItemName, 
    UserYesNo_05, int_regio, Region, IndustryCode, 
    CAST(NULL AS DOUBLE) AS Volume, ClassName, Date, Quantity, 
    TotalVolumne,
    typeView_Cover, dataUse_Cover, lstclass_Cover, optionview_Cover, 
    Warehouse_Cover, IndustryCode_Cover, Status_Cover
FROM Sum_DL

UNION ALL

SELECT 
    (SELECT FilterDate FROM DateFilter) AS data_date, CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    Warehouse, WarehouseName, CAST(NULL AS VARCHAR) AS ItemCode, CAST(NULL AS VARCHAR) AS ItemName, 
    UserYesNo_05, int_regio, Region, IndustryCode, 
    CAST(NULL AS DOUBLE) AS Volume, ClassName, Date, Quantity, 
    TotalVolumne,
    typeView_Cover, dataUse_Cover, lstclass_Cover, optionview_Cover, 
    Warehouse_Cover, IndustryCode_Cover, Status_Cover
FROM Sum_SHG
{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}