{{
    config(
        materialized='table', -- Sử dụng 'table' vì dữ liệu snapshot lớn, khó incremental
        views_enabled=False,
        -- Giữ lại cấu trúc partitioning cho việc lưu trữ, nhưng bỏ unique_key incremental
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    StockAgingByWH_FullNew_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__StockAgingByWH_FullNew') }}),
    orkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orkrg') }}),
    orsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orsrg') }}),
    MdataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MdataItems') }}),
    cicmpy_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__cicmpy') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__Items') }}),
    KPI_ItemClassByUser_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__KPI_ItemClassByUser') }}),
    humres_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__humres') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemClasses') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__SHIndustry') }}),
    magaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__magaz') }}),
    MPLmagaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }}),
    ItemAccounts_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__itemaccounts') }}),

    -- 1. CTE: Ngày Snapshot (RefDate)
    RefDate AS (
        SELECT MAX(S0.Syscreated) AS MaxRefDate
        FROM StockAgingByWH_FullNew_Source S0
    ),

    -- 2. CTE: Subquery bên trong logic @tblItemXNK (Lấy ngày tạo order mới nhất)
    G_inner AS (
        SELECT 
            s.artcode AS ItemCode, 
            MAX(s.syscreated) AS syscreated
        FROM orkrg_Source k
        JOIN orsrg_Source s ON s.ordernr = k.ordernr
        JOIN MdataItems_Source i ON s.artcode = i.ItemCode
        WHERE k.ord_soort = 'B' AND s.ar_soort <> 'P'
          AND (i.Assortment <= 350 OR (i.Assortment = 900 AND i.Class_01 = 'RO1'))
        GROUP BY s.artcode
    ),
    
    -- 3. CTE Ranking: Thay thế logic TOP 1 trong OUTER APPLY cho ItemAccounts
    ItemAccountsRanked AS (
        SELECT
            ia.AccountCode, 
            ia.PurchaseCurrency, 
            ia.PurchaseOrderSize,
            ia.ItemCode,
            ROW_NUMBER() OVER(PARTITION BY ia.ItemCode, ia.AccountCode ORDER BY ia.ItemCode) as rn
        FROM ItemAccounts_Source ia
        WHERE ia.MainAccount = TRUE
    ),

    -- 4. CTE: Tương đương với @tblItemXNK (Nhà cung cấp, người quản lý, PO Size)
    tblItemXNK AS (
        SELECT 
            G.ItemCode,
            T.AccountCode,
            T.PurchaseCurrency,
            G.cmp_name,
            u.UserName,
            u.FullName,
            T.PurchaseOrderSize,
            G.afldat AS ReportDate
        FROM (
            SELECT 
                k.crdnr, s.artcode AS ItemCode, c.cmp_name, c.cmp_acc_man, k.Division, c.cmp_wwn, s.syscreated, k.afldat
            FROM orsrg_Source s
            JOIN orkrg_Source k ON s.ordernr = k.ordernr
            JOIN cicmpy_Source c ON c.crdnr = k.crdnr
            WHERE k.ord_soort = 'B'
        ) G
        INNER JOIN G_inner ON G_inner.ItemCode = G.ItemCode AND G_inner.syscreated = G.syscreated
        
        JOIN AppUsers_Source u ON u.res_id = G.cmp_acc_man AND u.Division = G.Division
        
        LEFT JOIN ItemAccountsRanked T 
            ON G.ItemCode = T.ItemCode 
            AND G.cmp_wwn = T.AccountCode 
            AND T.rn = 1
    )

-- 5. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Snapshot
    CAST(DATE_ADD('day', -1, R.MaxRefDate) AS DATE) AS data_date, -- Ngày báo cáo (MaxRefDate - 1)
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    COALESCE(X.fullname, SI.IndustryCode) AS ClassGroup,
    i.Assortment,
    i.ItemCode,
    s.ItemName,
    i.Class_05,
    i.Class_07,
    CASE WHEN i.UserYesNo_05 = FALSE THEN 'ON' ELSE 'OFF' END AS UserYesNo_05,
    i.UserField_06,
    xnk.Fullname,
    xnk.Cmp_name,
    COALESCE(xnk.PurchaseOrderSize, 0) AS POSize, 
    COALESCE(i.UserNumber_06, 0) AS MOQ,
    COALESCE(i.CostPriceStandard, 0) AS CostPriceStandard,
    
    SUM(StockQty) AS StockQty,
    SUM(StockQtyAge0) + SUM(StockQtyAge1) AS StockQtyAge1,
    SUM(StockQtyAge2) + SUM(StockQtyAge3) + SUM(StockQtyAge4) + SUM(StockQtyAge5) + SUM(StockQtyAge6) + SUM(StockQtyAge7) AS StockQtyAge2,
    SUM(StockQtyAge8) + SUM(StockQtyAge9) + SUM(StockQtyAge10) AS StockQtyAge3,
    SUM(StockQtyAge11) AS StockQtyAge4,
    SUM(StockQtyAge12) AS StockQtyAge5,
    SUM(StockQtyAgeMax) AS StockQtyAgeMax,
    
    SUM(StockQty) * COALESCE(i.CostPriceStandard, 0) AS StockAmount,
    (SUM(StockQtyAge0) + SUM(StockQtyAge1)) * COALESCE(i.CostPriceStandard, 0) AS Amount1,
    (SUM(StockQtyAge2) + SUM(StockQtyAge3) + SUM(StockQtyAge4) + SUM(StockQtyAge5) + SUM(StockQtyAge6) + SUM(StockQtyAge7)) * COALESCE(i.CostPriceStandard, 0) AS Amount2,
    (SUM(StockQtyAge8) + SUM(StockQtyAge9) + SUM(StockQtyAge10)) * COALESCE(i.CostPriceStandard, 0) AS Amount3,
    SUM(StockQtyAge11) * COALESCE(i.CostPriceStandard, 0) AS Amount4,
    SUM(StockQtyAge12) * COALESCE(i.CostPriceStandard, 0) AS Amount5,
    SUM(StockQtyAgeMax) * COALESCE(i.CostPriceStandard, 0) AS AmountMax,
    
    i.Class_01,
    ic.Description AS ICName,
    i.Class_08,
    CASE WHEN si.IndustryCode = 'LN' THEN 'ĐTĐL' ELSE si.IndustryCode END AS IndustryCode,
    COALESCE(ic1.Description, '') AS IClass05Name,
    
    xnk.ReportDate,
    wh.int_regio,
    ml.Region
    
FROM 
    StockAgingByWH_FullNew_Source S
CROSS JOIN RefDate R
INNER JOIN Items_Source i ON TRIM(S.ItemCode) = TRIM(i.ItemCode)
LEFT JOIN KPI_ItemClassByUser_Source GI 
    ON COALESCE(GI.ItemClassCode, '') = COALESCE(i.Class_01, '') 
    AND GI.JobGroup = 'QLNH' AND GI.IndustryCode <> 'GD' AND GI.Active = TRUE
LEFT JOIN humres_Source X ON GI.UserName = X.usr_id
INNER JOIN ItemClasses_Source ic ON i.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
LEFT JOIN ItemClasses_Source ic1 ON i.Class_05 = ic1.ItemClassCode AND ic1.ClassID = 5
INNER JOIN SHIndustry_Source si ON i.Assortment BETWEEN si.ItemGroupMin AND si.ItemGroupMax
LEFT JOIN tblItemXNK xnk ON TRIM(S.ItemCode) = TRIM(xnk.ItemCode)
LEFT JOIN magaz_Source wh ON wh.magcode = s.Warehouse
LEFT JOIN MPLmagaz_Source ml ON wh.magcode = ml.magcode

WHERE 
    S.Date = DATE_ADD('day', -1, R.MaxRefDate)
    AND i.Assortment < 400

GROUP BY 
    COALESCE(X.fullname, SI.IndustryCode), i.Assortment, i.ItemCode, s.ItemName, i.CostPriceStandard, i.Class_01, ic.Description, i.Class_08, 
    CASE WHEN si.IndustryCode = 'LN' THEN 'ĐTĐL' ELSE si.IndustryCode END, ic1.Description, X.fullname, SI.IndustryCode, i.Class_05, i.Class_07, i.UserYesNo_05, i.UserField_06, 
    xnk.Fullname, xnk.Cmp_name, xnk.PurchaseOrderSize, i.UserNumber_06, xnk.ReportDate, wh.int_regio, ml.Region
    
ORDER BY 
    ClassGroup

{% endset %}

{{ query }}