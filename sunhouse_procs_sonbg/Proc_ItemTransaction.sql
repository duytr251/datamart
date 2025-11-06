{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng ItemCode, Warehouse, và ngày ETL làm unique_key
        unique_key=['data_date', 'itemcode', 'warehouse'], 
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
    grtbk_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__grtbk') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__Items') }}),
    ItemAssortment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__ItemAssortment') }}),
    orsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orsrg') }}),
    orkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orkrg') }}),
    ItemAccounts_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__itemaccounts') }}),
    voorrd_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__voorrd') }}),
    
    -- 1. CTE: gmt (Giao dịch kho)
    gmt AS (
        SELECT 
            gmt.artcode, 
            gmt.warehouse,
            gmt.datum, -- Giữ lại datum để GROUP BY và lọc incremental
            SUM(gmt.aantal) AS QtyPreviousStock, -- Tạm thời tính tổng, sẽ điều chỉnh sau
            COALESCE(SUM(CASE WHEN gmt.aantal >= 0 AND gbk.omzrek IN ('A', 'G') THEN gmt.aantal END), 0) AS QtyReceivedStock,
            SUM(CASE WHEN gmt.aantal < 0 AND gbk.omzrek IN ('A', 'G') THEN gmt.aantal END) AS QtyFulfilledStock,
            
            it.Class_01 AS ItemClass1_Cover,
            ita.Assortment AS ItemGroup_Cover
        FROM gbkmut_Source gmt
        INNER JOIN grtbk_Source gbk ON gbk.reknr = gmt.reknr 
        INNER JOIN Items_Source it ON it.ItemCode = gmt.artcode 
        INNER JOIN ItemAssortment_Source ita ON ita.Assortment = it.Assortment 
        WHERE gmt.transtype IN ('X','N','C','P') 
          AND gbk.omzrek IN ('G', 'A') 
          AND gmt.reknr = it.GLAccountDistribution 
          AND it.condition IN ('A') 
          AND it.Type IN ('S', 'B') 
          
          -- Lọc Incremental: Chỉ lấy giao dịch của ngày ETL
          {% if is_incremental() %}
            AND gmt.datum = CAST('{{ var("etl_date") }}' AS DATE)
          {% endif %}
          
        GROUP BY gmt.artcode, gmt.warehouse, it.Class_01, ita.Assortment, gmt.datum
    ),

    -- 2. CTE: vbs (Đơn hàng mua)
    vbs AS (
        SELECT 
            vbs.magcode, vbs.artcode, vbs.ItemClass1_Cover, vbs.ItemGroup_Cover,
            SUM(vbs.QtyOrdered) AS QtyOrdered, 
            SUM(vbs.QtyReceived) AS QtyReceived,
            SUM(vbs.QtyOrdered - COALESCE(vbs.QtyReceived, 0.0)) AS QtyToBeReceived
        FROM (
            SELECT 
                srg.magcode, srg.artcode, srg.ordernr,
                SUM(srg.esr_aantal * CASE WHEN al.SlsPkgsPerPurPkg <> 0 THEN al.SlsPkgsPerPurPkg ELSE 1 END) AS QtyOrdered, 
                SUM(srg.aant_gelev * CASE WHEN al.SlsPkgsPerPurPkg <> 0 THEN al.SlsPkgsPerPurPkg ELSE 1 END) AS QtyReceived,
                i.Class_01 AS ItemClass1_Cover,
                ia.Assortment AS ItemGroup_Cover
            FROM orsrg_Source srg
            INNER JOIN orkrg_Source krg ON krg.ordernr = srg.ordernr 
            INNER JOIN Items_Source i ON i.ItemCode = srg.artcode 
            INNER JOIN ItemAssortment_Source ia ON ia.Assortment = i.Assortment 
            INNER JOIN ItemAccounts_Source al ON al.ItemCode = srg.artcode AND al.crdnr = krg.crdnr 
            LEFT JOIN ItemAccounts_Source ald ON ald.ItemCode = srg.artcode AND ald.crdnr = i.lev_crdnr 
            WHERE krg.ord_soort = 'B' AND krg.ordbv_afgd <> 0 AND krg.afgehandld = 0 
              AND srg.artcode IS NOT NULL 
              AND i.condition IN ('A') AND i.Type IN ('S', 'B')
            -- Lọc Incremental: Chỉ cần dữ liệu PO đang mở (không cần lọc ngày)
            GROUP BY srg.magcode, srg.artcode, srg.ordernr, i.Class_01, ia.Assortment
        ) AS vbs 
        GROUP BY vbs.magcode, vbs.artcode, vbs.ItemClass1_Cover, vbs.ItemGroup_Cover
    ),

    -- 3. CTE: srg (Đơn hàng bán)
    srg AS (
        SELECT 
            srg.magcode, srg.artcode, srg.ItemClass1_Cover, srg.ItemGroup_Cover,
            SUM(srg.QtyInOrder) AS QtyInOrder, 
            SUM(QtyInProduction) AS QtyInProduction
        FROM (
            SELECT 
                srg.magcode, srg.artcode,
                SUM(CASE WHEN krg.ord_soort IN ('V','I') THEN srg.esr_aantal - srg.aant_gelev END) AS QtyInOrder, 
                SUM(CASE WHEN krg.ord_soort IN ('M') THEN srg.esr_aantal - srg.aant_gelev END) AS QtyInProduction,
                i.Class_01 AS ItemClass1_Cover,
                ia.Assortment AS ItemGroup_Cover
            FROM orsrg_Source srg
            INNER JOIN orkrg_Source krg ON krg.ordernr = srg.ordernr 
            INNER JOIN Items_Source i ON i.ItemCode = srg.artcode 
            LEFT JOIN ItemAccounts_Source ald ON ald.ItemCode = i.ItemCode AND ald.crdnr = i.lev_crdnr 
            INNER JOIN ItemAssortment_Source ia ON ia.Assortment = i.Assortment 
            WHERE krg.afgehandld = 0 AND krg.ord_soort IN ('V','M') 
              AND i.condition IN ('A') AND i.Type IN ('S', 'B') 
            GROUP BY srg.ordernr, srg.magcode, srg.artcode, i.Class_01, ia.Assortment
            HAVING ROUND(SUM(srg.esr_aantal), 3) > ROUND(SUM(srg.aant_gelev), 3) 
        ) AS srg 
        GROUP BY srg.magcode, srg.artcode, srg.ItemClass1_Cover, srg.ItemGroup_Cover
    ),
    
    -- 4. CTE: bal (Gộp 3 CTE trên)
    bal AS (
        SELECT 
            COALESCE(gmt.artcode, vbs.artcode, srg.artcode) AS ItemCode,
            COALESCE(gmt.warehouse, vbs.magcode, srg.magcode) AS warehouse,
            SUM(gmt.QtyPreviousStock) AS QtyPreviousStock, -- Tính tổng lại trên ngày ETL
            SUM(gmt.QtyReceivedStock) AS QtyReceivedStock, 
            SUM(gmt.QtyFulfilledStock) AS QtyFulfilledStock,
            SUM(vbs.QtyOrdered) AS QtyOrdered, 
            SUM(vbs.QtyReceived) AS QtyReceived, 
            SUM(vbs.QtyToBeReceived) AS QtyToBeReceived, 
            SUM(srg.QtyInOrder) AS QtyInOrder,
            
            COALESCE(gmt.ItemClass1_Cover, vbs.ItemClass1_Cover, srg.ItemClass1_Cover) AS ItemClass1_Cover,
            COALESCE(gmt.ItemGroup_Cover, vbs.ItemGroup_Cover, srg.ItemGroup_Cover) AS ItemGroup_Cover,
            -- Dùng ItemCode là giá trị chính cho các cột Cover
            COALESCE(gmt.artcode, vbs.artcode, srg.artcode) AS ItemCodeFrom_Cover,
            COALESCE(gmt.artcode, vbs.artcode, srg.artcode) AS ItemCodeTo_Cover,
            COALESCE(gmt.warehouse, vbs.magcode, srg.magcode) AS Warehouse_Cover,
            MAX(gmt.datum) AS Date_Cover -- Lấy ngày giao dịch cuối cùng của ngày ETL
            
        FROM gmt
        FULL OUTER JOIN vbs ON vbs.artcode = gmt.artcode AND vbs.magcode = gmt.warehouse
        FULL OUTER JOIN srg ON srg.artcode = COALESCE(gmt.artcode, vbs.artcode) 
                          AND srg.magcode = COALESCE(gmt.warehouse, vbs.magcode)
        GROUP BY 1, 2, 10, 11 -- GROUP BY tất cả các cột không tổng hợp
    ),

    -- 5. Logic QtyInOrder cuối cùng (Tái tạo vì CTE bal không đủ)
    srg_final AS (
        SELECT 
            srg.artcode, srg.magcode, 
            SUM(CASE WHEN krg.ord_soort IN ('V','I') THEN srg.esr_aantal - srg.aant_gelev END) AS QtyInOrder
        FROM orsrg_Source srg
        INNER JOIN orkrg_Source krg ON krg.ordernr = srg.ordernr
        INNER JOIN Items_Source i ON i.ItemCode = srg.artcode 
        INNER JOIN ItemAssortment_Source ia ON ia.Assortment = i.Assortment 
        WHERE krg.afgehandld = 0 AND krg.ord_soort IN ('V','M') 
          AND i.condition IN ('A') AND i.Type IN ('S', 'B') 
        GROUP BY srg.artcode, srg.magcode 
        HAVING ROUND(SUM(srg.esr_aantal), 3) > ROUND(SUM(srg.aant_gelev), 3)
    )

-- 6. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    bal.warehouse,
    bal.ItemCode,
    CASE 
        WHEN i.condition = 'A' THEN 'Active' 
        WHEN i.condition = 'B' THEN 'Blocked' 
        WHEN i.condition = 'D' THEN 'Discontinued' 
        WHEN i.condition = 'E' THEN 'Inactive' 
        WHEN i.condition = 'F' THEN 'Future' 
    END AS condition,
    i.Description_0 AS ItemDescription,
    i.PackageDescription AS Unit,
    COALESCE(bal.QtyPreviousStock, 0) AS QtyPreviousStock,
    COALESCE(bal.QtyReceivedStock, 0) AS QtyReceivedStock,
    COALESCE(bal.QtyFulfilledStock, 0) AS QtyFulfilledStock,
    (COALESCE(bal.QtyPreviousStock, 0) + COALESCE(bal.QtyReceivedStock, 0) + COALESCE(bal.QtyFulfilledStock, 0)) AS QtyActualStock,
    COALESCE(bal.QtyToBeReceived, 0) AS QtyToBeReceived,
    COALESCE(srg_final.QtyInOrder, 0) AS QtyInOrder,
    COALESCE(vrd.CostPrice, i.CostPriceStandard) AS CostPriceStandard,
    TRY_CAST(i.Userfield_01 AS DOUBLE) AS VolumeM3,
    rd.maxvrd AS NMAX,
    rd.bestniv AS NMIN,
    1 AS Sort,
    
    -- CỘT "COVER"
    bal.Date_Cover,
    bal.Warehouse_Cover,
    bal.ItemGroup_Cover,
    bal.ItemCodeFrom_Cover,
    bal.ItemCodeTo_Cover,
    bal.ItemClass1_Cover,
    CASE 
        WHEN (COALESCE(bal.QtyPreviousStock,0) + COALESCE(bal.QtyReceivedStock,0) + COALESCE(bal.QtyFulfilledStock,0) + 
              COALESCE(bal.QtyToBeReceived,0) + COALESCE(srg_final.QtyInOrder,0)) <> 0 THEN TRUE
        ELSE FALSE
    END AS HasTransaction_Cover
    
FROM bal
INNER JOIN Items_Source i ON bal.ItemCode = i.ItemCode
INNER JOIN ItemAssortment_Source ia ON ia.Assortment = i.Assortment
LEFT JOIN voorrd_Source rd ON i.ItemCode = rd.artcode AND rd.magcode IN ('K21A','NA1')
LEFT JOIN ItemAccounts_Source al ON al.ItemCode = i.ItemCode AND al.crdnr = i.lev_crdnr
-- JOIN lại với kết quả QtyInOrder (SO/MO đang mở)
LEFT JOIN srg_final ON srg_final.artcode = bal.ItemCode AND srg_final.magcode = bal.warehouse
-- Lấy CostPrice của kho
LEFT JOIN voorrd_Source vrd ON vrd.magcode = bal.warehouse AND vrd.artcode = bal.ItemCode
WHERE 
    i.condition IN ('A') AND i.Type IN ('S', 'B') 
    AND (COALESCE(bal.QtyPreviousStock,0) + COALESCE(bal.QtyReceivedStock,0) + COALESCE(bal.QtyFulfilledStock,0) + 
         COALESCE(bal.QtyToBeReceived,0) + COALESCE(srg_final.QtyInOrder,0)) <> 0 -- Chỉ lấy các mặt hàng có giao dịch hoặc PO/SO mở
ORDER BY 
    i.Assortment, i.ItemCode, bal.warehouse

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}