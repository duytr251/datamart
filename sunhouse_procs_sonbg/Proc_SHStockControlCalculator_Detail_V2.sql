{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ItemCode và ngày ETL
        unique_key=['data_date', 'itemcode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. CTE: Tính toán các mốc ngày dựa trên ngày hiện tại (Bắt buộc phải giữ nguyên)
    DateParams AS (
        SELECT
            CURRENT_DATE AS ViewDate,
            date_trunc('month', CURRENT_DATE) AS FirstDayOfMonth,
            last_day_of_month(CURRENT_DATE) AS LastDayOfMonth,
            
            date_add('month', -6, date_trunc('month', CURRENT_DATE)) AS LKT6,
            date_add('month', -5, date_trunc('month', CURRENT_DATE)) AS LKT5,
            date_add('month', -4, date_trunc('month', CURRENT_DATE)) AS LKT4,
            date_add('month', -3, date_trunc('month', CURRENT_DATE)) AS LKT3,
            date_add('month', -2, date_trunc('month', CURRENT_DATE)) AS LKT2,
            date_add('month', -1, date_trunc('month', CURRENT_DATE)) AS LKT1,
            
            date_add('month', 5, date_add('year', -1, date_trunc('month', CURRENT_DATE))) AS CKT6,
            date_add('month', 4, date_add('year', -1, date_trunc('month', CURRENT_DATE))) AS CKT5,
            date_add('month', 3, date_add('year', -1, date_trunc('month', CURRENT_DATE))) AS CKT4,
            date_add('month', 2, date_add('year', -1, date_trunc('month', CURRENT_DATE))) AS CKT3,
            date_add('month', 1, date_add('year', -1, date_trunc('month', CURRENT_DATE))) AS CKT2,
            date_add('year', -1, date_trunc('month', CURRENT_DATE)) AS CKT1,
            
            date_add('month', 1, date_trunc('month', CURRENT_DATE)) AS KHT1,
            date_add('month', 2, date_trunc('month', CURRENT_DATE)) AS KHT2,
            date_add('month', 3, date_trunc('month', CURRENT_DATE)) AS KHT3,
            date_add('month', 4, date_trunc('month', CURRENT_DATE)) AS KHT4,
            date_add('month', 5, date_trunc('month', CURRENT_DATE)) AS KHT5,
            
            last_day_of_month(date_add('month', -1, CURRENT_DATE)) AS LastLKT1,
            last_day_of_month(date_add('month', 5, CURRENT_DATE)) AS LastKHT5,
            
            CAST(YEAR(CURRENT_DATE) AS INTEGER) AS CurrentYear
    ),

    -- 1. CTE: Base Items (Lọc danh sách Item chính)
    BaseItems AS (
        SELECT i.ItemCode, i.ItemName, i.IndustryCode, i.Class_01, i.Class_05, i.Class_07,
               i.UserYesNo_05, i.UserField_06, i.Assortment, i.PurchaseOrderSize,
               i.UserNumber_06 as MOQ, i.UserNumber_10 as NumDayOfManufacture, 
               i.UserNumber_11 as NumDayOfRoad, i.UserYesNo_03 AS IsDLItem
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }} i
        WHERE 
            i.ItemType NOT IN ('P','R')
            AND (i.Assortment BETWEEN 100 AND 350 OR (i.Assortment = 900 AND i.Class_01 = 'RO1'))
            AND i.ItemName NOT LIKE '%carton%'
    ),

    -- 2. CTE: Sales Forecast Data (KH)
    SalesForecastData AS (
        SELECT a.ItemCode,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT6 THEN a.SFQuantity ELSE 0 END) AS KHLKT6,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT5 THEN a.SFQuantity ELSE 0 END) AS KHLKT5,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT4 THEN a.SFQuantity ELSE 0 END) AS KHLKT4,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT3 THEN a.SFQuantity ELSE 0 END) AS KHLKT3,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT2 THEN a.SFQuantity ELSE 0 END) AS KHLKT2,
            SUM(CASE WHEN date_add('day', -14, a.DateManual) = d.LKT1 THEN a.SFQuantity ELSE 0 END) AS KHLKT1
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__View_Saleforecast') }} a
        CROSS JOIN DateParams d
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = a.ItemCode)
          AND a.DateManual BETWEEN d.LKT6 AND d.LastLKT1
        GROUP BY a.ItemCode
    ),

    -- 3. CTE: Sales Actual Data (TH)
    SalesActualData AS (
        SELECT a.artcode as ItemCode,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT6 THEN a.SaleQuantity ELSE 0 END) AS THLKT6,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT6 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT6_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT6 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT6_MN,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT5 THEN a.SaleQuantity ELSE 0 END) AS THLKT5,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT5 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT5_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT5 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT5_MN,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT4 THEN a.SaleQuantity ELSE 0 END) AS THLKT4,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT4 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT4_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT4 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT4_MN,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT3 THEN a.SaleQuantity ELSE 0 END) AS THLKT3,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT3 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT3_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT3 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT3_MN,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT2 THEN a.SaleQuantity ELSE 0 END) AS THLKT2,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT2 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT2_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT2 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT2_MN,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT1 THEN a.SaleQuantity ELSE 0 END) AS THLKT1,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT1 AND a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS THLKT1_MB,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual) = d.LKT1 AND a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS THLKT1_MN
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHBaseSaleBIData') }} a
        CROSS JOIN DateParams d
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = a.artcode)
          AND a.SaleDateManual BETWEEN d.LKT6 AND d.LastLKT1
        GROUP BY a.artcode
    ),

    -- 4. CTE: Sales Forecast Data (CK)
    SalesForecastDataCK AS (
        SELECT a.ItemCode,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT6 THEN a.SFQuantity ELSE 0 END) AS KHCK6,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT5 THEN a.SFQuantity ELSE 0 END) AS KHCK5,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT4 THEN a.SFQuantity ELSE 0 END) AS KHCK4,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT3 THEN a.SFQuantity ELSE 0 END) AS KHCK3,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT2 THEN a.SFQuantity ELSE 0 END) AS KHCK2,
            SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.CKT1 THEN a.SFQuantity ELSE 0 END) AS KHCK1
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__View_Saleforecast') }} a
        CROSS JOIN DateParams d
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = a.ItemCode)
          AND a.DateManual BETWEEN d.CKT1 AND date_add('month', 5, last_day_of_month(d.CKT1))
        GROUP BY a.ItemCode
        HAVING SUM(a.SFQuantity) <> 0
    ),

    -- 5. CTE: Sales Actual Data (CK)
    SalesActualDataCK AS (
        SELECT a.artcode AS ItemCode,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT6 THEN a.SaleQuantity ELSE 0 END) AS THCK6,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT5 THEN a.SaleQuantity ELSE 0 END) AS THCK5,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT4 THEN a.SaleQuantity ELSE 0 END) AS THCK4,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT3 THEN a.SaleQuantity ELSE 0 END) AS THCK3,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT2 THEN a.SaleQuantity ELSE 0 END) AS THCK2,
            SUM(CASE WHEN date_add('day', -14, a.SaleDateManual)=d.CKT1 THEN a.SaleQuantity ELSE 0 END) AS THCK1
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHBaseSaleBIData') }} a
        CROSS JOIN DateParams d
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = a.artcode)
          AND a.SaleDateManual BETWEEN d.CKT1 AND date_add('month', 5, last_day_of_month(d.CKT1))
        GROUP BY a.artcode
        HAVING SUM(a.SaleQuantity) <> 0
    ),

    -- 6. CTE: Sales Forecast Data 6 tháng (KHT)
    SaleForecastData6Month AS (
        SELECT G.ItemCode,
            SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ1 ELSE 0 END) AS KHDBMB1, SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ2 ELSE 0 END) AS KHDBMB2,
            SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ3 ELSE 0 END) AS KHDBMB3, SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ4 ELSE 0 END) AS KHDBMB4,
            SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ5 ELSE 0 END) AS KHDBMB5, SUM(CASE WHEN cc.Region='MB' THEN KHDBTQ6 ELSE 0 END) AS KHDBMB6,
            SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ1 ELSE 0 END) AS KHDBMN1, SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ2 ELSE 0 END) AS KHDBMN2,
            SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ3 ELSE 0 END) AS KHDBMN3, SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ4 ELSE 0 END) AS KHDBMN4,
            SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ5 ELSE 0 END) AS KHDBMN5, SUM(CASE WHEN cc.Region='MN' THEN KHDBTQ6 ELSE 0 END) AS KHDBMN6,
            SUM(KHDBTQ1) AS KHDBTQ1, SUM(KHDBTQ2) AS KHDBTQ2, SUM(KHDBTQ3) AS KHDBTQ3, 
            SUM(KHDBTQ4) AS KHDBTQ4, SUM(KHDBTQ5) AS KHDBTQ5, SUM(KHDBTQ6) AS KHDBTQ6
        FROM (
            SELECT a.CostCenter, a.ItemCode,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.FirstDayOfMonth THEN a.SFQuantity ELSE 0 END) AS KHDBTQ1,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.KHT1 THEN a.SFQuantity ELSE 0 END) AS KHDBTQ2,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.KHT2 THEN a.SFQuantity ELSE 0 END) AS KHDBTQ3,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.KHT3 THEN a.SFQuantity ELSE 0 END) AS KHDBTQ4,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.KHT4 THEN a.SFQuantity ELSE 0 END) AS KHDBTQ5,
                SUM(CASE WHEN date_add('day', -14, a.DateManual)=d.KHT5 THEN a.SFQuantity ELSE 0 END) AS KHDBTQ6
            FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__View_Saleforecast') }} a
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = a.ItemCode)
              AND a.DateManual BETWEEN d.FirstDayOfMonth AND d.LastKHT5
            GROUP BY a.CostCenter, a.ItemCode
            HAVING SUM(a.SFQuantity) <> 0
        ) G
        JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHCostcenter') }} cc ON G.CostCenter = cc.CostCenter
        GROUP BY G.ItemCode
    ),

    -- 7. CTE: Fulfill Data
    FulfillData AS (
        SELECT G.ItemCode, SUM(G.QuanPlan) AS QuanPlan, SUM(G.QuanPlanMB) AS QuanPlanMB, SUM(G.QuanPlanMT) AS QuanPlanMT, SUM(G.QuanPlanMN) AS QuanPlanMN, SUM(G.QuanActualCK) AS QuanActualCK
        FROM (
            SELECT a.ItemCode, SUM(CASE WHEN a.Region = 'MB' THEN a.SaleQuantity ELSE 0 END) AS QuanPlanMB, SUM(CASE WHEN a.Region = 'MT' THEN a.SaleQuantity ELSE 0 END) AS QuanPlanMT,
                SUM(CASE WHEN a.Region = 'MN' THEN a.SaleQuantity ELSE 0 END) AS QuanPlanMN, SUM(a.SaleQuantity) AS QuanPlan, 0 AS QuanActualCK
            FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHBaseSaleFullfilResult') }} a
            CROSS JOIN DateParams d
            WHERE a.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth AND a.Division = 101
            GROUP BY a.ItemCode
            
            UNION ALL
            
            SELECT a.artcode AS ItemCode, 0 AS QuanPlanMB, 0 AS QuanPlanMT, 0 AS QuanPlanMN, 0 AS QuanPlan, SUM(a.SaleQuantity) AS QuanActualCK
            FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHBaseSaleBIData') }} a
            CROSS JOIN DateParams d
            WHERE a.SaleDateManual BETWEEN date_add('year', -1, d.FirstDayOfMonth) AND date_add('year', -1, d.LastDayOfMonth)
            GROUP BY a.artcode
        ) G
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = G.ItemCode)
        GROUP BY G.ItemCode
    ),

    -- 8. CTE: OnOrder
    OnOrderData AS (
        SELECT G.ItemCode, SUM(CASE WHEN cc.Region='MB' THEN g.OnOrderTQ ELSE 0 END) AS OnOrderMB, SUM(CASE WHEN cc.Region='MT' THEN g.OnOrderTQ ELSE 0 END) AS OnOrderMT,
            SUM(CASE WHEN cc.Region='MN' THEN g.OnOrderTQ ELSE 0 END) AS OnOrderMN, SUM(G.OnOrderTQ) AS OnOrderTQ
        FROM (
            SELECT TRIM(k.kstplcode) AS kstplcode, s.artcode AS ItemCode, SUM(s.esr_aantal - s.aant_gelev) AS OnOrderTQ
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON s.ordernr = k.ordernr
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = s.artcode)
              AND s.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth 
              AND k.ord_soort='V' AND k.fiattering='J' AND s.ar_soort<>'P'
            GROUP BY k.kstplcode, s.artcode
            HAVING SUM(s.esr_aantal - s.aant_gelev) <> 0
        ) g
        JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHCostcenter') }} cc ON cc.CostCenter = g.kstplcode
        WHERE cc.ChannelCode <> 'TL'
        GROUP BY G.ItemCode
    ),

    -- 9. CTE: Road Goods Data
    RoadGoodsData AS (
        SELECT a.ItemCode, a.OnRoadT0, a.OnRoadT1, a.OnRoadT2, a.OnRoadT3, a.OnRoadT4, a.OnRoadT5
        FROM (
            SELECT s.artcode AS ItemCode,
                SUM(CASE WHEN s.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth THEN s.esr_aantal ELSE 0 END) AS OnRoadT0,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT1 AND last_day_of_month(d.KHT1) THEN s.esr_aantal ELSE 0 END) AS OnRoadT1,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT2 AND last_day_of_month(d.KHT2) THEN s.esr_aantal ELSE 0 END) AS OnRoadT2,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT3 AND last_day_of_month(d.KHT3) THEN s.esr_aantal ELSE 0 END) AS OnRoadT3,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT4 AND last_day_of_month(d.KHT4) THEN s.esr_aantal ELSE 0 END) AS OnRoadT4,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT5 AND last_day_of_month(d.KHT5) THEN s.esr_aantal ELSE 0 END) AS OnRoadT5
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON k.ordernr = s.ordernr
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = s.artcode)
              AND k.ord_soort='B' AND k.fiattering='J' AND s.aant_gelev = 0 
              AND s.afldat BETWEEN d.FirstDayOfMonth AND d.LastKHT5
            GROUP BY s.artcode
        ) a
    ),

    -- 10. CTE: Ordered Goods Data
    OrderedGoodsData AS (
        SELECT a.artcode AS ItemCode, 
            SUM(a.BookedT0) AS BookedT0, SUM(a.BookedT1) AS BookedT1, 
            SUM(a.BookedT2) AS BookedT2, SUM(a.BookedT3) AS BookedT3
        FROM (
            SELECT s.artcode,
                SUM(CASE WHEN s.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth THEN s.esr_aantal ELSE 0 END) AS BookedT0,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT1 AND last_day_of_month(d.KHT1) THEN s.esr_aantal ELSE 0 END) AS BookedT1,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT2 AND last_day_of_month(d.KHT2) THEN s.esr_aantal ELSE 0 END) AS BookedT2,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT3 AND last_day_of_month(d.KHT3) THEN s.esr_aantal ELSE 0 END) AS BookedT3
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON k.ordernr = s.ordernr
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__Items') }} i ON TRIM(s.artcode) = i.ItemCode
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems bi WHERE bi.ItemCode = s.artcode)
              AND k.ord_soort='K' AND s.afldat BETWEEN d.FirstDayOfMonth AND last_day_of_month(d.KHT3)
              AND i.Assortment <= 400 AND i.Class_07 ='SX' AND k.afgehandld = 0
            GROUP BY s.artcode
            
            UNION ALL
            
            SELECT s.artcode, SUM(CASE WHEN s.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth THEN s.esr_aantal ELSE 0 END) AS BookedT0,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT1 AND last_day_of_month(d.KHT1) THEN s.esr_aantal ELSE 0 END) AS BookedT1,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT2 AND last_day_of_month(d.KHT2) THEN s.esr_aantal ELSE 0 END) AS BookedT2,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT3 AND last_day_of_month(d.KHT3) THEN s.esr_aantal ELSE 0 END) AS BookedT3
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON k.ordernr = s.ordernr
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__Items') }} i ON TRIM(s.artcode) = i.ItemCode
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems bi WHERE bi.ItemCode = s.artcode)
              AND k.ord_soort='K' AND s.afldat BETWEEN d.FirstDayOfMonth AND last_day_of_month(d.KHT3)
              AND i.Assortment <= 400 AND i.Class_07 ='NK' AND k.afgehandld = 0 AND k.crdnr ='832463'
              AND k.orddat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth
            GROUP BY s.artcode
            
            UNION ALL
            
            SELECT s.artcode, SUM(CASE WHEN s.afldat BETWEEN d.FirstDayOfMonth AND d.LastDayOfMonth THEN s.esr_aantal ELSE 0 END) AS BookedT0,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT1 AND last_day_of_month(d.KHT1) THEN s.esr_aantal ELSE 0 END) AS BookedT1,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT2 AND last_day_of_month(d.KHT2) THEN s.esr_aantal ELSE 0 END) AS BookedT2,
                SUM(CASE WHEN s.afldat BETWEEN d.KHT3 AND last_day_of_month(d.KHT3) THEN s.esr_aantal ELSE 0 END) AS BookedT3
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON k.ordernr = s.ordernr
            JOIN {{ source('dp_warehouse_staging', 'stg_exact101__Items') }} i ON TRIM(s.artcode) = i.ItemCode
            CROSS JOIN DateParams d
            WHERE EXISTS (SELECT 1 FROM BaseItems bi WHERE bi.ItemCode = s.artcode)
              AND k.ord_soort='K' AND s.afldat BETWEEN d.FirstDayOfMonth AND last_day_of_month(d.KHT3)
              AND i.Assortment <= 400 AND i.Class_07 IN ('TN','VIET') AND k.crdnr ='832462' AND k.afgehandld = 0
            GROUP BY s.artcode
        ) a
        GROUP BY a.artcode
    ),

    -- 11. CTE: Stock Data
    StockData AS (
        SELECT sb.ItemCode,
            SUM(CASE WHEN b.Region='MB' AND b.int_regio<>'K' AND b.magcode NOT IN ('BC21', 'BC22','BCYS') THEN sb.Quantity ELSE 0 END) as QuantityMB,
            SUM(CASE WHEN b.Region='MT' AND b.int_regio<>'K' AND b.magcode <> 'TCMT' THEN sb.Quantity ELSE 0 END) as QuantityMT,
            SUM(CASE WHEN b.Region='MN' AND b.int_regio<>'K' AND b.magcode NOT IN ('NRWO', 'NRWL') THEN sb.Quantity ELSE 0 END) as QuantityMN,
            SUM(CASE WHEN b.magcode IN ('NRWO', 'NRWL', 'BC21','BC22','TCMT','BCYS') THEN sb.Quantity ELSE 0 END) as QuantityRework,
            SUM(CASE WHEN b.magcode IN ('BC21','BC22','BCYS') THEN sb.Quantity ELSE 0 END) as Quantity_RW_MB,
            SUM(CASE WHEN b.magcode IN ('TCMT') THEN sb.Quantity ELSE 0 END) as Quantity_RW_MT,
            SUM(CASE WHEN b.magcode IN ('NRWO','NRWL') THEN sb.Quantity ELSE 0 END) as Quantity_RW_MN,
            SUM(CASE WHEN b.magcode IN ('BAMZ','BATA','BBOX','BLZD','BMET', 'BSBS', 'BTKI') THEN sb.Quantity ELSE 0 END) as QuanKG_TMDT
        FROM (
            SELECT ItemCode, Warehouse, SUM(Quantity) as Quantity
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__StockBalances_sync') }}
            CROSS JOIN DateParams d
            WHERE Date = d.ViewDate -- Lấy đúng ngày ViewDate
            GROUP BY ItemCode, Warehouse
            HAVING SUM(Quantity) <> 0
        ) sb
        INNER JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }} b ON sb.Warehouse = b.magcode 
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = sb.ItemCode)
          AND b.Division = 101 AND b.int_regio IN('A','T','K')
        GROUP BY sb.ItemCode
    ),

    -- 12. CTE: IBT Data
    IBTData AS (
        SELECT G.artcode AS ItemCode,
            SUM(CASE WHEN m.Region = 'MB' AND m.magcode IN ('BA21','BA22','BAGT', 'BAYS') THEN G.aantal ELSE 0 END) AS IBT_MB,
            SUM(CASE WHEN m.Region = 'MT' AND m.magcode IN ('TAMT') THEN G.aantal ELSE 0 END) AS IBT_MT,
            SUM(CASE WHEN m.Region = 'MN' AND m.magcode IN ('NALA','NAOV') THEN G.aantal ELSE 0 END) AS IBT_MN,
            SUM(CASE WHEN m.Region = 'MN' AND m.magcode IN ('NALA') THEN G.aantal ELSE 0 END) AS IBT_NALA_MN,
            SUM(CASE WHEN m.Region = 'MN' AND m.magcode IN ('NAOV') THEN G.aantal ELSE 0 END) AS IBT_NAOV_MN
        FROM (
            SELECT a.IBTDeliveryNr, a.artcode, SUM(a.aantal) AS aantal 
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }} a
            CROSS JOIN DateParams d
            WHERE a.IBTDeliveryNr IS NOT NULL AND a.transtype = 'N' AND a.warehouse = 'TRAN'
              AND EXISTS (
                  SELECT 1 FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }} g 
                  WHERE g.IBTDeliveryNr IS NOT NULL AND g.transtype = 'N' AND g.warehouse <> 'TRAN' 
                    AND g.afldat BETWEEN date_add('month', -1, d.FirstDayOfMonth) AND d.ViewDate 
                    AND g.IBTDeliveryNr = a.IBTDeliveryNr
              )
            GROUP BY a.IBTDeliveryNr, a.artcode
            HAVING SUM(a.aantal) <> 0
        ) G
        LEFT JOIN LATERAL (
            SELECT DISTINCT b.warehouse 
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }} b 
            WHERE b.IBTDeliveryNr = G.IBTDeliveryNr AND b.transtype = 'B' AND b.transsubtype = 'A' AND b.warehouse<>'TRAN'
        ) B ON TRUE
        JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }} m ON B.warehouse = m.magcode AND m.Division = 101
        WHERE EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = G.artcode)
        GROUP BY G.artcode
        HAVING SUM(G.aantal) <> 0
    ),

    -- 13. CTE: Saleout Quantity Data
    SaleoutQuantityData AS (
        SELECT G.ItemClassCode,
            SUM(CASE WHEN S.SMonth = MONTH(d.LKT1) AND S.SYear = YEAR(d.LKT1) THEN S.SaleQuantity ELSE 0 END ) AS THLKO_T1,
            SUM(CASE WHEN S.SMonth = MONTH(d.LKT2) AND S.SYear = YEAR(d.LKT2) THEN S.SaleQuantity ELSE 0 END ) AS THLKO_T2,
            SUM(CASE WHEN S.SMonth = MONTH(d.LKT3) AND S.SYear = YEAR(d.LKT3) THEN S.SaleQuantity ELSE 0 END ) AS THLKO_T3,
            SUM(CASE WHEN S.SMonth = MONTH(d.LKT4) AND S.SYear = YEAR(d.LKT4) THEN S.SaleQuantity ELSE 0 END ) AS THLKO_T4
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__ProPlanItemSaleUpload') }} S
        JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__ProPlanItemGroupItemClass') }} G ON S.GroupCode = G.GroupCode
        CROSS JOIN DateParams d
        WHERE S.SDate >= date_add('month', -4, d.FirstDayOfMonth)
        GROUP BY G.ItemClassCode
    ),

    -- 14. CTE: BQNLK
    BQNLKData AS (
        SELECT R.ItemCode, 
            ROUND((SUM(SaleQuantity) / CAST(MONTH(MAX(R.afldat)) AS DOUBLE)), 0) AS BQNTHLK
        FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHBaseSaleFullfilResult') }} R 
        CROSS JOIN DateParams d
        WHERE YEAR(afldat) = d.CurrentYear AND afldat <= d.FirstDayOfMonth
        GROUP BY R.ItemCode
    ),
    
    -- 15. CTE: Cost Price
    CostPriceData AS (
        SELECT T.ItemCode, T.NewCost AS CostPrice
        FROM (
            SELECT H.ItemCode, H.NewCost,
                ROW_NUMBER() OVER (PARTITION BY H.ItemCode ORDER BY H.ChangedDate DESC) as rn
            FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems_CostHistory') }} H
            WHERE H.ChangedDate <= CURRENT_TIMESTAMP
              AND EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = H.ItemCode)
        ) T
        WHERE T.rn = 1
    ),
    
    -- 16. CTE: XNK Info
    XNKData AS (
        SELECT DISTINCT lo.artcode AS ItemCode, ia.AccountCode, ia.PurchaseCurrency, c.cmp_name, u.FullName, ia.PurchaseOrderSize
        FROM (
            SELECT s.artcode, MAX(s.syscreated) as MaxDate
            FROM {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s
            INNER JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k ON s.ordernr = k.ordernr
            WHERE k.ord_soort = 'B' AND s.ar_soort <> 'P'
              AND EXISTS (SELECT 1 FROM BaseItems i WHERE i.ItemCode = s.artcode)
            GROUP BY s.artcode
        ) lo
        INNER JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }} s ON lo.artcode = s.artcode AND lo.MaxDate = s.syscreated
        INNER JOIN {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }} k ON s.ordernr = k.ordernr
        INNER JOIN {{ source('dp_warehouse_staging', 'stg_exact101__cicmpy') }} c ON c.crdnr = k.crdnr
        LEFT JOIN {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }} u ON u.res_id = k.represent_id AND u.Division = k.Division AND u.Active = TRUE
        LEFT JOIN {{ source('dp_warehouse_staging', 'stg_exact101__ItemAccounts') }} ia ON c.cmp_wwn = ia.AccountCode AND ia.ItemCode = lo.artcode AND ia.MainAccount = TRUE
    ),

    -- 17. CTE: DRO Data
    DROData AS (
        SELECT a.ItemCode, a.SeviceLevel, a.SafetyFactor
        FROM (
            SELECT ItemCode, SeviceLevel, SafetyFactor, FromDate,
                ROW_NUMBER() OVER (PARTITION BY ItemCode ORDER BY FromDate DESC) as rn
            FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItemDRO') }}
            CROSS JOIN DateParams d
            WHERE FromDate <= d.ViewDate
        ) a
        WHERE a.rn = 1
    ),
    
    -- 18. CTE: Main Calculation (Gom tất cả các chỉ số)
    CalculatedData AS (
        SELECT 
            i.ItemCode, i.ItemName, i.Class_05, i.Class_07,
            CAST(i.Assortment AS VARCHAR(10)) || '-' || i.IndustryCode AS IndustryCode,
            CASE WHEN i.UserYesNo_05 = FALSE THEN 'ON' ELSE 'OFF' END AS UserYesNo_05,
            i.UserField_06, i.Class_01 AS ItemClassCode, i.Assortment, i.PurchaseOrderSize AS POSize, i.MOQ, i.IsDLItem,
            ROUND(COALESCE(sf.KHLKT6, 0), 0) AS KHLKT6, ROUND(COALESCE(sf.KHLKT5, 0), 0) AS KHLKT5, ROUND(COALESCE(sf.KHLKT4, 0), 0) AS KHLKT4, ROUND(COALESCE(sf.KHLKT3, 0), 0) AS KHLKT3, ROUND(COALESCE(sf.KHLKT2, 0), 0) AS KHLKT2, ROUND(COALESCE(sf.KHLKT1, 0), 0) AS KHLKT1,
            ROUND(COALESCE(sfc.KHCK6, 0), 0) AS KHCK6, ROUND(COALESCE(sfc.KHCK5, 0), 0) AS KHCK5, ROUND(COALESCE(sfc.KHCK4, 0), 0) AS KHCK4, ROUND(COALESCE(sfc.KHCK3, 0), 0) AS KHCK3, ROUND(COALESCE(sfc.KHCK2, 0), 0) AS KHCK2, ROUND(COALESCE(sfc.KHCK1, 0), 0) AS KHCK1,
            ROUND(COALESCE(sa.THLKT6, 0), 0) AS THLKT6, ROUND(COALESCE(sa.THLKT6_MB, 0), 0) AS THLKT6_MB, ROUND(COALESCE(sa.THLKT6_MN, 0), 0) AS THLKT6_MN, 
            ROUND(COALESCE(sa.THLKT5, 0), 0) AS THLKT5, ROUND(COALESCE(sa.THLKT5_MB, 0), 0) AS THLKT5_MB, ROUND(COALESCE(sa.THLKT5_MN, 0), 0) AS THLKT5_MN,
            ROUND(COALESCE(sa.THLKT4, 0), 0) AS THLKT4, ROUND(COALESCE(sa.THLKT4_MB, 0), 0) AS THLKT4_MB, ROUND(COALESCE(sa.THLKT4_MN, 0), 0) AS THLKT4_MN, 
            ROUND(COALESCE(sa.THLKT3, 0), 0) AS THLKT3, ROUND(COALESCE(sa.THLKT3_MB, 0), 0) AS THLKT3_MB, ROUND(COALESCE(sa.THLKT3_MN, 0), 0) AS THLKT3_MN,
            ROUND(COALESCE(sa.THLKT2, 0), 0) AS THLKT2, ROUND(COALESCE(sa.THLKT2_MB, 0), 0) AS THLKT2_MB, ROUND(COALESCE(sa.THLKT2_MN, 0), 0) AS THLKT2_MN, 
            ROUND(COALESCE(sa.THLKT1, 0), 0) AS THLKT1, ROUND(COALESCE(sa.THLKT1_MB, 0), 0) AS THLKT1_MB, ROUND(COALESCE(sa.THLKT1_MN, 0), 0) AS THLKT1_MN,
            ROUND(COALESCE(sac.THCK6, 0), 0) AS THCK6, ROUND(COALESCE(sac.THCK5, 0), 0) AS THCK5, ROUND(COALESCE(sac.THCK4, 0), 0) AS THCK4, ROUND(COALESCE(sac.THCK3, 0), 0) AS THCK3, ROUND(COALESCE(sac.THCK2, 0), 0) AS THCK2, ROUND(COALESCE(sac.THCK1, 0), 0) AS THCK1,
            ROUND(COALESCE(sf6.KHDBMB6, 0), 0) AS KHDBMB6, ROUND(COALESCE(sf6.KHDBMB5, 0), 0) AS KHDBMB5, ROUND(COALESCE(sf6.KHDBMB4, 0), 0) AS KHDBMB4, ROUND(COALESCE(sf6.KHDBMB3, 0), 0) AS KHDBMB3, ROUND(COALESCE(sf6.KHDBMB2, 0), 0) AS KHDBMB2, ROUND(COALESCE(sf6.KHDBMB1, 0), 0) AS KHDBMB1,
            ROUND(COALESCE(sf6.KHDBMN6, 0), 0) AS KHDBMN6, ROUND(COALESCE(sf6.KHDBMN5, 0), 0) AS KHDBMN5, ROUND(COALESCE(sf6.KHDBMN4, 0), 0) AS KHDBMN4, ROUND(COALESCE(sf6.KHDBMN3, 0), 0) AS KHDBMN3, ROUND(COALESCE(sf6.KHDBMN2, 0), 0) AS KHDBMN2, ROUND(COALESCE(sf6.KHDBMN1, 0), 0) AS KHDBMN1,
            ROUND(COALESCE(sf6.KHDBTQ6, 0), 0) AS KHDBTQ6, ROUND(COALESCE(sf6.KHDBTQ5, 0), 0) AS KHDBTQ5, ROUND(COALESCE(sf6.KHDBTQ4, 0), 0) AS KHDBTQ4, ROUND(COALESCE(sf6.KHDBTQ3, 0), 0) AS KHDBTQ3, ROUND(COALESCE(sf6.KHDBTQ2, 0), 0) AS KHDBTQ2, ROUND(COALESCE(sf6.KHDBTQ1, 0), 0) AS KHDBTQ1,
            ROUND(COALESCE(ff.QuanPlanMB, 0), 0) AS QuanPlanMB, ROUND(COALESCE(ff.QuanPlanMT, 0), 0) AS QuanPlanMT, ROUND(COALESCE(ff.QuanPlanMN, 0), 0) AS QuanPlanMN, ROUND(COALESCE(ff.QuanPlan, 0), 0) AS QuanPlan, ROUND(COALESCE(ff.QuanActualCK, 0), 0) AS QuanActualCK,
            ROUND(COALESCE(oo.OnOrderMB, 0), 0) AS OnOrderMB, ROUND(COALESCE(oo.OnOrderMT, 0), 0) AS OnOrderMT, ROUND(COALESCE(oo.OnOrderMN, 0), 0) AS OnOrderMN, ROUND(COALESCE(oo.OnOrderTQ, 0), 0) AS OnOrderTQ,
            ROUND(COALESCE(rg.OnRoadT5, 0), 0) + ROUND(COALESCE(rg.OnRoadT4, 0), 0) + ROUND(COALESCE(rg.OnRoadT3, 0), 0) + ROUND(COALESCE(rg.OnRoadT2, 0), 0) + ROUND(COALESCE(rg.OnRoadT1, 0), 0) + ROUND(COALESCE(rg.OnRoadT0, 0), 0) AS TotalOnRoad,
            ROUND(COALESCE(rg.OnRoadT5, 0), 0) AS OnRoadT5, ROUND(COALESCE(rg.OnRoadT4, 0), 0) AS OnRoadT4, ROUND(COALESCE(rg.OnRoadT3, 0), 0) AS OnRoadT3, ROUND(COALESCE(rg.OnRoadT2, 0), 0) AS OnRoadT2, ROUND(COALESCE(rg.OnRoadT1, 0), 0) AS OnRoadT1, ROUND(COALESCE(rg.OnRoadT0, 0), 0) AS OnRoadT0,
            ROUND(COALESCE(og.BookedT3, 0), 0) + ROUND(COALESCE(og.BookedT2, 0), 0) + ROUND(COALESCE(og.BookedT1, 0), 0) + ROUND(COALESCE(og.BookedT0, 0), 0) AS BookedAll,
            ROUND(COALESCE(og.BookedT3, 0), 0) AS BookedT3, ROUND(COALESCE(og.BookedT2, 0), 0) AS BookedT2, ROUND(COALESCE(og.BookedT1, 0), 0) AS BookedT1, ROUND(COALESCE(og.BookedT0, 0), 0) AS BookedT0,
            ROUND(COALESCE(ibt.IBT_MB, 0), 0) AS IBT_MB, ROUND(COALESCE(ibt.IBT_MT, 0), 0) AS IBT_MT, ROUND(COALESCE(ibt.IBT_MN, 0), 0) AS IBT_MN, ROUND(COALESCE(ibt.IBT_NALA_MN, 0), 0) AS IBT_NALA_MN, ROUND(COALESCE(ibt.IBT_NAOV_MN, 0), 0) AS IBT_NAOV_MN,
            ROUND(COALESCE(bq.BQNTHLK, 0), 0) AS BQNTHLK,
            ROUND(COALESCE(st.QuantityMB, 0), 0) AS QuantityMB, ROUND(COALESCE(st.QuantityMT, 0), 0) AS QuantityMT, ROUND(COALESCE(st.QuantityMN, 0), 0) AS QuantityMN, 
            ROUND(COALESCE(st.QuantityRework, 0), 0) AS QuantityRework, ROUND(COALESCE(st.Quantity_RW_MB, 0), 0) AS Quantity_RW_MB, ROUND(COALESCE(st.Quantity_RW_MT, 0), 0) AS Quantity_RW_MT, ROUND(COALESCE(st.Quantity_RW_MN, 0), 0) AS Quantity_RW_MN, ROUND(COALESCE(st.QuanKG_TMDT, 0), 0) AS QuanKG_TMDT,
            ROUND(COALESCE(i.NumDayOfManufacture, 0), 0) AS NumDayOfManufacture, ROUND(COALESCE(i.NumDayOfRoad, 0), 0) AS NumDayOfRoad,
            COALESCE(cp.CostPrice, 0) AS CostPrice, COALESCE(x.FullName, '') AS FullName, COALESCE(x.cmp_name, '') AS cmp_name, COALESCE(x.PurchaseCurrency, '') AS PurchaseCurrency,
            COALESCE(dro.SeviceLevel, 0) AS SeviceLevel, COALESCE(dro.SafetyFactor, 0) AS SafetyFactor
        FROM BaseItems i
        LEFT JOIN SalesForecastData sf ON i.ItemCode = sf.ItemCode LEFT JOIN SalesActualData sa ON i.ItemCode = sa.ItemCode LEFT JOIN StockData st ON i.ItemCode = st.ItemCode LEFT JOIN CostPriceData cp ON i.ItemCode = cp.ItemCode LEFT JOIN XNKData x ON i.ItemCode = x.ItemCode LEFT JOIN DROData dro ON i.ItemCode = dro.ItemCode LEFT JOIN SalesForecastDataCK sfc ON i.ItemCode = sfc.ItemCode LEFT JOIN SalesActualDataCK sac ON i.ItemCode = sac.ItemCode LEFT JOIN SaleForecastData6Month sf6 ON i.ItemCode = sf6.ItemCode LEFT JOIN FulfillData ff ON i.ItemCode = ff.ItemCode LEFT JOIN OnOrderData oo ON i.ItemCode = oo.ItemCode LEFT JOIN RoadGoodsData rg ON i.ItemCode = rg.ItemCode LEFT JOIN OrderedGoodsData og ON i.ItemCode = og.ItemCode LEFT JOIN IBTData ibt ON i.ItemCode = ibt.ItemCode LEFT JOIN BQNLKData bq ON i.ItemCode = bq.ItemCode
    ),
    
    -- 19. CTE: Final Calculations
    FinalData AS (
        SELECT d.*,
            (d.QuantityMB + d.QuantityMT + d.QuantityMN) AS QuantityTQ,
            ROUND((d.THLKT6 + d.THLKT5 + d.THLKT4 + d.THLKT3 + d.THLKT2 + d.THLKT1) / 180.0, 0) AS QuanAVGTHLKByDay,
            ROUND((KHDBTQ1 + KHDBTQ2 + KHDBTQ3)/90.0, 0) AS QuanAVGByDay,
            ROUND((ABS(KHLKT6 - THLKT6) + ABS(KHLKT5 - THLKT5) +ABS(KHLKT4 - THLKT4) +ABS(KHLKT3 - THLKT3) +ABS(KHLKT2 - THLKT2) +ABS(KHLKT1 - THLKT1)
                 + ABS(KHCK6 -THCK6) + ABS(KHCK5 -THCK5) +ABS(KHCK4 -THCK4) +ABS(KHCK3 -THCK3) +ABS(KHCK2 -THCK2) +ABS(KHCK1 -THCK1))/12.0, 0) AS MAD,
            (d.NumDayOfManufacture + d.NumDayOfRoad) AS LeadTime
        FROM CalculatedData d
    ),
    
    -- 20. CTE: Advanced Calculations
    CompleteData AS (
        SELECT d.*,
            (QuantityMB + QuantityMT + QuantityMN + QuanKG_TMDT + QuantityRework + TotalOnRoad) AS TotalQuanStock,
            ROUND(MAD * SafetyFactor, 0) AS SafetyStock,
            ROUND(QuanAVGByDay * NumDayOfRoad, 0) AS MinStock,
            ROUND(QuanAVGByDay * (NumDayOfManufacture + 2 * NumDayOfRoad), 0) AS MaxStock,
            ROUND(QuanAVGByDay * (NumDayOfManufacture + NumDayOfRoad), 0) AS SumMinStock,
            ROUND(QuanAVGByDay * 2 * (NumDayOfManufacture + NumDayOfRoad), 0) AS SumMaxStock
        FROM FinalData d
    ),
    
    -- 21. CTE: Final Business Logic
    ResultData AS (
        SELECT d.*,
            (SumMinStock + SafetyStock) AS ReOrderPoint,
            CASE 
                WHEN TotalQuanStock < (QuanAVGByDay * LeadTime + SafetyStock) 
                     AND UserYesNo_05 <> 'OFF' THEN 'Chạm' 
                ELSE '' 
            END AS StatusReOrderPoint,
            (CostPrice * QuantityTQ) AS CostQuantityTQ
        FROM CompleteData d
    )

-- 22. FINAL SELECT
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    d.ViewDate, MONTH(d.ViewDate) AS SMonth, YEAR(d.ViewDate) AS SYear,
    R.ItemCode, R.ItemName, R.Class_05, R.Class_07, R.IndustryCode,
    '' AS PropertiesItem, R.UserYesNo_05, R.UserField_06, R.ItemClassCode, R.Assortment, 
    ic.Description AS ItemClassName, R.FullName, R.cmp_name, R.PurchaseCurrency, 
    R.POSize, R.MOQ, R.IsDLItem, 
    
    R.KHLKT6, R.KHLKT5, R.KHLKT4, R.KHLKT3, R.KHLKT2, R.KHLKT1, R.THLKT6, R.THLKT6_MB, R.THLKT6_MN, R.THLKT5, R.THLKT5_MB, R.THLKT5_MN,
    R.THLKT4, R.THLKT4_MB, R.THLKT4_MN, R.THLKT3, R.THLKT3_MB, R.THLKT3_MN, R.THLKT2, R.THLKT2_MB, R.THLKT2_MN, R.THLKT1, R.THLKT1_MB, R.THLKT1_MN,
    R.KHCK6, R.KHCK5, R.KHCK4, R.KHCK3, R.KHCK2, R.KHCK1, R.THCK6, R.THCK5, R.THCK4, R.THCK3, R.THCK2, R.THCK1,
    R.KHDBMB1, R.KHDBMB2, R.KHDBMB3, R.KHDBMB4, R.KHDBMB5, R.KHDBMB6, R.KHDBMN1, R.KHDBMN2, R.KHDBMN3, R.KHDBMN4, R.KHDBMN5, R.KHDBMN6,
    R.KHDBTQ1, R.KHDBTQ2, R.KHDBTQ3, R.KHDBTQ4, R.KHDBTQ5, R.KHDBTQ6,
    R.QuanPlan, R.QuanPlanMB, R.QuanPlanMT, R.QuanPlanMN, R.QuanActualCK, R.OnOrderMB, R.OnOrderMT, R.OnOrderMN, R.OnOrderTQ,
    R.TotalOnRoad, R.OnRoadT5, R.OnRoadT4, R.OnRoadT3, R.OnRoadT2, R.OnRoadT1, R.OnRoadT0, R.BookedAll, R.BookedT3, R.BookedT2, R.BookedT1, R.BookedT0,
    R.IBT_MB, R.IBT_MT, R.IBT_MN, R.IBT_NALA_MN, R.IBT_NAOV_MN, R.BQNTHLK,
    R.QuantityMB, R.QuantityMT, R.QuantityMN, R.QuantityTQ, R.QuantityRework, 
    R.Quantity_RW_MB, R.Quantity_RW_MT, R.Quantity_RW_MN, R.QuanKG_TMDT,
    R.QuanAVGTHLKByDay, R.QuanAVGByDay, R.MAD, R.SafetyStock, R.MinStock, R.MaxStock, R.SumMinStock, R.SumMaxStock, R.ReOrderPoint, R.TotalQuanStock, R.StatusReOrderPoint,
    (R.SumMinStock + R.SafetyStock) - R.TotalQuanStock AS QuanNeedPurchase,
    
    CASE WHEN R.QuanAVGByDay = 0 THEN 0 ELSE ROUND((R.TotalQuanStock - R.ReOrderPoint) / NULLIF(R.QuanAVGByDay, 0), 0) END AS TimeReOrderPoint,
    CASE WHEN R.QuanAVGByDay = 0 OR ABS((R.TotalQuanStock - R.ReOrderPoint) / NULLIF(R.QuanAVGByDay, 0)) > 10000 THEN NULL
         ELSE date_add('day', CAST(ROUND((R.TotalQuanStock - R.ReOrderPoint) / NULLIF(R.QuanAVGByDay, 0), 0) AS BIGINT), d.ViewDate) END AS TimeOrder,
    
    R.CostPrice, R.CostQuantityTQ, 
    R.CostPrice * CASE WHEN (R.ReOrderPoint - R.TotalQuanStock) > 0 THEN (R.ReOrderPoint - R.TotalQuanStock) ELSE 0 END AS CostQuanNeedPurchase,
    R.CostPrice * R.MinStock AS CostMinStock, R.CostPrice * R.MaxStock AS CostMaxStock,
    R.NumDayOfManufacture, R.NumDayOfRoad, R.LeadTime, R.SeviceLevel, R.SafetyFactor,

    CASE WHEN R.QuantityTQ < R.MinStock THEN 'Chạm' ELSE '' END AS WarningMinStock,
    CASE WHEN R.QuantityTQ > R.MaxStock THEN 'Vượt' ELSE '' END AS WarningMaxStock,
    CASE WHEN R.QuantityTQ < R.MinStock THEN R.MinStock - R.QuantityTQ ELSE 0 END AS QuanLessThanMinStock,
    CASE WHEN R.QuantityTQ > R.MaxStock THEN R.QuantityTQ - R.MaxStock ELSE 0 END AS QuanBiggerThanMaxStock,
    CASE WHEN R.TotalQuanStock < R.SumMinStock THEN 'Chạm' ELSE '' END AS WarningMinStockUserPO,
    CASE WHEN R.TotalQuanStock > R.SumMaxStock THEN 'Vượt' ELSE '' END AS WarningMaxStockUserPO,
    CASE WHEN R.TotalQuanStock < R.SumMinStock THEN R.SumMinStock - R.TotalQuanStock ELSE 0 END AS QuanMinStockUserPO,
    CASE WHEN R.TotalQuanStock > R.SumMaxStock THEN R.TotalQuanStock - R.SumMaxStock ELSE 0 END AS QuanMaxStockUserPO,

    (CASE WHEN R.QuanAVGTHLKByDay = 0 THEN 0 ELSE ROUND((R.MAD * R.SafetyFactor)/R.QuanAVGTHLKByDay, 0) END) AS NumDaySafetyStock,
    (CASE WHEN R.QuanAVGTHLKByDay = 0 THEN 0 ELSE ROUND(R.MinStock/R.QuanAVGTHLKByDay, 0) END) AS NumDayMinStock,
    (CASE WHEN R.QuanAVGTHLKByDay = 0 THEN 0 ELSE ROUND(R.MaxStock/R.QuanAVGTHLKByDay, 0) END) AS NumDayMaxStock,
    (CASE WHEN R.QuanAVGTHLKByDay = 0 THEN 0 ELSE ROUND(R.SumMinStock/R.QuanAVGTHLKByDay, 0) END) AS NumDaySumMinStock,
    (CASE WHEN R.QuanAVGTHLKByDay = 0 THEN 0 ELSE ROUND(R.SumMaxStock/R.QuanAVGTHLKByDay, 0) END) AS NumDaySumMaxStock,

    CASE 
        WHEN R.THLKT1 = 0 OR R.THLKT2 = 0 OR R.THLKT3 = 0 OR R.THLKT4 = 0 THEN ''
        WHEN ((R.THLKT1 / NULLIF(R.THLKT2, 0)) >= 1.15 AND (R.THLKT2 / NULLIF(R.THLKT3, 0)) >= 1.15 AND (R.THLKT3 / NULLIF(R.THLKT4, 0)) >= 1.15)
          AND ((R.THLKT1 + R.THLKT2 + R.THLKT3) / 3.0 >= R.BQNTHLK) 
          AND ((p.THLKO_T1 / NULLIF(p.THLKO_T2, 0)) >= 1.10 AND (p.THLKO_T2 / NULLIF(p.THLKO_T3, 0)) >= 1.10 AND (p.THLKO_T3 / NULLIF(p.THLKO_T4, 0)) >= 1.10) THEN 'UpTrend' 
        WHEN ((R.THLKT1 / NULLIF(R.THLKT2, 0)) <= 0.85 AND (R.THLKT2 / NULLIF(R.THLKT3, 0)) <= 0.85 AND (R.THLKT3 / NULLIF(R.THLKT4, 0)) <= 0.85)
          AND ((R.THLKT1 + R.THLKT2 + R.THLKT3) / 3.0 <= R.BQNTHLK)
          AND ((p.THLKO_T1 / NULLIF(p.THLKO_T2, 0)) <= 0.8 AND (p.THLKO_T2 / NULLIF(p.THLKO_T3, 0)) <= 0.8 AND (p.THLKO_T3 / NULLIF(p.THLKO_T4, 0)) <= 0.8) THEN 'DownTrend' 
        ELSE ''
    END AS TrendItem,

    0 AS QtyToBeReceived,
    i.UserField_09

FROM ResultData R
CROSS JOIN DateParams d 
LEFT JOIN {{ source('dp_warehouse_staging', 'stg_exact101__items') }} i ON R.ItemCode = i.ItemCode
LEFT JOIN {{ source('dp_warehouse_staging', 'stg_exact101__ItemClasses') }} ic ON ic.ItemClassCode = R.ItemClassCode AND ic.ClassID = 1
LEFT JOIN LATERAL (
    SELECT S.THLKO_T1, S.THLKO_T2, S.THLKO_T3, S.THLKO_T4 
    FROM SaleoutQuantityData S 
    WHERE S.ItemClassCode = R.ItemClassCode
) p ON TRUE
WHERE 
    NOT (R.UserYesNo_05 = 'OFF' AND R.TotalQuanStock = 0)
ORDER BY 
    R.IndustryCode, R.ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}