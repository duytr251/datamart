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
    -- 0. Define Sources
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__Items') }}),
    orkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }}),
    orsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }}),
    cicmpy_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__cicmpy') }}),
    SHBaseSaleFullfilResult_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHBaseSaleFullfilResult') }}),
    MPLmagaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPLmagaz') }}),
    gbkmut_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }}),
    grtbk_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__grtbk') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHCostcenter') }}),
    ItemAssortment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__ItemAssortment') }}),
    SHIndustry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHIndustry') }}),
    Transaction_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__Transaction') }}),
    TransactionItem_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__TransactionItem') }}),
    WarrantyProcess_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyProcess') }}),
    WarrantyTErrorGroup_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyTErrorGroup') }}),
    WarrantyCenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyCenter') }}),
    WarrantyOnsiteItem_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyOnsiteItem') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MDataItems') }}),
    Customers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__Customers') }}),
    WarrantyRMAProcess_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyRMAProcess') }}),
    ProPlanItemSaleUpload_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__ProPlanItemSaleUpload') }}),
    WarrantyItemClassQuota_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WarrantyItemClassQuota') }}),
    SHRequestExchangeWarranty_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHRequestExchangeWarranty') }}),
    SHRequestExchangeWarrantyDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHRequestExchangeWarrantyDetail') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__ItemClasses') }}),


    -- 1. CTE: Tương đương với biến bảng @tblItems (Lấy nhà cung cấp cuối cùng)
    tblItems AS (
        SELECT
            I.ItemCode,
            J.crdcode,
            J.cmp_name
        FROM Items_Source I
        LEFT JOIN LATERAL (
            SELECT
                C.crdcode,
                C.cmp_name
            FROM orkrg_Source k
            JOIN orsrg_Source s ON s.ordernr = k.ordernr
            JOIN cicmpy_Source c ON c.crdnr = k.crdnr
            WHERE k.ord_soort = 'B'
            AND s.artcode = I.ItemCode
            ORDER BY k.syscreated DESC
            LIMIT 1
        ) J ON TRUE
    ),

    -- 2. CTE: Dữ liệu Bán hàng (SalesData)
    SalesData AS (
        SELECT
            R.ItemCode,
            SUM(R.LYSaleQtyAClass) AS LYSaleQtyAClass, SUM(R.LYSaleQtyBClass) AS LYSaleQtyBClass, SUM(R.LYSaleQuantity) AS LYSaleQuantity,
            SUM(R.SaleQtyAClass) AS SaleQtyAClass, SUM(R.SaleQtyBClass) AS SaleQtyBClass, SUM(R.SaleQuantity) AS SaleQuantity,
            SUM(R.GT_SaleQuantity) AS GT_SaleQuantity, SUM(R.GT_SaleQuantity_SUP) AS GT_SaleQuantity_SUP, SUM(R.GT_SaleQuantity_NoSUP) AS GT_SaleQuantity_NoSUP
        FROM (
            -- 2.1: LY Sales
            SELECT R.ItemCode,
                SUM(CASE WHEN W.int_regio IN ('A','D') THEN R.SaleQuantity ELSE 0 END) LYSaleQtyAClass,
                SUM(CASE WHEN W.int_regio = 'B' THEN R.SaleQuantity ELSE 0 END) LYSaleQtyBClass,
                SUM(R.SaleQuantity) AS LYSaleQuantity,
                0 AS SaleQtyAClass, 0 AS SaleQtyBClass, 0 AS SaleQuantity,
                0 AS GT_SaleQuantity, 0 AS GT_SaleQuantity_SUP, 0 AS GT_SaleQuantity_NoSUP
            FROM SHBaseSaleFullfilResult_Source R
            JOIN MPLmagaz_Source W ON W.Division = R.Division AND W.magcode = R.magcode
            WHERE COALESCE(W.int_regio,'') IN ('A','B','D')
            AND R.SaleChannel IS NOT NULL
            -- Lọc Incremental cho LY Sales (Giả sử cần 1 năm trước ngày ETL)
            -- AND R.SaleDate BETWEEN DATE_ADD('year', -1, CAST('{{ var("etl_date") }}' AS DATE)) AND DATE_ADD('year', -1, CAST('{{ var("etl_date") }}' AS DATE))
            GROUP BY R.ItemCode

            UNION ALL

            -- 2.2: Current Sales
            SELECT R.ItemCode,
                0 AS LYSaleQtyAClass, 0 AS LYSaleQtyBClass, 0 AS LYSaleQuantity,
                SUM(CASE WHEN W.int_regio IN ('A','D') THEN ROUND(R.SaleQuantity,0) ELSE 0 END) SaleQtyAClass,
                SUM(CASE WHEN W.int_regio = 'B' THEN ROUND(R.SaleQuantity,0) ELSE 0 END) SaleQtyBClass,
                0 AS SaleQuantity,
                SUM(CASE WHEN R.SaleChannel = 'GT' THEN ROUND(R.SaleQuantity,0) ELSE 0 END) AS GT_SaleQuantity,
                SUM(CASE WHEN R.SaleChannel = 'GT' AND ( (R.intCmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND R.IndustryCode = 'TBNB') OR (R.intCmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND R.IndustryCode = 'DGD') ) THEN ROUND(R.SaleQuantity,0) ELSE 0 END) AS GT_SaleQuantity_SUP,
                SUM(CASE WHEN R.SaleChannel = 'GT' THEN ROUND(R.SaleQuantity,0) ELSE 0 END) - SUM(CASE WHEN R.SaleChannel = 'GT' AND ( (R.intCmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND R.IndustryCode = 'TBNB') OR (R.intCmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND R.IndustryCode = 'DGD') ) THEN ROUND(R.SaleQuantity,0) ELSE 0 END) AS GT_SaleQuantity_NoSUP
            FROM SHBaseSaleFullfilResult_Source R
            JOIN MPLmagaz_Source W ON W.Division = R.Division AND (W.magcode = R.magcode OR R.magcode =W.magcodeOld)
            WHERE COALESCE(W.int_regio,'') IN ('A','B','D')
            AND R.SaleChannel IS NOT NULL
            -- Lọc Incremental (Chỉ lấy dữ liệu của ngày ETL)
            {% if is_incremental() %}
                AND R.SaleDate = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
            GROUP BY R.ItemCode

            UNION ALL

            -- 2.3: RMA Sales
            SELECT g.artcode AS ItemCode,
                0 AS LYSaleQtyAClass, 0 AS LYSaleQtyBClass, 0 AS LYSaleQuantity, 0 AS SaleQtyAClass, 0 AS SaleQtyBClass,
                SUM(CASE WHEN g.transtype = 'B' AND g.freefield1 NOT IN ('K', 'D', 'O') THEN NULL ELSE
                    CASE WHEN gr.omzrek = 'J' AND g.aantal < 0 THEN - g.aantal ELSE CASE WHEN gr.omzrek IN ('G', 'K', 'N') AND g.aantal > 0 THEN g.aantal END END END) AS SaleQuantity,
                0 AS GT_SaleQuantity, 0 AS GT_SaleQuantity_SUP, 0 AS GT_SaleQuantity_NoSUP
            FROM gbkmut_Source g
            JOIN grtbk_Source gr ON g.reknr = gr.reknr
            JOIN SHCostcenter_Source sc ON sc.CostCenter = TRIM(g.kstplcode)
            JOIN Items_Source i ON g.artcode = i.itemcode
            JOIN ItemAssortment_Source ia ON i.Assortment = ia.Assortment
            JOIN SHIndustry_Source si ON i.Assortment BETWEEN si.ItemGroupMin AND si.ItemGroupMax
            LEFT JOIN cicmpy_Source c ON g.debnr = c.debnr AND g.debnr IS NOT NULL AND c.debnr IS NOT NULL
            WHERE g.reknr = COALESCE(i.glaccountdistribution, ia.GLStock)
            AND NOT ( g.aantal = 0 AND g.bdr_hfl = 0 ) AND g.transtype IN ('N', 'C', 'P', 'X') AND g.remindercount <= 10 AND g.transsubtype ='H'
            AND g.warehouse IN ('BN08','TNMT','NRML')
            -- Lọc Incremental
            {% if is_incremental() %}
                AND g.datum = CAST('{{ var("etl_date") }}' AS DATE) -- Giả định cột ngày giao dịch là g.datum
            {% endif %}
            GROUP BY g.artcode
        ) R
        GROUP BY R.ItemCode
    ),

    -- 3. CTE: Dữ liệu Bảo hành (WarrantyData)
    WarrantyData AS (
        SELECT
            R.ItemCode,
            SUM(R.LYQty_Received) AS LYQty_Received, SUM(R.LYQty_ReceivedTechnical) AS LYQty_ReceivedTechnical,
            SUM(R.Qty_Received) AS Qty_Received, SUM(R.Qty_ReceivedTechnical) AS Qty_ReceivedTechnical,
            SUM(R.MT_ReceivedTechnical) AS MT_ReceivedTechnical, SUM(R.GT_ReceivedTechnical) AS GT_ReceivedTechnical, SUM(R.GT_ReceivedTechnical_SUP) AS GT_ReceivedTechnical_SUP, SUM(R.GT_ReceivedTechnical_NoSUP) AS GT_ReceivedTechnical_NoSUP,
            SUM(R.DMX_ReceivedTechnical) AS DMX_ReceivedTechnical, SUM(R.DMX_ReturnQuantity) AS DMX_ReturnQuantity
        FROM (
            -- 3.1 & 3.3: WM (LY + Current)
            SELECT D.ItemCode,
                SUM(CASE WHEN R.IsLY = TRUE THEN D.FulfillQuantity ELSE 0 END) AS LYQty_Received,
                SUM(CASE WHEN R.IsLY = TRUE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE THEN D.FulfillQuantity ELSE 0 END ) AS LYQty_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE THEN D.FulfillQuantity ELSE 0 END) AS Qty_Received,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' THEN D.FulfillQuantity ELSE 0 END ) AS Qty_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') <> '' THEN D.FulfillQuantity ELSE 0 END ) AS MT_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') = '' THEN D.FulfillQuantity ELSE 0 END ) AS GT_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') = '' AND ( (K.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (K.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN D.FulfillQuantity ELSE 0 END ) AS GT_ReceivedTechnical_SUP,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') = '' THEN D.FulfillQuantity ELSE 0 END ) - SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') = '' AND ( (K.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (K.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN D.FulfillQuantity ELSE 0 END ) AS GT_ReceivedTechnical_NoSUP,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND D.IsWarranty = TRUE AND COALESCE(GG.TechnicalSolution,'') <> 'Advisory' AND COALESCE(D.ChainCode,'') = 'DMX' THEN D.FulfillQuantity ELSE 0 END ) AS DMX_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(GG.CurrentStatus,'') = 'OK' AND COALESCE(GG.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND COALESCE(D.ChainCode,'') = 'DMX' AND TRIM(COALESCE(GG.TechnicalError, '')) = 'THẨM ĐỊNH LỖI' THEN D.FulfillQuantity ELSE 0 END ) AS DMX_ReturnQuantity
            FROM (
                SELECT *, 
                       CASE WHEN M.TransDate < DATE_TRUNC('year', CAST('{{ var("etl_date") }}' AS DATE)) THEN TRUE ELSE FALSE END AS IsLY 
                FROM Transaction_Source M 
                -- Lọc Incremental (Master TransDate)
                {% if is_incremental() %}
                    WHERE M.TransDate = CAST('{{ var("etl_date") }}' AS DATE) OR M.TransDate = DATE_ADD('year', -1, CAST('{{ var("etl_date") }}' AS DATE))
                {% endif %}
            ) M
            INNER JOIN TransactionItem_Source D ON M.TransactionId = D.TransactionId
            LEFT JOIN (
                SELECT WP.TransactionItemId, WP.TEGID, WP.CurrentStatus, WP.TechnicalSolution, WP.TechnicalError
                FROM WarrantyProcess_Source WP
                JOIN (
                    SELECT WS.TransactionItemId, MAX(EndProcess) AS EndProcess
                    FROM WarrantyProcess_Source WS
                    GROUP BY WS.TransactionItemId
                ) G ON WP.EndProcess = G.EndProcess AND WP.TransactionItemId = G.TransactionItemId
            ) GG ON D.TransactionItemId = GG.TransactionItemId
            LEFT JOIN WarrantyCenter_Source C ON M.CenterCode=C.CenterCode
            LEFT JOIN SHCostcenter_Source CC ON D.CostCenter=CC.CostCenter
            JOIN MDataItems_Source I ON I.ItemCode = D.ItemCode
            JOIN Customers_Source K ON K.CustomerCode = M.CustomerCode
            WHERE COALESCE(M.TransType,'') = 'WRF'
            GROUP BY D.ItemCode, R.IsLY
            
            UNION ALL

            -- 3.2 & 3.4: Onsite (LY + Current)
            SELECT WOI.ItemCode,
                SUM(CASE WHEN R.IsLY = TRUE AND COALESCE(WOI.CurrentStatus,'') <> 'CANCEL' AND COALESCE(WOI.TEGID,'') <> '7FE4CC23-46EB-4F62-90AE-B97A669909BA' THEN WOI.QuantityReceived ELSE 0 END) AS LYQty_Received,
                SUM(CASE WHEN R.IsLY = TRUE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) THEN WOI.QuantityReceived ELSE 0 END ) AS LYQty_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.TEGID,'') <> '7FE4CC23-46EB-4F62-90AE-B97A669909BA' THEN WOI.QuantityReceived ELSE 0 END) AS Qty_Received,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' THEN WOI.QuantityReceived ELSE 0 END ) AS Qty_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') <> '' THEN WOI.QuantityReceived ELSE 0 END ) AS MT_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') = '' THEN WOI.QuantityReceived ELSE 0 END ) AS GT_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') = '' AND ( (K.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (K.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN WOI.QuantityReceived ELSE 0 END ) AS GT_ReceivedTechnical_SUP,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') = '' THEN WOI.QuantityReceived ELSE 0 END ) - SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') = '' AND ( (K.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (K.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN WOI.QuantityReceived ELSE 0 END ) AS GT_ReceivedTechnical_NoSUP,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND WOI.IsValid = TRUE AND ( (COALESCE(WOI.TechnicalSolution,'') NOT IN ('RatingClassify','ChangeNew','Advisory') AND COALESCE(I.Class_01,'') <> 'RO1') OR COALESCE(I.Class_01,'') = 'RO1' ) AND COALESCE(WOI.WarrantyType,'') <> 'Services' AND COALESCE(WOI.ChainCode,'') = 'DMX' THEN WOI.QuantityReceived ELSE 0 END ) AS DMX_ReceivedTechnical,
                SUM(CASE WHEN R.IsLY = FALSE AND COALESCE(WOI.CurrentStatus,'') = 'OK' AND COALESCE(WOI.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5' AND COALESCE(WOI.TechnicalSolution,'') = 'RatingClassify' AND COALESCE(WOI.ChainCode,'') = 'DMX' AND COALESCE(WOI.TechnicalSolution,'') IN ('RatingClassify','Advisory') AND COALESCE(WOI.WarrantyType,'') <> 'Services' THEN WOI.QuantityReceived ELSE 0 END ) AS DMX_ReturnQuantity
            FROM (
                SELECT *, 
                       CASE WHEN WOI.DateReceived < DATE_TRUNC('year', CAST('{{ var("etl_date") }}' AS DATE)) THEN TRUE ELSE FALSE END AS IsLY 
                FROM WarrantyOnsiteItem_Source WOI
                WHERE COALESCE(WOI.CurrentStatus,'') NOT IN ('CANCEL') AND COALESCE(WOI.DataBy,'') <>'IMPORT'
                -- Lọc Incremental (Master DateReceived)
                {% if is_incremental() %}
                    AND WOI.DateReceived = CAST('{{ var("etl_date") }}' AS DATE) OR WOI.DateReceived = DATE_ADD('year', -1, CAST('{{ var("etl_date") }}' AS DATE))
                {% endif %}
            ) R
            LEFT JOIN WarrantyCenter_Source C ON R.WDCCode=C.CenterCode
            LEFT JOIN SHCostcenter_Source cc ON R.CostCenter = cc.CostCenter
            JOIN MDataItems_Source I ON I.ItemCode = R.ItemCode
            JOIN Customers_Source K ON K.CustomerCode = R.CustomerCode
            GROUP BY R.ItemCode, R.IsLY

            UNION ALL

            -- 3.5: Exchange (Current)
            SELECT D.ItemCode,
                0 AS LYQty_Received, 0 AS LYQty_ReceivedTechnical,
                SUM(D.RMAQuantity) AS Qty_Received,
                SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' THEN D.RMAQuantity ELSE 0 END) AS Qty_ReceivedTechnical,
                SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' AND COALESCE(C.ChannelCode,'') = 'MT' THEN D.RMAQuantity ELSE 0 END) AS MT_ReceivedTechnical,
                SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' AND COALESCE(C.ChannelCode,'') = 'GT' THEN D.RMAQuantity ELSE 0 END) AS GT_ReceivedTechnical,
                SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' AND COALESCE(C.ChannelCode,'') = 'GT' AND ( (M.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (M.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN D.RMAQuantity ELSE 0 END) AS GT_ReceivedTechnical_SUP,
                SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' AND COALESCE(C.ChannelCode,'') = 'GT' THEN D.RMAQuantity ELSE 0 END) - SUM(CASE WHEN COALESCE(D.DefectType,'') = 'REW_DEFECT_KTV' AND COALESCE(C.ChannelCode,'') = 'GT' AND ( (M.cmp_wwn IN ('767D858B-A46A-40BF-A9C5-C62DD1529D81','F8543FDA-2D42-4139-99D7-F9A08A8F063D') AND I.IndustryCode = 'TBNB') OR (M.cmp_wwn IN ('64B14351-68AC-40BF-846A-D01BBD2147C0','F83BC672-CB93-4B9F-A45C-7CE369FBE68A','6A3DC580-0018-477C-A29B-386BBBFE2EDF','1EDB3A31-292B-4141-91D8-185FF3C4996E','DCC0EFB8-3BB4-46D7-A050-15C9DBCD333C','ECA0CBAF-F68B-4E9F-A0AB-F3962B1762F1','AAC8321E-3C14-44FC-953C-012D871B77A6','48D040EF-78BA-4AE1-A80C-59912ECA2470','46F21A1D-39E0-45B8-85F2-5830A64AC022','270F80C9-F1B4-49E8-B44B-957DA44A4E92','FB68C99B-6A56-45ED-A2AC-834EFD875048','54E8CB7B-64AC-41E2-9871-E7B84AA3DF05','6B4DAA66-3615-450C-90E5-36897E6EE41C','96BC0098-6440-4C28-93C8-19DCF450A0F0','C8C5BCEF-4F4A-433E-AFF1-2F4C1FAEF98D','E1BA39FD-3D07-42C8-87B8-14720D3E54FE','5201271B-0B92-4C9F-B0FE-0E2ED57ADE7F') AND I.IndustryCode = 'DGD') ) THEN D.RMAQuantity ELSE 0 END) AS GT_ReceivedTechnical_NoSUP,
                0 AS DMX_ReceivedTechnical, 0 AS DMX_ReturnQuantity
            FROM SHRequestExchangeWarranty_Source M
            JOIN SHRequestExchangeWarrantyDetail_Source D ON M.Id = D.MasterId
            JOIN SHCostcenter_Source C ON M.RequestCostCenter = C.CostCenter
            JOIN MDataItems_Source I ON I.ItemCode = D.ItemCode
            -- Lọc Incremental (Master CreateDate)
            {% if is_incremental() %}
                WHERE M.CreateDate = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
            GROUP BY D.ItemCode
        ) R
        GROUP BY R.ItemCode
    ),

    -- 4. CTE: Gộp dữ liệu Sales và Warranty
    AggregatedData AS (
        SELECT
            ItemCode,
            SUM(LYSaleQtyAClass) AS LYSaleQtyAClass, SUM(LYSaleQtyBClass) AS LYSaleQtyBClass, SUM(LYSaleQuantity) AS LYSaleQuantity,
            SUM(SaleQtyAClass) AS SaleQtyAClass, SUM(SaleQtyBClass) AS SaleQtyBClass, SUM(SaleQuantity) AS SaleQuantity,
            SUM(GT_SaleQuantity) AS GT_SaleQuantity, SUM(GT_SaleQuantity_SUP) AS GT_SaleQuantity_SUP, SUM(GT_SaleQuantity_NoSUP) AS GT_SaleQuantity_NoSUP,
            SUM(LYQty_Received) AS LYQty_Received, SUM(LYQty_ReceivedTechnical) AS LYQty_ReceivedTechnical,
            SUM(Qty_Received) AS Qty_Received, SUM(Qty_ReceivedTechnical) AS Qty_ReceivedTechnical,
            SUM(MT_ReceivedTechnical) AS MT_ReceivedTechnical, SUM(GT_ReceivedTechnical) AS GT_ReceivedTechnical, SUM(GT_ReceivedTechnical_SUP) AS GT_ReceivedTechnical_SUP, SUM(GT_ReceivedTechnical_NoSUP) AS GT_ReceivedTechnical_NoSUP,
            SUM(DMX_ReceivedTechnical) AS DMX_ReceivedTechnical, SUM(DMX_ReturnQuantity) AS DMX_ReturnQuantity
        FROM (
            SELECT ItemCode, LYSaleQtyAClass, LYSaleQtyBClass, LYSaleQuantity, SaleQtyAClass, SaleQtyBClass, SaleQuantity, GT_SaleQuantity, GT_SaleQuantity_SUP, GT_SaleQuantity_NoSUP,
                   0 AS LYQty_Received, 0 AS LYQty_ReceivedTechnical, 0 AS Qty_Received, 0 AS Qty_ReceivedTechnical, 0 AS MT_ReceivedTechnical, 0 AS GT_ReceivedTechnical, 0 AS GT_ReceivedTechnical_SUP, 0 AS GT_ReceivedTechnical_NoSUP, 0 AS DMX_ReceivedTechnical, 0 AS DMX_ReturnQuantity
            FROM SalesData
            UNION ALL
            SELECT ItemCode, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   LYQty_Received, LYQty_ReceivedTechnical, Qty_Received, Qty_ReceivedTechnical, MT_ReceivedTechnical, GT_ReceivedTechnical, GT_ReceivedTechnical_SUP, GT_ReceivedTechnical_NoSUP, DMX_ReceivedTechnical, DMX_ReturnQuantity
            FROM WarrantyData
        ) K
        GROUP BY ItemCode
    ),

    -- 5. CTEs cho các LEFT JOIN phụ
    RMA AS (
        SELECT p.ItemCode, SUM(p.QuantityReceived) AS QuantityReceived
        FROM WarrantyRMAProcess_Source P
        JOIN WarrantyTErrorGroup_Source G ON G.TEGID = P.TEGID
        JOIN SHCostcenter_Source C ON P.Costcenter = C.CostCenter
        WHERE COALESCE(G.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5'
        GROUP BY p.ItemCode
    ),
    DMX AS (
        SELECT B.ItemCode, SUM(B.SaleQuantity) AS SaleQuantity
        FROM SHBaseSaleFullfilResult_Source B
        JOIN MPLmagaz_Source W ON W.Division = B.Division AND W.magcode = B.magcode
        WHERE COALESCE(B.ChainCode,'') = 'DMX'
        AND COALESCE(W.int_regio,'') IN ('A','B','D')
        GROUP BY B.ItemCode
    ),
    RMADMX AS (
        SELECT p.ItemCode, SUM(p.QuantityReceived) AS QuantityReceived
        FROM WarrantyRMAProcess_Source P
        JOIN WarrantyTErrorGroup_Source G ON G.TEGID = P.TEGID
        WHERE P.Division = 223
        AND COALESCE(G.TEGID,'') = 'eb8c8f52-47b9-4e3b-b9cf-73affe1957f5'
        GROUP BY p.ItemCode
    ),
    MO AS (
        SELECT P.ItemCode, SUM(CAST(P.SaleQuantity AS INTEGER)) AS MT_SaleOutQuantity
        FROM ProPlanItemSaleUpload_Source P
        JOIN SHCostcenter_Source sc ON sc.CostCenter = P.Costcenter AND SC.ChannelCode ='MT'
        WHERE CAST(P.SaleQuantity AS INTEGER) <> 0
        GROUP BY P.ItemCode
    ),

    -- **CTE cho QuotaData**
    QuotaData AS (
        SELECT
            Q.ItemClassCode,
            Q.Class_03,
            Q.TargetPercent,
            Q.ImprovementPercent,
            Q.BarePercent,
            ROW_NUMBER() OVER (PARTITION BY Q.ItemClassCode, Q.Class_03 ORDER BY Q.CreateDate DESC) AS rn
        FROM WarrantyItemClassQuota_Source Q
    )


-- 6. FINAL SELECT
SELECT
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    A.ItemCode, I.ItemName, IC.ItemClassCode, IC.Description AS ItemClassName, I.IndustryCode, SI.IndustryName,
    I.Class_06 AS Segment,
    CASE WHEN I.UserYesNo_05 = TRUE THEN 'OFF' ELSE I.UserField_06 END AS ItemStatus,
    I.Class_07 AS SourceItem,
    A.LYSaleQtyAClass, A.LYSaleQtyBClass, A.LYSaleQuantity,
    A.LYQty_Received, A.LYQty_ReceivedTechnical,
    A.SaleQtyAClass, A.SaleQtyBClass, A.SaleQuantity,
    A.Qty_Received, A.Qty_ReceivedTechnical,
    F.crdcode, F.cmp_name,
    COALESCE(RMA.QuantityReceived,0) AS RMAQuantityReceived,
    COALESCE(DMX.SaleQuantity,0) AS DMXSaleQuantity,
    COALESCE(RMADMX.QuantityReceived,0) AS RMADMXQuantityReceived,
    COALESCE(QT.TargetPercent,0) AS QuotaWarrantyTarget,
    COALESCE(QT.ImprovementPercent,0) AS QuotaWarrantyImprove,
    COALESCE(QT.BarePercent,0) AS QuotaWarrantyMax,
    A.MT_ReceivedTechnical,
    A.GT_ReceivedTechnical,
    A.GT_ReceivedTechnical_SUP,
    A.GT_ReceivedTechnical_NoSUP,
    A.GT_SaleQuantity,
    A.GT_SaleQuantity_SUP,
    A.GT_SaleQuantity_NoSUP,
    COALESCE(MO.MT_SaleOutQuantity, 0) AS MT_SaleOutQuantity,
    A.DMX_ReceivedTechnical,
    A.DMX_ReturnQuantity
FROM AggregatedData A
JOIN MDataItems_Source I ON I.ItemCode = A.ItemCode
JOIN ItemClasses_Source IC ON I.Class_01 = IC.ItemClassCode AND IC.ClassID = 1
JOIN SHIndustry_Source SI ON SI.IndustryCode = I.IndustryCode
LEFT JOIN RMA ON RMA.ItemCode = A.ItemCode
LEFT JOIN DMX ON DMX.ItemCode = A.ItemCode
LEFT JOIN RMADMX ON RMADMX.ItemCode = A.ItemCode
LEFT JOIN tblItems F ON F.ItemCode = A.ItemCode
LEFT JOIN MO ON MO.ItemCode = A.ItemCode
LEFT JOIN QuotaData QT
    ON QT.ItemClassCode = IC.ItemClassCode
    AND (I.Class_03 = QT.Class_03 OR COALESCE(QT.Class_03,'') ='')
    AND QT.rn = 1
WHERE
    I.IndustryCode <> 'DCN'
    AND I.UserYesNo_05 = FALSE
ORDER BY
    I.IndustryCode, I.Class_01, I.ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}