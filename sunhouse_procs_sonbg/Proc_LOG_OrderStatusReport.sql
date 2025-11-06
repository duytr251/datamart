{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key: Ngày ETL, Số Order, Mã Item, và Số Request (để phân biệt trạng thái)
        unique_key=['data_date', 'order_number', 'itemcode', 'requestnumber'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    PrepareLoadGoods_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPL_PrepareLoadGoods') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),
    PrepareLoadGoodsDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPL_PrepareLoadGoodsDetail') }}),
    Packaging_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPL_Packaging') }}),
    SHECOMApiRawDataDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHECOMApiRawDataDetail') }}),
    SHECOMShopeeHandOver_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHECOMShopeeHandOver') }}),
    SHECOMShopeeHandOverDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHECOMShopeeHandOverDetail') }}),
    SHECOMApiRawDataStatusHistory_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHECOMApiRawDataStatusHistory') }}),
    MPL_PackagingDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPL_PackagingDetail') }}),
    SH_ECOM_Combo_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SH_ECOM_Combo') }}),
    SH_ECOM_ComboItem_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SH_ECOM_ComboItem') }}),
    SHECOMApiRawData_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHECOMApiRawData') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MDataItems') }}),


    -- 1. CTE MỚI: Dữ liệu Phiếu gom (Pre-join để đơn giản hóa Nhánh 2 & 3)
    PrepareLoadGoodsInfo AS (
        SELECT 
            g.PLGNumber, u.FullName, g.EndTime, gd.EcommerceSite, gd.PLGDetailId, gd.OrderNumber, g.WarehouseLocation
        FROM PrepareLoadGoods_Source g
        JOIN AppUsers_Source u ON g.ProcessBy = u.UserName 
        JOIN PrepareLoadGoodsDetail_Source gd ON g.Id = gd.MasterId
        WHERE g.Active = TRUE AND gd.Active = TRUE
    ),

    -- 2. CTE MỚI: Dữ liệu Đóng gói (Pre-join để đơn giản hóa Nhánh 3)
    PackagingInfo AS (
        SELECT u.FullName, g.EndTime, g.OrderNumber
        FROM Packaging_Source g
        JOIN AppUsers_Source u ON g.PackagingBy = u.UserName 
        WHERE g.Active = TRUE
    ),

    -- 3. CTE: Gộp 3 nhánh vào ListOrder (Áp dụng lọc Incremental ở đây)
    ListOrder AS (
        -- Nhánh 1: (status = 'New', 'Processing', 'End')
        SELECT 
            m.PLGNumber AS RequestNumber, m.PLGStatus AS Status, d.OrderNumber AS order_number, u.FullName AS CollectionFullName, m.EndTime AS CollectionTime,
            au.FullName as PackagingByFullName, CAST(NULL AS TIMESTAMP) AS PackagingTime, '' as HOByFullName, CAST(NULL AS TIMESTAMP) AS HODate, m.Note AS Note,
            m.WarehouseLocation, d.EcommerceSite, m.ProcessBy AS UserName_Cover, m.CreateDate AS EventDate
        FROM PrepareLoadGoods_Source m
        JOIN AppUsers_Source u ON m.ProcessBy = u.UserName 
        JOIN PrepareLoadGoodsDetail_Source d ON m.Id = d.MasterId
        JOIN AppUsers_Source au ON d.PackagingBy = au.UserName
        WHERE m.Active = TRUE AND d.Active = TRUE
            {% if is_incremental() %}
                AND m.CreateDate >= CAST('{{ var("etl_date") }}' AS DATE) -- Lọc theo ngày tạo
            {% endif %}

        UNION ALL

        -- Nhánh 2: (status = 'PackingDone')
        SELECT 
            g.PLGNumber AS RequestNumber, 'PackingDone' AS Status, k.OrderNumber AS order_number, g.FullName AS CollectionFullName, g.EndTime AS CollectionTime,
            u.FullName AS PackagingByFullName, k.EndTime AS PackagingTime, '' as HOByFullName, CAST(NULL AS TIMESTAMP) AS HODate, k.Note,
            g.WarehouseLocation, g.EcommerceSite, k.PackagingBy AS UserName_Cover, k.EndTime AS EventDate
        FROM Packaging_Source k
        JOIN AppUsers_Source u ON k.PackagingBy = u.UserName 
        JOIN SHECOMApiRawDataDetail_Source rd ON k.OrderNumber = rd.order_number
        LEFT JOIN PrepareLoadGoodsInfo g ON g.PLGDetailId = k.PLGDetailId
        WHERE k.P_Status = 'End' AND k.Active = TRUE
            {% if is_incremental() %}
                AND k.EndTime >= CAST('{{ var("etl_date") }}' AS DATE) -- Lọc theo ngày hoàn thành đóng gói
            {% endif %}
        
        UNION ALL

        -- Nhánh 3: (status = 'OUT')
        SELECT 
            m.HONumber AS RequestNumber, 'OUT' AS Status, d.ordersn AS order_number,
            g.FullName AS CollectionFullName, g.EndTime AS CollectionTime, p.FullName AS PackagingByFullName, p.EndTime AS PackagingTime,
            u.FullName AS HOByFullName, m.HODate, m.HONote AS Note, CAST(NULL AS VARCHAR) AS WarehouseLocation, 
            m.EcommerceSite, m.HOBy AS UserName_Cover, m.HODate AS EventDate
        FROM SHECOMShopeeHandOver_Source m
        JOIN AppUsers_Source u ON m.HOBy = u.UserName 
        JOIN SHECOMShopeeHandOverDetail_Source d ON m.Id = d.MasterId
        LEFT JOIN PrepareLoadGoodsInfo g ON g.OrderNumber = d.ordersn
        LEFT JOIN PackagingInfo p ON p.OrderNumber = d.ordersn
        WHERE m.HOType = 'OUT'
            {% if is_incremental() %}
                AND m.HODate >= CAST('{{ var("etl_date") }}' AS DATE) -- Lọc theo ngày bàn giao
            {% endif %}
    ),

    -- 4. CTE: OrderSiteUpdateTime
    OrderSiteUpdateTime AS (
        SELECT a.ordersn AS order_number, MAX(a.SiteUpdateTime) AS SiteUpdateTime
        FROM SHECOMApiRawDataStatusHistory_Source a
        JOIN ListOrder b ON a.ordersn = b.order_number
        WHERE (a.EcommerceSite = 'Shopee' AND a.order_status IN('PROCESSED'))
           OR (a.EcommerceSite = 'Lazada' AND a.order_status IN ('packed'))
           OR (a.EcommerceSite = 'TikTok' AND a.order_status = 'AWAITING_COLLECTION')
        GROUP BY a.ordersn
    ),

    -- 5. CTE: HandoverTracking
    HandoverTracking AS (
        SELECT 
            hod.ordersn, hod.tracking_no, hom.HONumber,
            ROW_NUMBER() OVER(PARTITION BY hod.ordersn, hom.HONumber ORDER BY hom.Id DESC) AS rn
        FROM SHECOMShopeeHandOver_Source hom
        JOIN SHECOMShopeeHandOverDetail_Source hod ON hom.Id = hod.MasterId
    ),

    -- 6. CTE: PackagingData
    PackagingData AS (
        SELECT p.OrderNumber, d.ItemCode, d.QtyOrder
        FROM Packaging_Source p
        JOIN MPL_PackagingDetail_Source d ON p.Id = d.MasterId
        WHERE p.Active = TRUE
    ),

    -- 7. CTE: SiteUpdates (Từ #OrderSiteUpdateTime)
    SiteUpdates AS (
        SELECT order_number, MAX(SiteUpdateTime) AS SiteUpdateTime
        FROM OrderSiteUpdateTime
        GROUP BY order_number
    ),

    -- 8. CTE: JOIN trước dữ liệu Combo
    ComboSource AS (
        SELECT
            B1.ComboCode, B1.ValidFrom, B1.ValidTo, BD.ItemCode, BD.ItemName, BD.PriceRate, B1.ComboPrice, BD.QuantityRate
        FROM SH_ECOM_Combo_Source B1
        JOIN SH_ECOM_ComboItem_Source BD ON BD.ComboCode = B1.ComboCode AND BD.ComboId = B1.Id
        WHERE B1.Active = TRUE
    ),

    -- 9. CTE: Gộp dữ liệu chính và xếp hạng (ROW_NUMBER)
    FinalQuery AS (
        SELECT
            m.RequestNumber, rd.EcommerceSite, m.Status,
            CASE m.Status
                WHEN 'New' THEN 'Mới tạo' WHEN 'Processing' THEN 'Đang gom' WHEN 'End' THEN 'Kết thúc gom'
                WHEN 'PackingDone' THEN 'Đã đóng gói' WHEN 'OUT' THEN 'Bàn giao đi' ELSE ''
            END AS StatusName,
            
            COALESCE(p_src.ItemCode, rd.ItemCode) AS ItemCode,
            COALESCE(p_src.ItemName, i.ItemName) AS ItemName,
            
            m.order_number, m.CollectionFullName, m.CollectionTime, m.PackagingByFullName, m.PackagingTime, m.HOByFullName, m.HODate,
            st.SiteUpdateTime, COALESCE(ha.tracking_no, rd.tracking_no) AS tracking_no, COALESCE(pd.QtyOrder, rd.quantity) AS quantity,
            rd.shipping_carrier, m.Note,
            
            -- CỘT "COVER"
            m.Status AS Status_Cover, m.WarehouseLocation AS Warehouse_Cover, rd.EcommerceSite AS EcommerceSite_Cover, m.UserName_Cover AS UserName_Cover,
            CAST(m.CollectionTime AS DATE) AS Date_Cover,
            CASE 
                WHEN COALESCE(rd.warehouse_code, o.warehouse) IN (
                    'VN0157WSZ', 'VN10619-WH-10002', '7352861353030797061', '7498657948904097554', '7498657948904081170', '7509840536435902225'
                ) THEN 'NECL' 
                ELSE 'BECO' 
            END AS Warehouse_Computed_Cover,
            
            ROW_NUMBER() OVER(
                PARTITION BY m.order_number, rd.ItemCode 
                ORDER BY p_src.ValidFrom DESC NULLS LAST 
            ) as rn,
            m.EventDate -- Lấy ngày sự kiện chính để đảm bảo lọc incremental
        FROM ListOrder AS m
        JOIN SHECOMApiRawData_Source o ON o.order_number = m.order_number
        JOIN SHECOMApiRawDataDetail_Source rd ON rd.order_number = o.order_number
        LEFT JOIN ComboSource p_src
            ON rd.ItemCode = p_src.ComboCode AND o.create_time BETWEEN p_src.ValidFrom AND COALESCE(p_src.ValidTo, DATE '2299-12-31')
        LEFT JOIN PackagingData pd ON pd.OrderNumber = m.order_number AND pd.ItemCode = COALESCE(p_src.ItemCode, rd.ItemCode) 
        LEFT JOIN HandoverTracking ha ON ha.ordersn = m.order_number AND ha.HONumber = m.RequestNumber AND ha.rn = 1
        JOIN MDataItems_Source i ON i.ItemCode = rd.ItemCode
        LEFT JOIN SiteUpdates st ON st.order_number = m.order_number
    )

-- 10. SELECT cuối cùng: Lọc lấy rn = 1
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    m.RequestNumber, m.EcommerceSite, m.Status, m.StatusName, m.ItemCode, m.ItemName, m.order_number, m.CollectionFullName, m.CollectionTime,
    m.PackagingByFullName, m.PackagingTime, m.HOByFullName, m.HODate, m.SiteUpdateTime, m.tracking_no, m.quantity, m.shipping_carrier, m.Note,
    m.Status_Cover, m.Warehouse_Cover, m.EcommerceSite_Cover, m.UserName_Cover, m.Date_Cover, m.Warehouse_Computed_Cover, m.EventDate
FROM FinalQuery m
WHERE m.rn = 1
ORDER BY 
    CollectionTime, order_number, ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}