WITH
    -- 1. CTE MỚI: Dữ liệu Phiếu gom (thay thế LATERAL trong Nhánh 2 và 3)
    PrepareLoadGoodsInfo AS (
        SELECT 
            g.PLGNumber, 
            g.WarehouseLocation, 
            u.FullName, 
            g.EndTime, 
            gd.EcommerceSite,
            gd.PLGDetailId, -- Dùng để join với Nhánh 2
            gd.OrderNumber  -- Dùng để join với Nhánh 3
        FROM dp_warehouse.staging.stg_appdatashgextappdata__MPL_PrepareLoadGoods g
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers u ON g.ProcessBy = u.UserName 
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__MPL_PrepareLoadGoodsDetail gd ON g.Id = gd.MasterId
        WHERE g.Active = TRUE AND gd.Active = TRUE
    ),

    -- 2. CTE MỚI: Dữ liệu Đóng gói (thay thế LATERAL trong Nhánh 3)
    PackagingInfo AS (
        SELECT 
            u.FullName,
            g.EndTime,
            g.OrderNumber
        FROM dp_warehouse.staging.stg_appdatashgextappdata__MPL_Packaging g
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers u ON g.PackagingBy = u.UserName 
        WHERE g.Active = TRUE
    ),

    -- 3. CTE: Gộp 3 nhánh INSERT vào #ListOrder
    ListOrder AS (
        -- Nhánh 1: (status = 'New', 'Processing', 'End')
        SELECT 
            m.PLGNumber AS RequestNumber, 
            m.PLGStatus AS Status, 
            d.OrderNumber AS order_number, 
            u.FullName AS CollectionFullName, 
            m.EndTime AS CollectionTime,
            au.FullName as PackagingByFullName,
            CAST(NULL AS TIMESTAMP) AS PackagingTime, 
            '' as HOByFullName,
            CAST(NULL AS TIMESTAMP) AS HODate, 
            m.Note AS Note,
            m.WarehouseLocation,
            d.EcommerceSite,
            m.ProcessBy AS UserName_Cover
        FROM dp_warehouse.staging.stg_appdatashgextappdata__MPL_PrepareLoadGoods m
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers u ON m.ProcessBy = u.UserName 
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__MPL_PrepareLoadGoodsDetail d ON m.Id = d.MasterId
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers au ON d.PackagingBy = au.UserName
        WHERE m.Active = TRUE AND d.Active = TRUE

        UNION ALL

        -- Nhánh 2: (status = 'PackingDone')
        SELECT 
            g.PLGNumber AS RequestNumber, 
            'PackingDone' AS Status, 
            k.OrderNumber AS order_number, 
            g.FullName AS CollectionFullName, 
            g.EndTime AS CollectionTime,
            u.FullName AS PackagingByFullName,
            k.EndTime AS PackagingTime,
            '' as HOByFullName,
            CAST(NULL AS TIMESTAMP) AS HODate,
            k.Note,
            g.WarehouseLocation,
            g.EcommerceSite,
            k.PackagingBy AS UserName_Cover
        FROM dp_warehouse.staging.stg_appdatashgextappdata__MPL_Packaging k
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers u ON k.PackagingBy = u.UserName 
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SHECOMApiRawDataDetail rd ON k.OrderNumber = rd.order_number
        -- *** ĐÃ SỬA LỖI: Thay thế LATERAL JOIN bằng LEFT JOIN ***
        LEFT JOIN PrepareLoadGoodsInfo g ON g.PLGDetailId = k.PLGDetailId
        WHERE k.P_Status = 'End' 
          AND k.Active = TRUE
        
        UNION ALL

        -- Nhánh 3: (status = 'OUT')
        SELECT 
            m.HONumber AS RequestNumber, 
            'OUT' AS Status, 
            d.ordersn AS order_number,
            g.FullName AS CollectionFullName, 
            g.EndTime AS CollectionTime,
            p.FullName AS PackagingByFullName,
            p.EndTime AS PackagingTime,
            u.FullName AS HOByFullName,
            m.HODate,
            m.HONote AS Note,
            CAST(NULL AS VARCHAR) AS WarehouseLocation, 
            m.EcommerceSite,
            m.HOBy AS UserName_Cover
        FROM dp_warehouse.staging.stg_appdatashgextappdata__SHECOMShopeeHandOver m
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__AppUsers u ON m.HOBy = u.UserName 
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SHECOMShopeeHandOverDetail d ON m.Id = d.MasterId
        -- *** ĐÃ SỬA LỖI: Thay thế LATERAL JOIN bằng LEFT JOIN ***
        LEFT JOIN PrepareLoadGoodsInfo g ON g.OrderNumber = d.ordersn
        -- *** ĐÃ SỬA LỖI: Thay thế LATERAL JOIN bằng LEFT JOIN ***
        LEFT JOIN PackagingInfo p ON p.OrderNumber = d.ordersn
        WHERE m.HOType = 'OUT' 
    ),

    -- 4. CTE: Tương đương #OrderSiteUpdateTime
    OrderSiteUpdateTime AS (
        SELECT 
            a.ordersn AS order_number,
            MAX(a.SiteUpdateTime) AS SiteUpdateTime
        FROM dp_warehouse.staging.stg_appdatashgextappdata__SHECOMApiRawDataStatusHistory a
        JOIN ListOrder b ON a.ordersn = b.order_number
        WHERE (a.EcommerceSite = 'Shopee' AND a.order_status IN('PROCESSED'))
           OR (a.EcommerceSite = 'Lazada' AND a.order_status IN ('packed'))
           OR (a.EcommerceSite = 'TikTok' AND a.order_status = 'AWAITING_COLLECTION')
        GROUP BY a.ordersn
    ),

    -- 5. CTE: HandoverTracking
    HandoverTracking AS (
        SELECT 
            hod.ordersn, 
            hod.tracking_no, 
            hom.HONumber,
            ROW_NUMBER() OVER(PARTITION BY hod.ordersn, hom.HONumber ORDER BY hom.Id DESC) AS rn
        FROM dp_warehouse.staging.stg_appdatashgextappdata__SHECOMShopeeHandOver hom
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SHECOMShopeeHandOverDetail hod ON hom.Id = hod.MasterId
    ),

    -- 6. CTE: PackagingData
    PackagingData AS (
        SELECT 
            p.OrderNumber, d.ItemCode, d.QtyOrder
        FROM dp_warehouse.staging.stg_appdatashgextappdata__MPL_Packaging p
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__MPL_PackagingDetail d ON p.Id = d.MasterId
        WHERE p.Active = TRUE
    ),

    -- 7. CTE: SiteUpdates (Từ #OrderSiteUpdateTime)
    SiteUpdates AS (
        SELECT 
            order_number, 
            MAX(SiteUpdateTime) AS SiteUpdateTime
        FROM OrderSiteUpdateTime
        GROUP BY order_number
    ),

    -- 8. CTE: JOIN trước dữ liệu Combo
    ComboSource AS (
        SELECT 
            B1.ComboCode,
            B1.ValidFrom,
            B1.ValidTo,
            BD.ItemCode, 
            BD.ItemName, 
            BD.PriceRate, 
            B1.ComboPrice, 
            BD.QuantityRate
        FROM dp_warehouse.staging.stg_appdatashgextappdata__SH_ECOM_Combo B1
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SH_ECOM_ComboItem BD 
            ON BD.ComboCode = B1.ComboCode AND BD.ComboId = B1.Id
        WHERE B1.Active = TRUE
    ),

    -- 9. CTE: Gộp dữ liệu chính và xếp hạng (ROW_NUMBER)
    FinalQuery AS (
        SELECT
            m.RequestNumber,
            rd.EcommerceSite,
            m.Status,
            CASE m.Status
                WHEN 'New' THEN 'Mới tạo'
                WHEN 'Processing' THEN 'Đang gom'
                WHEN 'End' THEN 'Kết thúc gom'
                WHEN 'PackingDone' THEN 'Đã đóng gói'
                WHEN 'OUT' THEN 'Bàn giao đi'
                ELSE ''
            END AS StatusName,
            
            COALESCE(p_src.ItemCode, rd.ItemCode) AS ItemCode,
            COALESCE(p_src.ItemName, i.ItemName) AS ItemName,
            
            m.order_number,
            m.CollectionFullName,
            m.CollectionTime,
            m.PackagingByFullName,
            m.PackagingTime,
            m.HOByFullName,
            m.HODate,
            st.SiteUpdateTime,
            COALESCE(ha.tracking_no, rd.tracking_no) AS tracking_no,
            COALESCE(pd.QtyOrder, rd.quantity) AS quantity,
            rd.shipping_carrier,
            m.Note,
            
            -- CỘT "COVER"
            m.Status AS Status_Cover,
            m.WarehouseLocation AS Warehouse_Cover,
            rd.EcommerceSite AS EcommerceSite_Cover,
            m.UserName_Cover AS UserName_Cover,
            CAST(m.CollectionTime AS DATE) AS Date_Cover,
            CASE 
                WHEN COALESCE(rd.warehouse_code, o.warehouse) IN (
                    'VN0157WSZ', 'VN10619-WH-10002', '7352861353030797061', 
                    '7498657948904097554', '7498657948904081170', '7509840536435902225'
                ) THEN 'NECL' 
                ELSE 'BECO' 
            END AS Warehouse_Computed_Cover,
            
            ROW_NUMBER() OVER(
                PARTITION BY m.order_number, rd.ItemCode 
                ORDER BY p_src.ValidFrom DESC NULLS LAST 
            ) as rn

        FROM ListOrder AS m
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SHECOMApiRawData o ON o.order_number = m.order_number
        JOIN dp_warehouse.staging.stg_appdatashgextappdata__SHECOMApiRawDataDetail rd ON rd.order_number = o.order_number

        LEFT JOIN ComboSource p_src
            ON rd.ItemCode = p_src.ComboCode 
            AND o.create_time BETWEEN p_src.ValidFrom AND COALESCE(p_src.ValidTo, DATE '2299-12-31')

        LEFT JOIN PackagingData pd
            ON pd.OrderNumber = m.order_number
            AND pd.ItemCode = COALESCE(p_src.ItemCode, rd.ItemCode) 

        LEFT JOIN HandoverTracking ha
            ON ha.ordersn = m.order_number 
            AND ha.HONumber = m.RequestNumber 
            AND ha.rn = 1

        JOIN dp_warehouse.staging.stg_appdatashgextappdata__MDataItems i
            ON i.ItemCode = rd.ItemCode

        LEFT JOIN SiteUpdates st
            ON st.order_number = m.order_number
    )

-- 10. SELECT cuối cùng: Lọc lấy rn = 1
SELECT *
FROM FinalQuery
WHERE rn = 1 
ORDER BY 
    CollectionTime, order_number, ItemCode;