WITH
    -- 1. CTE: Tương đương với Subquery (G)
    G AS (
        -- 1.1: Phần SELECT đầu tiên
        SELECT 
            B.artcode AS ItemCode,
            B.SaleIndustry AS IndustryCode,
            C.textfield11 AS ChainCode,
            B.Costcenter,
            C.MTGroupNetPrice,
            SUM(CASE WHEN B.SYear = (YEAR(CURRENT_DATE) - 1) THEN B.SaleQuantity ELSE 0 END) AS LYSaleQuantity,
            SUM(CASE WHEN B.SYear = YEAR(CURRENT_DATE) THEN B.SaleQuantity ELSE 0 END) AS SaleQuantity,
            CAST(NULL AS DATE) AS MustZoningDate
        FROM dp_warehouse.staging.stg_appdatashGextappdata__SHBaseSaleBIData B
        JOIN dp_warehouse.staging.stg_appdatashGextappdata__Cicmpy_Consolidated C ON B.invcmp_wwn = C.cmp_wwn
        WHERE 
            C.textfield11 IS NOT NULL
            AND B.SaleIndustry IS NOT NULL
        GROUP BY 
            B.artcode, B.SaleIndustry, C.textfield11, C.MTGroupNetPrice, B.Costcenter

        UNION ALL

        -- 1.2: Phần UNION ALL
        SELECT 
            Z.ItemCode,
            I.IndustryCode,
            Z.ChainCode,
            Z.CostCenter,
            '' AS MTGroupNetPrice,
            0 AS LYSaleQTy,
            0 AS SaleQuantity,
            Z.MustZoningDate
        FROM dp_warehouse.staging.stg_appdatashGextappdata__SHPoliciesCodePlanTimeProduct Z
        JOIN dp_warehouse.staging.stg_appdatashGextappdata__MDataItems I ON I.ItemCode = Z.ItemCode
        WHERE 
            Z.Approval = TRUE
    ),

    -- 2. CTE: Tương đương với Subquery (PP)
    PP AS (
        SELECT 
            G.ItemCode, G.IndustryCode, G.ChainCode, G.Costcenter,
            SUM(G.LYSaleQuantity) AS LYSaleQuantity,
            SUM(G.SaleQuantity) AS SaleQuantity,
            MAX(G.MustZoningDate) AS MustZoningDate,
            MAX(G.MTGroupNetPrice) AS MTGroupNetPrice
        FROM G
        GROUP BY 
            G.ItemCode, G.IndustryCode, G.ChainCode, G.Costcenter
    ),
    
    -- 3. CTE: Tính SalePriceNoVat
    SalePriceNoVatData AS (
        SELECT 
            P.artcode,
            P.bedr1,
            ROW_NUMBER() OVER(PARTITION BY TRIM(P.artcode) ORDER BY P.validfrom DESC) AS rn
        FROM dp_warehouse.staging.stg_exact101__staffl P 
        WHERE TRIM(P.prijslijst) = '30-110310'
          AND P.AccountID IS NULL
    ),
    
    -- 4. CTE: Tính NTDSalePrice
    NTDSalePriceData AS (
        SELECT 
            P.artcode,
            P.bedr1,
            ROW_NUMBER() OVER(PARTITION BY TRIM(P.artcode) ORDER BY P.validfrom DESC) AS rn
        FROM dp_warehouse.staging.stg_appdatashGextappdata__ERPPriceData P
        WHERE P.prijslijst = 'NTD' AND P.cmp_wwn IS NULL
    ),
    
    -- 5. CTE: Tính NTDKMQDPrice
    NTDKMQDPriceData AS (
        SELECT 
            D.ItemCode,
            MAX(D.Price) AS Price
        FROM dp_warehouse.staging.stg_appdatashGextappdata__SHPriceListDetail D 
        WHERE D.PriceListCode = 'SHG_NTDKMQD'
        GROUP BY D.ItemCode
    ),
    
    -- 6. CTE MỚI: Tính GroupNetPrice (THAY THẾ SUBQUERY 1)
    GroupNetPriceData AS (
        SELECT 
            D.GroupCode,
            D.ItemCode,
            MAX(D.Price) AS Price 
        FROM dp_warehouse.staging.stg_appdatashGextappdata__SHPriceListDetail D 
        WHERE D.PriceListCode = 'MT_GROUP_NET_PRICE'
        GROUP BY 1, 2
    ),
    
    -- 7. CTE MỚI: Tính ZoningQuantityDate (THAY THẾ LEFT JOIN LATERAL X)
    ZoningDateData AS (
        SELECT 
            R.ItemCode, 
            MIN(R.afldat) AS afldat,
            C.textfield11 AS ChainCode
        FROM dp_warehouse.staging.stg_appdatashGextappdata__SHBaseSaleFullfilResult R
        JOIN dp_warehouse.staging.stg_appdatashGextappdata__Cicmpy_Consolidated C ON R.ordCmp_wwn = C.cmp_wwn
        GROUP BY 1, 3
    )

-- 8. SELECT cuối cùng
SELECT 
    PP.ItemCode,
    PP.IndustryCode,
    PP.ChainCode,
    PP.LYSaleQuantity,
    PP.SaleQuantity,
    PP.MustZoningDate,
    PP.MTGroupNetPrice,
    PP.Costcenter,
    sc.Region,
    
    -- **ĐÃ SỬA LỖI:** Thay Subquery 1 bằng LEFT JOIN
    COALESCE(GNP.Price, 0) AS GroupNetPrice,
    
    -- Subquery 2 (Đã sửa trước): NTDKMQDPrice
    COALESCE(NTDKMQD.Price, 0) AS NTDKMQDPrice,
    
    -- Subquery 3 (Đã sửa trước): NTDSalePrice
    COALESCE(NTD.bedr1, 0) AS NTDSalePrice,
    
    -- Subquery 4 (Đã sửa trước): SalePriceNoVat
    CAST(COALESCE(SPNV.bedr1, 0) AS DECIMAL) AS SalePriceNoVat,
    
    -- **ĐÃ SỬA LỖI:** Lấy từ JOIN mới
    (CASE WHEN ZDD.afldat IS NOT NULL THEN 1 ELSE NULL END) AS TodayZoningQuantity,
    ZDD.afldat AS ZoningQuantityDate,
    M.ItemName,
    M.Class_01,
    M.UserField_02
    
FROM PP

-- **ĐÃ SỬA LỖI:** Thêm JOIN với CTE mới
LEFT JOIN GroupNetPriceData GNP 
    ON GNP.ItemCode = PP.ItemCode
    AND GNP.GroupCode = PP.MTGroupNetPrice

-- **ĐÃ SỬA LỖI:** Thêm JOIN với CTE mới (thay thế X)
LEFT JOIN ZoningDateData ZDD
    ON ZDD.ItemCode = PP.ItemCode
    AND ZDD.ChainCode = PP.ChainCode -- Giả định ChainCode là điều kiện JOIN cần thiết

LEFT JOIN NTDKMQDPriceData NTDKMQD
    ON NTDKMQD.ItemCode = PP.ItemCode

LEFT JOIN NTDSalePriceData NTD 
    ON TRIM(NTD.artcode) = PP.ItemCode
    AND NTD.rn = 1

LEFT JOIN SalePriceNoVatData SPNV 
    ON TRIM(SPNV.artcode) = PP.ItemCode
    AND SPNV.rn = 1

JOIN dp_warehouse.staging.stg_appdatashGextappdata__MDataItems M ON M.ItemCode = PP.ItemCode
JOIN dp_warehouse.staging.stg_appdatashGextappdata__SHCostcenter sc ON sc.CostCenter = PP.Costcenter