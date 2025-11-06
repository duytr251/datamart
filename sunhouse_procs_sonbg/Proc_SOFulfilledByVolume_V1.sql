{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là Ngày ETL, Số SO, và Số Phiếu xuất kho
        unique_key=['data_date', 'sonumber', 'pakbonnr'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    cicmpy_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__cicmpy') }}),
    MPLmagaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLmagaz') }}),
    orhsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orhsrg') }}),
    orhkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orhkrg') }}),
    orkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__orkrg') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHCostcenter') }}),
    humres_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__humres') }}),
    AddressStates_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__AddressStates') }}),
    frhkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__frhkrg') }}),
    frhsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__frhsrg') }}),
    
    -- 1. CTE: Tương đương với biến bảng @tblCicmpy (Khách hàng đặc biệt)
    tblCicmpy AS (
        SELECT debnr 
        FROM cicmpy_Source 
        WHERE InvoiceDebtor = '832764' OR TextField11 = 'BigC'
    ),

    -- 2. CTE: Tương đương với subquery (w) (Lấy Region của kho)
    WarehouseRegion AS (
        SELECT DISTINCT G.magcode, G.Region 
        FROM MPLmagaz_Source G
        WHERE int_regio IN('A','D') AND Division = 101
    ),
    
    -- 3. Lọc Incremental cho bảng Master (Phiếu xuất kho)
    FilteredMaster AS (
        SELECT hk.*
        FROM orhkrg_Source hk
        WHERE hk.ord_soort IN ('I','T','V')
        
        -- Lọc Incremental (Chỉ lấy các phiếu xuất kho mới nhất)
        {% if is_incremental() %}
            AND hk.pakbon_dat = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    ),

    -- 4. CTE: Tương đương với #tblbase (Dữ liệu xuất kho chi tiết)
    tblbase AS (
        SELECT 
            k.ordernr AS SONumber, hk.pakbon_nr as PakbonNr, hk.refer, hk.orddat, k.Approved, k.afldat AS RequestDeliveryDate,
            hk.syscreated as Datefulfill, CAST(hk.syscreated AS TIME(0)) as TimeFul,
            COALESCE(DATE_DIFF('day', CAST(k.Approved AS DATE), CAST(hk.syscreated AS DATE)), 0) as SNAuth,
            c.debnr, k.bdr_ev_val AS totalAmountSO, COALESCE(c.StateCode,'') AS StateCode, TRIM(c.cmp_code) cmp_code, 
            hk.ord_debtor_name, hk.del_AddressLine1, St.Name as CityName, hk.afldat,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'GD' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3GD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DGD' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3DGD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DDD' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3DDD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'LN' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3LN,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'TBNB' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3TBNB,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DCN' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3DCN,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DDNB' THEN hs.aant_gelev * TRY_CAST(i.UserField_01 AS DOUBLE) END), 0)) AS m3DDNB,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'GD' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountGD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DGD' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountDGD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DDD' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountDDD,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'LN' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountLN,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'TBNB' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountTBNB,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DCN' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountDCN,
            SUM(COALESCE((CASE WHEN i.IndustryCode = 'DDNB' THEN hs.aant_gelev * hs.prijs_n END), 0)) AS AmountDDNB,
            hk.pakbon_dat, hk.magcode, TRIM(H.fullname) PersonName, TRIM(k.kstplcode) kstplcode, SUM(hs.aant_gelev * hs.prijs_n) totalAmountPX,
            co.Region AS Region_Cover, w.Region AS Warehouse_Region_Cover, co.ChannelCode AS ChanelCode_Cover,
            i.IndustryCode AS IndustryCode_Cover, TRIM(c.debcode) AS Debcode_Cover, i.Class_01 AS ItemClassCode_Cover, hk.magcode AS WH_Cover
        FROM orhsrg_Source hs
        JOIN FilteredMaster hk ON hk.pakbon_nr = hs.pakbon_nr
        JOIN orkrg_Source k ON hk.ordernr = k.ordernr
        JOIN MDataItems_Source i ON hs.artcode = i.ItemCode
        JOIN SHCostcenter_Source co ON k.kstplcode = co.CostCenter AND co.Division = 101 AND co.Active = TRUE
        JOIN cicmpy_Source c ON c.debnr = hk.verzdebnr
        JOIN WarehouseRegion w ON TRIM(hk.magcode) = w.magcode
        LEFT JOIN humres_Source H ON hk.represent_id = H.res_id
        LEFT JOIN AddressStates_Source St ON C.StateCode = St.StateCode AND St.CountryCode = 'VN'
        WHERE hs.ar_soort IN ('I','T','V') AND k.ord_soort = 'V' AND hs.aant_gelev <> 0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41
    ),

    -- 5. CTE: Tương đương `INSERT INTO @tblBaseParbon` (Lấy ngày hóa đơn - fakdat)
    tblBaseParbon AS (
        SELECT 
            a.*,
            f.fakdat,
            (a.m3GD + a.m3DGD + a.m3DDD + a.m3LN + a.m3TBNB + a.m3DCN + a.m3DDNB) AS totalM3ByIndustryClass,
            (a.AmountGD + a.AmountDGD + a.AmountDDD + a.AmountLN + a.AmountTBNB + a.AmountDCN + a.AmountDDNB) AS totalAmountByIndustryClass,
            COALESCE(DATE_DIFF('day', a.Datefulfill, f.fakdat), 0) AS SNfulfill
        FROM tblbase a
        LEFT JOIN (
            SELECT fs.pakbon_nr, fs.ordernr, MAX(fs.fakdat) AS fakdat
            FROM frhkrg_Source fk
            JOIN frhsrg_Source fs ON fs.dagbknr = fk.dagbknr AND fs.faknr = fk.faknr
            WHERE TRIM(fk.fak_soort) = 'V'
            GROUP BY fs.pakbon_nr, fs.ordernr
        ) f ON f.pakbon_nr = a.PakbonNr AND f.ordernr = a.SONumber
    ),

    -- 6. CTE: Tương đương 2x UPDATE (Tính Norms, Status, Rate)
    FinalData AS (
        SELECT
            t.*,
            -- Tính Norms
            CASE 
                WHEN TRIM(t.StateCode) IN ('4', '511', '83') THEN 
                    (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 3000000 AND t.totalAmountPX < 5000000 THEN '3' ELSE '2' END)
                WHEN TRIM(t.StateCode) NOT IN ('4', '511', '83') THEN 
                    (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 5000000 AND t.totalAmountPX < 30000000 THEN '6' ELSE '4' END)
            END AS Norms_Computed,
            
            -- Tính StatusOrderID (1/0)
            CAST(CASE 
                WHEN (COALESCE(DATE_DIFF('day', t.Datefulfill, COALESCE(t.fakdat, CURRENT_DATE)), 0) <= TRY_CAST(
                    CASE WHEN TRIM(t.StateCode) IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 3000000 AND t.totalAmountPX < 5000000 THEN '3' ELSE '2' END)
                         WHEN TRIM(t.StateCode) NOT IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 5000000 AND t.totalAmountPX < 30000000 THEN '6' ELSE '4' END) END AS DOUBLE))
                  OR (TRIM(t.StateCode) IN ('4', '511', '83') AND t.totalAmountPX < 3000000) 
                  OR (TRIM(t.StateCode) NOT IN ('4', '511', '83') AND t.totalAmountPX < 5000000)
                THEN '1' ELSE '0' END AS VARCHAR(20)) AS StatusOrderID,
                
            -- Tính StatusOrder (đạt/không đạt)
            CAST(CASE 
                WHEN (COALESCE(DATE_DIFF('day', t.Datefulfill, COALESCE(t.fakdat, CURRENT_DATE)), 0) <= TRY_CAST(
                    CASE WHEN TRIM(t.StateCode) IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 3000000 AND t.totalAmountPX < 5000000 THEN '3' ELSE '2' END)
                         WHEN TRIM(t.StateCode) NOT IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 5000000 AND t.totalAmountPX < 30000000 THEN '6' ELSE '4' END) END AS DOUBLE))
                  OR (TRIM(t.StateCode) IN ('4', '511', '83') AND t.totalAmountPX < 3000000) 
                  OR (TRIM(t.StateCode) NOT IN ('4', '511', '83') AND t.totalAmountPX < 5000000)
                THEN 'đạt' ELSE 'không đạt' END AS VARCHAR(50)) AS StatusOrder,
            
            -- Tính rateOnTime
            CAST(CASE 
                WHEN (TRIM(t.StateCode) IN ('4', '511', '83') AND t.totalAmountPX < 3000000) 
                  OR (TRIM(t.StateCode) NOT IN ('4', '511', '83') AND t.totalAmountPX < 5000000) THEN 100.0
                WHEN 
                    CASE WHEN TRIM(t.StateCode) IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 3000000 AND t.totalAmountPX < 5000000 THEN '3' ELSE '2' END)
                         WHEN TRIM(t.StateCode) NOT IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 5000000 AND t.totalAmountPX < 30000000 THEN '6' ELSE '4' END) END 
                    IS NULL THEN 0.0
                ELSE ROUND((CAST(COALESCE(DATE_DIFF('day', t.Datefulfill, COALESCE(t.fakdat, CURRENT_DATE)), 0) AS DOUBLE) / NULLIF(TRY_CAST(
                    CASE WHEN TRIM(t.StateCode) IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 3000000 AND t.totalAmountPX < 5000000 THEN '3' ELSE '2' END)
                         WHEN TRIM(t.StateCode) NOT IN ('4', '511', '83') THEN (CASE WHEN t.debnr IN (SELECT debnr FROM tblCicmpy) THEN '8' WHEN t.totalAmountPX >= 5000000 AND t.totalAmountPX < 30000000 THEN '6' ELSE '4' END) END 
                    AS DOUBLE), 0)), 2) * 100.0
            END AS DOUBLE) AS rateOnTime

        FROM tblBaseParbon t
    )

-- 7. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    t.SONumber, t.PakbonNr, t.refer, t.orddat, t.Approved, t.RequestDeliveryDate, t.Datefulfill, t.TimeFul, t.fakdat, t.SNAuth, t.SNfulfill,
    t.Norms_Computed AS Norms,
    t.StatusOrderID, t.StatusOrder, t.rateOnTime, t.debnr, t.totalAmountSO, t.StateCode, t.cmp_code, t.ord_debtor_name, t.del_AddressLine1, t.CityName,
    t.afldat, t.m3GD, t.m3DGD, t.m3DDD, t.m3LN, t.m3TBNB, t.m3DCN, t.m3DDNB,
    t.AmountGD, t.AmountDGD, t.AmountDDD, t.AmountLN, t.AmountTBNB, t.AmountDCN, t.AmountDDNB,
    t.pakbon_dat, t.magcode, t.PersonName, t.kstplcode, t.totalAmountPX, t.totalM3ByIndustryClass, t.totalAmountByIndustryClass,
    t.Region_Cover, t.Warehouse_Region_Cover, t.ChanelCode_Cover, t.IndustryCode_Cover, t.Debcode_Cover, t.ItemClassCode_Cover, t.WH_Cover
FROM FinalData t
ORDER BY t.pakbon_dat DESC, t.SONumber

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}