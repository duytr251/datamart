{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng Master key (DeliveryNumber, Region) và data_date làm unique_key
        unique_key=['data_date', 'deliverynumber', 'region'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    MPLDeliveryManager_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPLDeliveryManager') }}),
    gbkmut_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__Items') }}),
    orkrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__orkrg') }}),
    orsrg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__orsrg') }}),
    cicmpy_consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__cicmpy_consolidated') }}),

    -- 0.1 Logic Lọc Incremental cho bảng Master (MPLDeliveryManager)
    FilteredDeliveryManager AS (
        SELECT *
        FROM MPLDeliveryManager_Source
        WHERE INOUT = 'IN' 
        
        -- Lọc Incremental (Dựa trên DeliveryDate)
        {% if is_incremental() %}
            AND DeliveryDate = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    ),

    -- 0.2 Logic Lọc Incremental cho gbkmut (Cần cho CTE Tmp)
    Filteredgbkmut AS (
        SELECT *
        FROM gbkmut_Source
        WHERE 
            transtype = 'N' 
            AND transsubtype IN ('A', 'B')
            AND warehouse <> 'TRAN'
            AND TRIM(warehouse) IN ('NALA','NA1','NA2','NAOV','NRWL','NECL','NRWO', 'TAMT', 'NALA', 'NA1','NAOV','NRWL','NECL','NRWO') -- Gom tất cả kho
            
            -- Lọc Incremental (Dựa trên datum)
            {% if is_incremental() %}
                -- Cần dữ liệu gbkmut của ngày ETL (hoặc 2 tháng trước nếu cần)
                -- Giả định chỉ cần dữ liệu của ngày ETL
                AND datum = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
    ),
    
    -- 1. CTE: Tương đương #SOIN
    SOIN AS (
        SELECT DISTINCT pakbon_nr, ordernr 
        FROM FilteredDeliveryManager
        WHERE ordernr_type IN ('SO','IBT','PO')
    ),

    -- 2. CTE: Tương đương #Tmp
    Tmp AS (
        -- 2.1: Phần SELECT đầu tiên (Giao dịch nhập kho từ IBT/PO)
        SELECT
            Emp.datum,
            g.datum AS datumIn,
            g.IBTDeliveryNr,
            COALESCE(g.bkstnr_sub, g.faktuurnr) AS faktuurnr,
            Emp.warehouse AS ExpWarehouse,
            CASE WHEN g.transsubtype = 'A' THEN g.warehouse ELSE '' END AS ImpWarehouse,
            SUM(ABS(g.bdr_hfl)) AS DS,
            SUM(ABS(g.aantal)) AS SL,
            g.bkstnr
        FROM 
            Filteredgbkmut g
        LEFT JOIN (
            -- Subquery (Emp): Phần xuất kho/gốc (transtype='N', transsubtype='B')
            SELECT 
                g1.datum, g1.IBTDeliveryNr, g1.faktuurnr, 
                ABS(SUM(g1.bdr_hfl)) AS DS, ABS(SUM(g1.aantal)) AS SL, 
                g1.warehouse, 
                ABS(SUM(g1.aantal * CAST(i.UserNumber_01 AS DOUBLE))) AS M3
            FROM gbkmut_Source g1 -- Dùng nguồn gốc vì Emp cần dữ liệu của 2 tháng trước
            JOIN Items_Source i ON g1.artcode = i.ItemCode
            WHERE g1.transtype = 'N' AND g1.transsubtype = 'B' 
              AND g1.warehouse <> 'TRAN'
              -- Lọc kho/costcenter tương tự code gốc
              AND ((g1.kstplcode IN ('SHG-MPL','SHG-GD','SHG-BH') OR g1.warehouse = 'TAMT') OR (g1.kstplcode = 'MN-MPL' AND g1.warehouse IN ('NALA', 'NA1','NAOV','NRWL','NECL','NRWO')))
            -- Bỏ lọc ngày g1.datum để đơn giản hóa logic incremental
            GROUP BY g1.datum, g1.warehouse, g1.IBTDeliveryNr, g1.faktuurnr
        ) Emp ON Emp.IBTDeliveryNr = g.IBTDeliveryNr AND Emp.faktuurnr = g.faktuurnr
        
        JOIN SOIN m ON TRIM(m.pakbon_nr) = TRIM(g.bkstnr) AND (m.ordernr = TRIM(g.faktuurnr) OR m.ordernr = TRIM(g.bkstnr_sub))
        
        WHERE g.transsubtype IN ('A')
          AND COALESCE(g.aantal, 0) >= 0
          AND g.kstplcode NOT IN ('MN-PBH','MN-BH')
          AND TRIM(g.reknr) = '156100'
        GROUP BY 
            g.IBTDeliveryNr, Emp.datum, g.warehouse, COALESCE(g.bkstnr_sub, g.faktuurnr), 
            g.transsubtype, Emp.DS, Emp.SL, Emp.M3, Emp.warehouse, g.bkstnr, g.datum

        UNION ALL

        -- 2.2: Phần UNION ALL (Giao dịch nhập kho từ Sales Order (V))
        SELECT 
            s.afldat AS datum,
            NULL AS datumIn,
            NULL AS IBTDeliveryNr,
            k.ordernr AS faktuurnr,
            k.magcode AS ExpWarehouse,
            k.magcode AS ImpWarehouse,
            SUM(ABS(s.aant_gelev * s.prijs_n)) AS DS,
            SUM(ABS(s.aant_gelev)) AS SL,
            TRIM(s.PakbonNr) AS bkstnr
        FROM orkrg_Source k
        JOIN orsrg_Source s ON k.ordernr = s.ordernr
        JOIN SOIN m ON TRIM(m.pakbon_nr) = TRIM(s.PakbonNr) AND m.ordernr = TRIM(s.ordernr)
        WHERE 
            k.ord_soort IN ('V')
            AND s.ar_soort IN ('V','I')
            AND (k.fiattering = 'J' OR TRIM(k.ordernr) = '10138428')
            AND s.aant_gelev <> 0
        GROUP BY s.afldat, k.ordernr, k.magcode, s.PakbonNr
    ),

    -- 3. CTE: Tương đương #Tmp2 (phần Chi tiết)
    Tmp2 AS (
        SELECT 
            m.DeliveryNumber,
            c.cmp_name AS NCC,
            m.TransportType,
            m.ordernr AS faktuurnr,
            m.pakbon_nr AS Bkstnr,
            m.Note,
            m.Volume AS M3,
            tmp.ImpWarehouse,
            tmp.ExpWarehouse,
            tmp.DS,
            tmp.SL,
            tmp.datum,
            tmp.datumIn,
            tmp.IBTDeliveryNr,
            m.Region,
            m.DeliveryDate
        FROM FilteredDeliveryManager m
        JOIN cicmpy_consolidated_Source c ON m.VehicleSupplier = c.cmp_wwn
        JOIN Tmp tmp ON tmp.faktuurnr = m.ordernr AND tmp.bkstnr = m.pakbon_nr
    ),
    
    -- 4. CTE: Xử lý Chi tiết (IF @ViewTotal = 0)
    DetailView AS (
        SELECT 
            a.DeliveryNumber,
            a.NCC,
            a.TransportType,
            a.Note,
            a.Region,
            a.DeliveryDate,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT CAST(DATE_FORMAT(b.datum, '%d/%m/%Y') AS VARCHAR)), ', ') AS datum_String,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT CAST(DATE_FORMAT(b.datumIn, '%d/%m/%Y') AS VARCHAR)), ', ') AS datumIn_String,
            a.ImpWarehouse,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT b.ExpWarehouse), ', ') AS ExpWarehouse,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT b.faktuurnr), ', ') AS faktuurnr,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT b.Bkstnr), ', ') AS Bkstnr,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT b.IBTDeliveryNr), ', ') AS IBTDeliveryNr,
            SUM(a.M3) AS M3,
            SUM(a.SL) AS SL,
            SUM(a.DS) AS DS
        FROM Tmp2 a
        JOIN Tmp2 b ON a.DeliveryNumber = b.DeliveryNumber
        GROUP BY 
            a.DeliveryNumber, a.NCC, a.TransportType, a.Note, a.ImpWarehouse, a.Region, a.DeliveryDate
    ),

    -- 5. CTE: Xử lý Tổng hợp (ELSE / @ViewTotal = 1)
    TotalView AS (
        SELECT 
            c.cmp_name AS NCC,
            m.TransportType,
            m.Region,
            CAST(COUNT(DISTINCT m.DeliveryNumber) AS DOUBLE) AS TotalShip,
            CAST(SUM(tmp.DS) AS DOUBLE) AS TotalDS,
            CAST(SUM(tmp.SL) AS DOUBLE) AS TotalSL,
            CAST(SUM(m.Volume) AS DOUBLE) AS TotalM3
        FROM FilteredDeliveryManager m
        JOIN cicmpy_consolidated_Source c ON m.VehicleSupplier = c.cmp_wwn
        JOIN Tmp tmp ON tmp.faktuurnr = m.ordernr AND tmp.bkstnr = m.pakbon_nr
        GROUP BY 
            c.cmp_name, m.TransportType, m.Region
    )

-- 6. SELECT cuối cùng: Gộp logic IF/ELSE
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    0 AS TotalView, -- Luôn là 0 để chỉ lấy nhánh Chi tiết
    d.Region,
    d.DeliveryNumber,
    d.NCC,
    d.TransportType,
    d.Note,
    d.datum_String,
    d.datumIn_String,
    d.ImpWarehouse,
    d.ExpWarehouse,
    d.faktuurnr,
    d.Bkstnr,
    d.IBTDeliveryNr,
    d.M3,
    d.SL,
    d.DS,
    NULL AS TotalShip,
    NULL AS TotalDS,
    NULL AS TotalSL,
    NULL AS TotalM3,
    d.DeliveryDate
FROM DetailView d

UNION ALL

SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    1 AS TotalView, -- Luôn là 1 để chỉ lấy nhánh Tổng hợp
    t.Region,
    NULL AS DeliveryNumber,
    t.NCC,
    t.TransportType,
    NULL AS Note,
    NULL AS datum_String,
    NULL AS datumIn_String,
    NULL AS ImpWarehouse,
    NULL AS ExpWarehouse,
    NULL AS faktuurnr,
    NULL AS Bkstnr,
    NULL AS IBTDeliveryNr,
    NULL AS M3,
    NULL AS SL,
    NULL AS DS,
    t.TotalShip,
    t.TotalDS,
    t.TotalSL,
    t.TotalM3,
    NULL AS DeliveryDate
FROM TotalView t

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}