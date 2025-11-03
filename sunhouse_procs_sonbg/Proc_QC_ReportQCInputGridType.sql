{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là loại thời gian và giá trị thời gian (Type và Item).
        -- Thêm SortColumn để phân biệt cùng Item ở các năm/tháng khác nhau.
        unique_key=['data_date', 'type', 'sortcolumn', 'item'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    QCGoodsInspectionLotMaster_Source AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__QCGoodsInspectionLotMaster') }}
    ),
    AppUsers_Source AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}
    ),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT 
            *,
            -- Cột cần thiết cho lọc thời gian
            CAST(YEAR(InspectionDate) AS VARCHAR(10)) AS YearItem,
            'T' || CAST(MONTH(InspectionDate) AS VARCHAR(10)) AS MonthItem,
            'W' || CAST(WEEK(InspectionDate) AS VARCHAR(10)) AS WeekItem,
            LPAD(CAST(DAY(InspectionDate) AS VARCHAR(2)), 2, '0') || '/' || LPAD(CAST(MONTH(InspectionDate) AS VARCHAR(2)), 2, '0') AS DateItem
        FROM QCGoodsInspectionLotMaster_Source mas
        WHERE 
            mas.Active = TRUE
            AND mas.InspectionStatus = 'COMPLETE'
            -- Lọc Incremental (Chỉ lấy dữ liệu của ngày ETL hiện tại)
            {% if is_incremental() %}
                -- Giả định ngày cần lọc là InspectionDate
                AND CAST(mas.InspectionDate AS DATE) = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
    ),

    -- 1. CTE: NĂM (Dùng YEAR() và CAST)
    YearData AS (
        SELECT 
            mas.YearItem AS Item,
            COUNT(mas.Id) AS Total,
            SUM(CASE WHEN mas.Decision = 'NG' THEN 1 ELSE 0 END) AS TotalNG,
            CAST(2.5 AS DOUBLE) AS LimitTotal, 
            'YEAR' AS Type,
            mas.YearItem AS SortColumn -- YearItem đã là VARCHAR(10)
        FROM FilteredMaster mas
        JOIN AppUsers_Source au ON mas.InspectionBy = au.UserName
        GROUP BY 1, 6
    ),

    -- 2. CTE: THÁNG (Dùng MONTH() và LPAD)
    MonthData AS (
        SELECT 
            mas.MonthItem AS Item,
            COUNT(mas.Id) AS Total,
            SUM(CASE WHEN mas.Decision = 'NG' THEN 1 ELSE 0 END) AS TotalNG,
            CAST(2.5 AS DOUBLE) AS LimitTotal, 
            'MONTH' AS Type,
            CAST(YEAR(mas.InspectionDate) AS VARCHAR(4)) || '/' || LPAD(CAST(MONTH(mas.InspectionDate) AS VARCHAR(2)), 2, '0') AS SortColumn
        FROM FilteredMaster mas
        JOIN AppUsers_Source au ON mas.InspectionBy = au.UserName
        GROUP BY 1, 6
    ),

    -- 3. CTE: TUẦN (Dùng WEEK())
    WeekData AS (
        SELECT 
            mas.WeekItem AS Item,
            COUNT(mas.Id) AS Total,
            SUM(CASE WHEN mas.Decision = 'NG' THEN 1 ELSE 0 END) AS TotalNG,
            CAST(2.5 AS DOUBLE) AS LimitTotal, 
            'WEEK' AS Type,
            CAST(YEAR(mas.InspectionDate) AS VARCHAR(4)) || '/W' || CAST(WEEK(mas.InspectionDate) AS VARCHAR(10)) AS SortColumn
        FROM FilteredMaster mas
        JOIN AppUsers_Source au ON mas.InspectionBy = au.UserName
        GROUP BY 1, 6
    ),

    -- 4. CTE: NGÀY (Dùng DAY() và DATE_FORMAT)
    DateData AS (
        SELECT 
            mas.DateItem AS Item,
            COUNT(mas.Id) AS Total,
            SUM(CASE WHEN mas.Decision = 'NG' THEN 1 ELSE 0 END) AS TotalNG,
            CAST(2.5 AS DOUBLE) AS LimitTotal,
            'DATE' AS Type,
            DATE_FORMAT(mas.InspectionDate, '%Y/%m/%d') AS SortColumn
        FROM FilteredMaster mas
        JOIN AppUsers_Source au ON mas.InspectionBy = au.UserName
        GROUP BY 1, 6
    )

-- 5. Gộp tất cả kết quả
SELECT 
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM YearData T
UNION ALL
SELECT
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM MonthData T
UNION ALL
SELECT
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM WeekData T
UNION ALL
SELECT
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    T.*
FROM DateData T
ORDER BY 
    Type, SortColumn, Item

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}