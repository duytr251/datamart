{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày tháng (SMonth/SYear) và UserName
        unique_key=['data_date', 'smonth', 'syear', 'username'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    SHTaskAssignmentPerformance_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHTaskAssignmentPerformance') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }}),

    -- Lọc Incremental cho bảng Master
    FilteredPerformance AS (
        SELECT P.*
        FROM SHTaskAssignmentPerformance_Source P
        
        -- Lọc Incremental (Chỉ lấy dữ liệu của tháng/năm mới nhất)
        {% if is_incremental() %}
            -- Giả định ngày ETL chạy hàng tháng, chỉ cần lấy tháng/năm của ngày ETL
            AND P.SMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            AND P.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
        {% endif %}
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    U.FullName,
    P.Id,
    P.UserName,
    P.CostCenter,
    P.JobTitle,
    P.SMonth,
    P.SYear,
    P.CreateDate,
    P.TotalTask,
    P.CompTask,
    P.CompTaskOutDeadline,
    P.CompTaskOutDeadlineDays,
    P.TotalWorkDays,
    P.TotalRate,
    P.TotalTaskPercent,
    P.CompTaskPercent,
    P.CompTaskOutDeadlinePercent,
    P.CompTaskOutDeadlineDaysPercent,
    P.TotalRatePercent,
    P.TotalDelayPercent,
    P.TotalPercent,
    P.KPIPercent,
    
    -- Cột "Cover"
    U.UserName AS loginname_covered,
    U.CostCenter AS costcenter_covered,
    U.job_title AS jobtitle_covered
FROM 
    FilteredPerformance P
JOIN 
    AppUsers_Source U ON U.UserName = P.UserName 
WHERE
    U.Division <> 302 
ORDER BY 
    P.TotalPercent DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}