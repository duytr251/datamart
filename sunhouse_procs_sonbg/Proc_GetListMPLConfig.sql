{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng ID và FromDate làm unique_key
        unique_key=['data_date', 'id', 'fromdate'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    MPLmagazMPLConfig_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPLmagazMPLConfig') }}),
    MPLmagaz_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MPLmagaz') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT m.*
        FROM MPLmagazMPLConfig_Source m
        WHERE m.Active = TRUE
        
        -- Lọc Incremental (Chỉ lấy các bản ghi có FromDate mới nhất)
        {% if is_incremental() %}
            -- Lấy dữ liệu mới nhất (dùng FromDate để xác định bản ghi mới)
            AND m.FromDate >= (SELECT MAX(t.FromDate) FROM {{ this }} t WHERE t.FromDate IS NOT NULL)
        {% endif %}
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    m.Id,
    m.Division,
    m.Magcode,
    n.name AS MagName,
    m.Acreage,
    m.MaxSizeVolumn,
    COALESCE(m.MagazVolumn, 0) AS MagazVolumn,
    m.FromDate
FROM 
    FilteredMaster m
-- JOIN với MPLmagaz (gán cứng n.Division = 101)
JOIN 
    MPLmagaz_Source n ON m.Magcode = n.magcode AND n.Division = 101
ORDER BY 
    m.FromDate DESC, m.Magcode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}