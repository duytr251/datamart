{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng ID và Division làm unique_key (Giả sử ID là duy nhất trong Division)
        unique_key=['data_date', 'id', 'division'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    MPLGATEArealConfig_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__mplgatearealconfig') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }}),
    MPLWHLoadingPort_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLWHLoadingPort') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT G.*
        FROM MPLGATEArealConfig_Source G
        
        -- Lọc Incremental (Dựa trên CreateDate/ModifyDate)
        {% if is_incremental() %}
            -- Sử dụng ID làm cơ sở để xác định các bản ghi mới (Giả sử bảng này có ModifyDate hoặc CreateDate)
            -- Nếu không có ModifyDate, ta chỉ chèn mới (hoặc cần chạy lại toàn bộ)
            -- Ở đây, ta dùng ID để tránh quét lại toàn bộ nếu ID không đổi
            WHERE G.Id NOT IN (SELECT t.Id FROM {{ this }} t WHERE t.data_date = (SELECT MAX(data_date) FROM {{ this }}))
        {% endif %}
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    G.Id,
    G.UserName,
    G.ArealCode,
    G.LoadingPortId,
    G.Division,
    U.FullName,
    P.LoadingPort,
    U.Region
FROM 
    FilteredMaster G
JOIN
    AppUsers_Source U ON G.UserName = U.UserName
LEFT JOIN 
    MPLWHLoadingPort_Source P ON G.LoadingPortId = P.Id
ORDER BY 
    G.Division, G.UserName

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}