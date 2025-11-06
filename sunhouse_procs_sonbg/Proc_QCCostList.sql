{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày, số hóa đơn (faktuurnr), và Cost Unit
        unique_key=['data_date', 'date', 'faktuurnr', 'costunit'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    View_GBKMUT_TQ_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__View_GBKMUT_TQ') }}),
    kstdr_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exactreport__kstdr') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT G.*
        FROM View_GBKMUT_TQ_Source G
        WHERE G.transtype = 'N'
            AND SUBSTR(TRIM(G.reknr), 1, 4) = '6427'
            AND (TRIM(G.kstdrcode) IN ('038', '013', '019') OR TRIM(G.kstdrcode) LIKE 'Q%')
            
        -- Lọc Incremental (Chỉ lấy giao dịch của ngày ETL)
        {% if is_incremental() %}
            -- Sử dụng datum (ngày giao dịch) để lọc dữ liệu mới
            AND G.datum = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    G.datum AS date,
    G.faktuurnr,
    G.docnumber,
    G.oms25,
    -- Credit
    CASE 
        WHEN G.bdr_hfl < 0 THEN COALESCE(G.bdr_hfl, 0) 
        ELSE 0 
    END AS Credit,
    -- Debtor
    CASE 
        WHEN G.bdr_hfl >= 0 THEN COALESCE(G.bdr_hfl, 0) 
        ELSE 0 
    END AS debtor,
    G.kstplcode,
    G.kstdrcode AS costunit,
    K.oms25_0,
    G.res_id
FROM 
    FilteredMaster G 
INNER JOIN 
    kstdr_Source K ON G.kstdrcode = K.kstdrcode
ORDER BY 
    G.datum, G.faktuurnr, G.kstdrcode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}