{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là Ngày ETL, Division, mã Khách hàng (debnr), và Cost Center
        unique_key=['data_date', 'division', 'debnr', 'costcenter'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    gbkmut_101_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__gbkmut') }}),
    cicmpy_101_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__cicmpy') }}),
    humres_101_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__humres') }}),
    gbkmut_102_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact102__gbkmut') }}),
    cicmpy_102_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact102__cicmpy') }}),
    humres_102_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact102__humres') }}),

    -- 0.1 Lọc Incremental cho bảng Master (gbkmut)
    Filtered_gbkmut_101 AS (
        SELECT G.*
        FROM gbkmut_101_Source G
        WHERE g.bud_vers IS NULL 
            AND g.debnr <> '838375' 
            AND COALESCE(g.transsubtype, '') <> 'X'
            AND g.remindercount <= 10 
            AND g.bkstnr IS NOT NULL
            AND g.kstplcode IS NOT NULL
            -- Lọc Incremental (Chỉ lấy giao dịch mới)
            {% if is_incremental() %}
                AND G.datum = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
    ),
    
    Filtered_gbkmut_102 AS (
        SELECT G.*
        FROM gbkmut_102_Source G
        WHERE g.bud_vers IS NULL 
            AND COALESCE(g.transsubtype, '') <> 'X'
            AND g.remindercount <= 10 
            AND g.bkstnr IS NOT NULL
            AND g.kstplcode IS NOT NULL
            -- Lọc Incremental (Chỉ lấy giao dịch mới)
            {% if is_incremental() %}
                AND G.datum = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
    ),


    -- 1. CTE: Tương đương với @tb_base (Gộp dữ liệu 101 và 102)
    tb_base AS (
        -- Lấy công nợ 101
        SELECT 
            g.Division, MAX(g.res_id) AS res_id, MAX(h.fullname) AS fullname, g.debnr, MAX(c.debcode) AS debcode,
            CAST(c.cmp_wwn AS VARCHAR(340)) AS cmp_wwn, MAX(c.cmp_name) AS cmp_name, g.kstplcode, MAX(G.datum) AS datum, SUM(G.bdr_hfl) AS debtAmount,
            g.kstplcode AS CostCenter_Cover, CAST(c.cmp_wwn AS VARCHAR(340)) AS cmp_wwn_Cover, MAX(G.datum) AS ViewDate_Cover, MAX(h.usr_id) AS UserName_Cover
        FROM Filtered_gbkmut_101 G
        LEFT JOIN cicmpy_101_Source c ON c.debnr = G.debnr
        LEFT JOIN humres_101_Source h ON g.res_id = h.res_id
        GROUP BY g.Division, g.debnr, c.cmp_wwn, g.kstplcode

        UNION ALL

        -- Lấy công nợ 102
        SELECT 
            g.Division, MAX(g.res_id) AS res_id, MAX(h.fullname) AS fullname, g.debnr, MAX(c.debcode) AS debcode, 
            CAST(c.cmp_wwn AS VARCHAR(340)) AS cmp_wwn, MAX(c.cmp_name) AS cmp_name, g.kstplcode, MAX(G.datum) AS datum, SUM(G.bdr_hfl) AS debtAmount,
            g.kstplcode AS CostCenter_Cover, CAST(c.cmp_wwn AS VARCHAR(340)) AS cmp_wwn_Cover, MAX(G.datum) AS ViewDate_Cover, MAX(h.usr_id) AS UserName_Cover
        FROM Filtered_gbkmut_102 G
        LEFT JOIN cicmpy_102_Source c ON c.debnr = G.debnr
        LEFT JOIN humres_102_Source h ON g.res_id = h.res_id
        GROUP BY g.Division, g.debnr, c.cmp_wwn, g.kstplcode
    ),

    -- 2. CTE: Tương đương với @tb_102_zero (Công nợ 102 có tổng bằng 0)
    tb_102_zero AS (
        SELECT x.debnr, x.kstplcode
        FROM tb_base x 
        WHERE x.Division = 102
        GROUP BY x.debnr, x.kstplcode
        HAVING SUM(x.debtAmount) = 0
    )

-- 3. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    T.Division,
    T.kstplcode AS CostCenter, 
    T.res_id, 
    T.fullname,
    T.cmp_name, 
    T.cmp_wwn, 
    T.debnr,
    T.debcode,
    T.datum, 
    T.debtAmount,
    T.CostCenter_Cover,
    T.cmp_wwn_Cover,
    T.ViewDate_Cover,
    T.UserName_Cover
FROM tb_base T
LEFT JOIN tb_102_zero k 
    ON k.debnr = T.debnr 
    AND k.kstplcode = T.kstplcode 
    AND T.Division = 102
WHERE 
    k.debnr IS NULL -- Loại bỏ các bản ghi 102 có tổng debtAmount = 0
ORDER BY 
    T.cmp_name

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}