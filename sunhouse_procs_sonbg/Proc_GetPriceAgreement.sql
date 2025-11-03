{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng data_date làm unique_key để đảm bảo toàn bộ dữ liệu giá của ngày chạy được ghi đè
        unique_key=['data_date'],
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

SELECT
    -- Thêm cột data_date và ppn_tm
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    staffl.prijslijst AS price_list,
    items.Assortment AS item_group_code,
    items.Class_01 AS class_01_code,
    items.Userfield_02 AS model,
    items.Userfield_03 AS barcode,
    ia.Description AS item_group_name,
    ic.Description AS class_01_name,
    items.ItemCode AS item_code,
    items.Description_0 AS item_description,
    items.condition AS item_condition,
    staffl.unitcode AS unit_code,
    staffl.validfrom AS valid_from,
    staffl.validto AS valid_to,
    staffl.prijs83 AS price_level_83,
    staffl.bedr1 AS amount_1
FROM
    {{ source('dp_src_rep101', 'staffl') }} staffl
INNER JOIN
    {{ source('dp_src_rep101', 'items') }} items ON items.ItemCode = staffl.artcode
LEFT JOIN
    {{ source('dp_src_shg', 'src_exactreport_ItemClasses') }} ic ON items.Class_01 = ic.ItemClassCode AND ic.ClassID = 1
LEFT JOIN
    {{ source('dp_src_shg', 'src_exactreport_ItemAssortment') }} ia ON items.Assortment = ia.Assortment
LEFT JOIN
    {{ source('dp_src_rep101', 'cicmpy') }} c ON staffl.accountid = c.cmp_wwn
WHERE
    -- Lọc dữ liệu theo logic gốc: lấy giá đang hiệu lực tại thời điểm chạy
    items.isDiscount = 0
    AND staffl.LineType = '1'
    AND staffl.aantal1 IS NOT NULL
    AND items.condition = 'A'
    AND CURRENT_TIMESTAMP BETWEEN staffl.validfrom AND staffl.validto

{% endset %}

{% if is_incremental() %}

    {{ query }}

{% else %}

    -- Logic cho lần chạy Full Refresh ban đầu
    {{ query }}

{% endif %}