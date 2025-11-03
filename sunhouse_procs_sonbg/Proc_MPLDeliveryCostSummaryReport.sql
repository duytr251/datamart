{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày, trung tâm chi phí, loại đơn hàng, mã khách hàng/NCC
        unique_key=['data_date', 'deliverydate', 'costcenter', 'ordernr_type', 'debnr'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources and Parameters
    MPLDeliveryManager_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLDeliveryManager') }}),
    MPLDeliveryManagerMaster_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLDeliveryManagerMaster') }}),
    MPLVehicles_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLVehicles') }}),
    SHStatesCountry_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHStatesCountry') }}),
    Cicmpy_Consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__Cicmpy_Consolidated') }}),
    MPLDeliveryPriceByVolume_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLDeliveryPriceByVolume') }}),

    params AS (
        SELECT
            DATE '2025-05-01' AS FromDate_Filter,
            DATE '2025-05-31' AS ToDate_Filter,
            CAST(0 AS BOOLEAN) AS ViewTotal_Filter,
            'MINHNV6' AS UserName_Filter,
            'SHG-KDMT' AS CostCenter_Filter,
            'TRUCK' AS DeliveryType_Filter,
            CAST(1.3 AS DOUBLE) AS CoefficientMT,
            CAST(1.4 AS DOUBLE) AS CoefficientMB
    ),
    
    -- 0.1 Logic Lọc Incremental cho bảng Master
    FilteredDeliveryManager AS (
        SELECT D.*
        FROM MPLDeliveryManager_Source D
        -- Lọc Incremental (Chỉ lấy dữ liệu của ngày ETL)
        {% if is_incremental() %}
            -- Giả định ngày ETL chạy là ngày DeliveryDate
            WHERE D.DeliveryDate = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    ),


    -- 1. CTE: Tính toán `TotalVolume` trước
    TotalVolumes AS (
        SELECT 
            DeliveryNumber, 
            del_cmp_wwn,
            SUM(Volume) AS TotalVolume
        FROM MPLDeliveryManager_Source -- Dùng nguồn gốc để có tổng volume đầy đủ
        GROUP BY DeliveryNumber, del_cmp_wwn
    ),
    
    -- 2. CTE: Lấy giá cước TOP 1 theo điều kiện (Pre-ranked)
    DeliveryPriceRanked AS (
        SELECT 
            K.DeliveryPrice,
            K.cmp_wwn,
            K.SHProvinceId,
            K.VolumeFrom,
            K.VolumeTo,
            K.CostCenter,
            ROW_NUMBER() OVER(
                PARTITION BY K.cmp_wwn, K.SHProvinceId, K.VolumeFrom, K.VolumeTo, K.CostCenter
                ORDER BY K.CreateDate DESC
            ) AS rn
        FROM MPLDeliveryPriceByVolume_Source K
    ),

    -- 3. CTE: Tương đương với Subquery (M) - Base Aggregation
    M AS (
        SELECT 
            SUM(D.Volume) AS Volume,
            PV.Area,
            D.SHStatesId,
            D.del_cmp_wwn,
            CAST(DT.MPLUrban AS BOOLEAN) AS MPLUrban,
            SUM(D.FullFillAmount) AS FullFillAmount,
            SUM(D.AlocationAmount) AS AlocationAmount,
            CASE 
                WHEN D.del_cmp_wwn IN ('55BF6FBC-2493-43D4-80CC-F3ACE423280F','14B322B4-BACA-4E42-B58A-C5D41F67FF6E') 
                AND D.DeliveryDate <= DATE '2022-04-01' THEN 'SHG-ECOM' 
                ELSE TRIM(D.kstplcode)
            END AS CostCenter,
            CI.cmp_name,
            TRIM(CI.crdcode) AS debnr,
            D.ordernr_type,
            D.DeliveryType,
            D.CalculateType,
            D.DeliveryNumber,
            D.VehicleSupplier,
            MONTH(D.DeliveryDate) AS SMonth,
            YEAR(D.DeliveryDate) AS SYear,
            D.DeliveryDate,
            V.TotalVolume
        FROM FilteredDeliveryManager D
        JOIN MPLDeliveryManagerMaster_Source MT ON D.MasterKey = MT.MasterKey
        JOIN MPLVehicles_Source V_tbl ON V_tbl.VehicleId = D.VehicleId
        LEFT JOIN SHStatesCountry_Source PV ON PV.Id = D.SHStatesId
        LEFT JOIN SHStatesCountry_Source DT ON DT.Id = D.SHDistrictId
        LEFT JOIN Cicmpy_Consolidated_Source CI 
            ON D.VehicleSupplier = CI.cmp_wwn AND CI.crdcode IS NOT NULL AND CI.Division = 101
        LEFT JOIN TotalVolumes V ON D.DeliveryNumber = V.DeliveryNumber AND D.del_cmp_wwn = V.del_cmp_wwn
        GROUP BY 
            PV.Area, D.SHStatesId, D.del_cmp_wwn, CAST(DT.MPLUrban AS BOOLEAN), 
            (CASE WHEN D.del_cmp_wwn IN ('55BF6FBC-2493-43D4-80CC-F3ACE423280F','14B322B4-BACA-4E42-B58A-C5D41F67FF6E') AND D.DeliveryDate <= DATE '2022-04-01' THEN 'SHG-ECOM' ELSE TRIM(D.kstplcode) END), 
            CI.cmp_name, TRIM(CI.crdcode), D.ordernr_type, D.DeliveryType, D.CalculateType, 
            D.DeliveryNumber, D.VehicleSupplier, MONTH(D.DeliveryDate), YEAR(D.DeliveryDate), D.DeliveryDate, V.TotalVolume
    ),

    -- 4. CTE: Tương đương với Subquery (H) - Áp dụng giá đã lọc
    H AS (
        SELECT 
            M.*,
            Price.DeliveryPrice,
            CASE 
                WHEN M.CalculateType = 'ODD' THEN Price.DeliveryPrice
                WHEN M.CalculateType NOT IN ('ODD','HOS') THEN M.AlocationAmount 
                ELSE NULL
            END AS AlocationAmount_Priced
        FROM M
        LEFT JOIN DeliveryPriceRanked Price ON 
            Price.cmp_wwn = M.VehicleSupplier AND 
            Price.SHProvinceId = M.SHStatesId AND 
            M.TotalVolume BETWEEN Price.VolumeFrom AND Price.VolumeTo AND
            Price.rn = 1
    )

-- 5. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    SUM(H.AlocationAmount_Priced * (
        CASE 
            WHEN H.TotalVolume < 0.5 AND H.CalculateType = 'ODD' THEN 
                (CASE 
                    WHEN H.Area = 'MB' AND H.MPLUrban = FALSE THEN p.CoefficientMB
                    WHEN H.Area IN ('MN','MT') AND H.MPLUrban = FALSE THEN p.CoefficientMT
                    ELSE 1 
                END) 
            WHEN H.TotalVolume >= 0.5 AND H.CalculateType = 'ODD' THEN H.Volume * (CASE 
                    WHEN H.Area = 'MB' AND H.MPLUrban = FALSE THEN p.CoefficientMB
                    WHEN H.Area IN ('MN','MT') AND H.MPLUrban = FALSE THEN p.CoefficientMT
                    ELSE 1 
                END) 
            ELSE 1 
        END
    )) AS AlocationAmount,
    H.FullFillAmount,
    H.CostCenter,
    H.ordernr_type,
    H.debnr,
    H.DeliveryType,
    H.SMonth,
    H.SYear,
    H.cmp_name,
    SUM(H.Volume) AS Volume,
    H.DeliveryDate
FROM H
CROSS JOIN params p
GROUP BY 
    H.FullFillAmount, 
    H.CostCenter, 
    H.ordernr_type, 
    H.debnr, 
    H.DeliveryType, 
    H.SMonth, 
    H.SYear, 
    H.cmp_name,
    H.DeliveryDate
ORDER BY 
    H.DeliveryDate, H.debnr

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}