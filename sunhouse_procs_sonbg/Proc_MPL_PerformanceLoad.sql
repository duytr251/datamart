{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là MasterKey/DeliveryNumber và ngày ETL
        unique_key=['data_date', 'masterkey', 'deliverynumber'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    MPLDeliveryManager_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLDeliveryManager') }}),
    MPLVehicles_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLVehicles') }}),
    Cicmpy_Consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__Cicmpy_Consolidated') }}),
    MPLVehicleConfig_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLVehicleConfig') }}),
    MPLDeliveryPreLoad_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MPLDeliveryPreLoad') }}),

    -- 1. Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT M.*
        FROM MPLDeliveryManager_Source M
        WHERE M.VehicleId IS NOT NULL 
            AND M.DeliveryType IN ('TRUCK_SERVICE', 'TRUCK')
            
        -- Lọc Incremental (Chỉ lấy dữ liệu của ngày ETL)
        {% if is_incremental() %}
            AND M.DeliveryDate = CAST('{{ var("etl_date") }}' AS DATE)
        {% endif %}
    ),

    -- 2. CTE: Lấy cấu hình xe gần nhất (VCR)
    VehicleConfigRanked AS (
        SELECT 
            config.Tonage,
            config.Volumn,
            config.FromDate,
            ROW_NUMBER() OVER(PARTITION BY config.Tonage, config.Volumn ORDER BY config.FromDate DESC) AS rn
        FROM MPLVehicleConfig_Source config
    ),

    -- 3. CTE: Tương đương với Subquery (R) - Base Aggregation
    R AS (
        SELECT 
            M.MasterKey, 
            M.DeliveryNumber, 
            M.DriverName, 
            M.DeliveryDate, 
            M.DeliveryType, 
            M.Division, 
            V.VehicleNumber, 
            M.CalculateType, 
            C.cmp_name AS VehicleSupplier,
            V.VehicleType, 
            V.VehicleName, 
            MAX(M.RealDistance) AS MaxDistance, 
            COALESCE(M.VehicleTonnage, V.Tonnage) AS Tonnage,
            COALESCE(VCR.Volumn, V.Mass) AS Mass, 
            SUM(M.Volume) AS Volume, 
            M.CostCenter
        FROM 
            FilteredMaster M
        INNER JOIN 
            MPLVehicles_Source V ON M.VehicleId = V.VehicleId
        LEFT JOIN 
            Cicmpy_Consolidated_Source C ON M.VehicleSupplier = C.cmp_wwn
        LEFT JOIN 
            VehicleConfigRanked VCR ON VCR.Tonage = COALESCE(M.VehicleTonnage, V.Tonnage)
                                 AND VCR.FromDate <= M.DeliveryDate 
                                 AND VCR.rn = 1
        GROUP BY 
            M.MasterKey, M.DeliveryNumber, M.DriverName, M.DeliveryDate, M.DeliveryType, 
            M.VehicleTonnage, M.Division, V.VehicleNumber, V.VehicleType, V.VehicleName, 
            V.Mass, V.Tonnage, C.cmp_name, M.CostCenter, M.CalculateType, VCR.Volumn, VCR.Tonage
    ),
    
    -- 4. CTE: Lấy MaxAddress
    MaxAddressData AS (
        SELECT 
            D.DeliveryNumber,
            ROW_NUMBER() OVER(PARTITION BY D.DeliveryNumber ORDER BY D.RealDistance DESC) AS rn,
            D.del_AddressLine1 AS MaxAddress
        FROM MPLDeliveryManager_Source D
    ),
    
    -- 5. CTE: Lấy Tripcode
    TripcodeData AS (
        SELECT 
            P.PreloadId,
            ROW_NUMBER() OVER(PARTITION BY P.PreloadId ORDER BY P.Tripcode) AS rn, 
            P.Tripcode
        FROM MPLDeliveryPreLoad_Source P
    )


-- 6. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    R.MasterKey, 
    R.DeliveryNumber, 
    R.DriverName, 
    R.DeliveryDate,
    CAST(1 AS BOOLEAN) AS DeliveryStatus,
    R.Division, 
    R.VehicleNumber, 
    R.VehicleSupplier, 
    R.VehicleType, 
    R.VehicleName, 
    R.DeliveryType, 
    R.Tonnage,
    R.Mass, 
    R.Volume, 
    CASE 
        WHEN COALESCE(R.Mass, 0) > 0 THEN ROUND((R.Volume * 100.0 / R.Mass), 2) 
        ELSE 0 
    END AS PLoad, 
    R.MaxDistance, 
    R.CostCenter,
    
    MAD.MaxAddress,
    TD.Tripcode,
    
    CASE R.CalculateType 
        WHEN 'ODD' THEN 'Xe lẻ'
        WHEN 'NET' THEN 'Xe chuyến'
        ELSE '' 
    END AS CalculateType,
    
    R.DeliveryDate AS FromDate_Cover,
    R.DeliveryDate AS ToDate_Cover,
    R.CostCenter AS CostCenter_Cover,
    R.DriverName AS Keyword_Cover
    
FROM R
LEFT JOIN MaxAddressData MAD 
    ON MAD.DeliveryNumber = R.DeliveryNumber 
    AND MAD.rn = 1
    
LEFT JOIN TripcodeData TD 
    ON TD.PreloadId = R.MasterKey
    AND TD.rn = 1
    
ORDER BY 
    R.DeliveryDate DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}