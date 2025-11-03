{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng ID và ngày ETL làm unique_key
        unique_key=['data_date', 'id'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    FACMachine_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FACMachine') }}),
    FAC_MachineHistory_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FAC_MachineHistory') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MDataItems') }}),
    FAC_MaintainGroup_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FAC_MaintainGroup') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHCostcenter') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT M.*
        FROM FACMachine_Source M
        WHERE M.Active = TRUE
        
        -- Lọc Incremental (Chỉ lấy các bản ghi đã được sửa đổi kể từ lần chạy trước)
        {% if is_incremental() %}
            AND M.ModifyDate >= (SELECT MAX(t.ModifyDate) FROM {{ this }} t WHERE t.ModifyDate IS NOT NULL)
        {% endif %}
    ),

    -- Subquery (E) để tìm ngày lịch sử mới nhất
    LatestHistoryDate AS (
        SELECT 
            MAX(F.CreateDate) AS maxDate,
            F.MachineId
        FROM FAC_MachineHistory_Source F
        GROUP BY F.MachineId
    ),
    
    -- Subquery (H) để lấy chi tiết lịch sử mới nhất
    LatestHistoryDetail AS (
        SELECT 
            H.MachineId,
            H.ToUserName,
            H.FMHNumber,
            H.ToCostcenter,
            H.CreateDate AS maxDate -- Giữ lại cột ngày để join với E
        FROM FAC_MachineHistory_Source H
        JOIN LatestHistoryDate E ON E.MachineId = H.MachineId AND E.maxDate = H.CreateDate
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    M.Id,
    M.Division,
    M.ERPItemCode,
    M.ERPAssetNumber,
    M.SupplierModel,
    M.SerialNo,
    M.MachineName,
    M.DateOfPurchase,
    M.PowerConsumption,
    M.MadeIn,
    M.Manufacturer,
    M.MadeYear,
    M.UseFor,
    M.MaintainGroupCode,
    G.MaintainGroupName,
    M.MaintenanceSchedule,
    M.DicNote,
    M.SysNote,
    M.CreateDate,
    M.CreateBy,
    M.ModifyDate,
    M.ModifyBy,
    I.ItemName,
    I.ItemCode,
    M.vendor_key,
    M.vendor_name,
    
    COALESCE(H.ToUserName, '') AS ToUserName,
    COALESCE(H.FMHNumber, '') AS FMHNumber,
    H.ToCostcenter AS ToCostcenter,
    
    -- Chuyển đổi Subquery (SELECT CostCenterName LIMIT 1)
    (
        SELECT CostCenterName
        FROM SHCostcenter_Source
        WHERE CostCenter = H.ToCostcenter
        LIMIT 1
    ) AS ToCostcenterName,
    M.Active
FROM 
   	FilteredMaster M
LEFT JOIN LatestHistoryDetail H ON M.Id = H.MachineId
LEFT JOIN MDataItems_Source I ON M.ERPItemCode = I.ItemCode
LEFT JOIN FAC_MaintainGroup_Source G ON G.MaintainGroupCode = M.MaintainGroupCode
ORDER BY 
    M.ModifyDate DESC
    
{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}