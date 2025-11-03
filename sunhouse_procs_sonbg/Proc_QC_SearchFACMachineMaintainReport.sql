{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng Master ID và ngày ETL làm unique_key
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
    FAC_MachineMaintainHistory_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FAC_MachineMaintainHistory') }}),
    FACMachine_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FACMachine') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT *
        FROM FAC_MachineMaintainHistory_Source E
        WHERE 
            E.IsActive = TRUE
            AND E.RequestStatus <> 'KTTBREJECT'
            
            -- Lọc Incremental (Chỉ lấy dữ liệu mới/thay đổi)
            {% if is_incremental() %}
                -- Sử dụng CreateDate để lọc dữ liệu mới
                AND E.CreateDate >= (SELECT MAX(t.CreateDate) FROM {{ this }} t WHERE t.CreateDate IS NOT NULL)
            {% endif %}
    ),

    -- 1. CTE: Tính toán số lần bảo trì trên mỗi máy
    MachineCounts AS (
        SELECT 
            A.MachineId,
            M.SerialNo,
            M.MachineName,
            COUNT(A.MachineId) AS CountMaintain
        FROM 
            FAC_MachineMaintainHistory_Source A -- Sử dụng nguồn đầy đủ để tính tổng
            JOIN FACMachine_Source M ON A.MachineId = M.Id
        WHERE 
            A.IsActive = TRUE
        GROUP BY 
            A.MachineId, M.SerialNo, M.MachineName
    ),

    -- 2. CTE: Tổng số máy đang hoạt động
    MachineTotal AS (
        SELECT COUNT(*) AS CountMachine
        FROM FACMachine_Source M
        WHERE M.Active = TRUE
    )

-- 3. SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    E.Id,
    E.MachineId,
    E.RequestNumber,
    E.CreateBy,
    E.ToCostcenter AS CostCenter,
    E.CreateDate,
    B.SerialNo,
    B.MachineName,
    B.CountMaintain,
    
    -- Tính thời gian trễ (TimeOff)
    DATE_DIFF('hour', E.RequestTime, COALESCE(E.DirectorApprovedDate, CURRENT_TIMESTAMP)) AS TimeOff,
    
    -- Lấy tổng số máy từ CTE MachineTotal
    MT.CountMachine,
    
    E.RequestTime, -- Giữ lại RequestTime để tính TimeOff trong tương lai
    E.DirectorApprovedDate -- Giữ lại DirectorApprovedDate
    
FROM 
    FilteredMaster E
-- Join với CTE MachineCounts
JOIN MachineCounts B ON E.MachineId = B.MachineId
-- CROSS JOIN để lấy giá trị tổng
CROSS JOIN MachineTotal MT
ORDER BY 
    E.RequestTime DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}