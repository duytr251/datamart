{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Sử dụng Master ID và Detail ID làm unique_key để tránh mất dữ liệu chi tiết
        unique_key=['data_date', 'id', 'itemid'],
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define dbt Sources (Giả định các bảng staging đã được ánh xạ)
    SHWorkflowProcessUser AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHWorkflowProcessUser') }}
    ),
    RD_RequestMaster AS (
        SELECT * FROM {{ source('dp_src_appdata_shg_extappdata', 'RD_RequestMaster') }}
    ),
    RD_RequestDetail AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestDetail') }}
    ),
    SHCostcenter AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__shcostcenter') }}
    ),
    RD_RequestPlanMaster AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestPlanMaster') }}
    ),

    -- CTE 1: Tính toán Trạng thái Workflow
    WorkflowStatus AS (
        SELECT
            A.ProcessId,
            CASE
                WHEN COUNT(CASE WHEN A.ActionKey = 'WAITSTOP' THEN 1 END) > 0 THEN 'WAITSTOP'
                WHEN COUNT(CASE WHEN A.ActionKey = 'STOP' THEN 1 END) > 0 THEN 'STOP'
                WHEN COUNT(CASE WHEN A.ActionKey = 'REJECT' AND A.ProcessLevel = 4 THEN 1 END) > 0 THEN 'DISAGREE'
                WHEN COUNT(CASE WHEN A.ActionKey = 'REJECT' AND A.ProcessLevel <> 4 THEN 1 END) > 0 THEN 'REJECT'
                WHEN COUNT(CASE WHEN A.ActionKey = 'APPROVAL' THEN 1 END) = COUNT(CASE WHEN (A.ActionKey IS NOT NULL AND A.ActionKey <> 'NEW') THEN 1 END) THEN 'APPROVAL'
                WHEN COUNT(CASE WHEN A.ActionKey = 'WAIT' THEN 1 END) > 0 THEN 'WAIT'
                ELSE 'WAITSEND'
            END AS RequestStatus,
            COUNT(CASE WHEN A.ActionKey = 'REJECT' OR A.ActionKey = 'APPROVAL' THEN 1 END) AS StatusDetail,
            COUNT(CASE WHEN (A.ActionKey IS NOT NULL AND A.ActionKey <> 'NEW') THEN 1 END) AS StatusTotal
        FROM
            SHWorkflowProcessUser A
        WHERE
            A.WFCode = 'YEU_CAU_PHAT_TRIEN_SAN_PHAM'
        GROUP BY
            A.ProcessId
    )

-- FINAL SELECT (Tương đương với chế độ 'RDDetail')
SELECT 
    -- 1. Cột Metadata Incremental
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    RD.Id,
    RD.RequestNumber,
    RD.Costcenter,
    RD.RequestBy,
    RD.RequestDate,
    RD.Approval,
    RD.Active,
    RD.CreateDate,
    RD.CreateBy,
    RD.ModifyDate,
    RD.ModifyBy,
    C.CostCenterName,
    DT.Id AS ItemId,
    DT.ItemCode,
    DT.ItemName,
    DT.IndustryCode,
    DT.ClassCode,
    DT.PM,
    DT.AvgQty,
    DT.ItemType,
    DT.Segmentation,
    DT.TargetFromDate,
    DT.TargetToDate,
    RD.RequestTitle,
    RD.RequestNote,
    COALESCE(K.PlanStatus, 'WAITSEND') AS StatusPlan,
    CAST(COALESCE(D.StatusDetail, 0) AS VARCHAR) || '/' || CAST(COALESCE(D.StatusTotal, 0) AS VARCHAR) AS StatusDetail,
    COALESCE(D.RequestStatus, 'WAITSEND') AS RequestStatus,
    DT.ItemStatus,
    K.Id AS PlanId,
    K.RDPlanNumber,
    COALESCE(DT.RateNumberQuality, 0) AS RateNumberQuality,
    DT.RateNoteQuality,
    COALESCE(DT.RateNumberProcess, 0) AS RateNumberProcess,
    DT.RateNoteProcess,
    DT.RateBy,
    DT.RateDate,
    DT.Leader,
    DT.Performers
FROM 
    RD_RequestMaster RD
    LEFT JOIN RD_RequestDetail DT ON DT.MasterId = RD.Id
    INNER JOIN SHCostcenter C ON C.CostCenter = RD.Costcenter
    LEFT JOIN WorkflowStatus D ON D.ProcessId = RD.Id
    LEFT JOIN RD_RequestPlanMaster K ON DT.Id = K.RequestDetaiId
WHERE 
    -- Lọc dữ liệu đang hoạt động
    RD.Active = TRUE
    AND DT.Active = TRUE
    
    -- Lọc Incremental: Chỉ lấy các bản ghi đã được tạo hoặc sửa đổi gần đây
    {% if is_incremental() %}
        -- Giả định rằng dữ liệu mới được xác định qua ModifyDate (hoặc CreateDate)
        -- Sử dụng cột ModifyDate của cả Master và Detail để đảm bảo bắt được thay đổi
        AND (
            RD.ModifyDate >= (SELECT MAX(t.ModifyDate) FROM {{ this }} t WHERE t.ModifyDate IS NOT NULL)
            OR DT.ModifyDate >= (SELECT MAX(t.ModifyDate) FROM {{ this }} t WHERE t.ModifyDate IS NOT NULL)
        )
    {% endif %}
ORDER BY 
    RD.CreateDate DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}