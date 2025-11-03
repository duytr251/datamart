{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Sử dụng Master ID và Detail ID làm unique_key vì đây là dữ liệu chi tiết
        unique_key=['data_date', 'id', 'detaildid'],
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
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestMaster') }}
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
    RD_RequestPlanDetail AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestPlanDetail') }}
    ),
    SHProcessTaskLink AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHProcessTaskLink') }}
    ),
    SHTaskDeadline AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskDeadline') }}
    ),
    SHTaskAssignment AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskAssignment') }}
    ),
    AppUsers AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}
    ),
    MDataItems AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__MDataItems') }}
    ),
    RD_RequestDetailHistoryDeadline AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestDetailHistoryDeadline') }}
    ),
    ItemClasses AS (
        SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__ItemClasses') }}
    ),

    -- 1. CTE cho #Tmp_RequestPlanDetail
    Tmp_RequestPlanDetail AS (
        SELECT
            R.Id AS PlanDetailID,
            PM.RequestDetaiId,
            P.TaskId,
            R.ToDate AS ToDateWF,
            TD.ToDate AS ToDateDL
        FROM
            RD_RequestPlanDetail R
            INNER JOIN RD_RequestPlanMaster PM ON R.PlanMasterId = PM.Id
            INNER JOIN RD_RequestDetail RD ON PM.RequestDetaiId = RD.Id
            INNER JOIN RD_RequestMaster RM ON RD.MasterId = RM.Id
            INNER JOIN SHProcessTaskLink P ON R.Id = P.ProcessId
            LEFT JOIN LATERAL (
                SELECT D.ToDate
                FROM SHTaskDeadline D
                WHERE
                    D.TaskAssignmentId = P.TaskId
                    AND D.IsAuthorized = TRUE
                ORDER BY
                    D.CreateDate
                LIMIT 1
            ) TD ON TRUE
        WHERE
            P.ProcessTableName IN ('RD_Request', 'RD_RequestPlanDetail')
            AND PM.Active = TRUE
            AND RD.Active = TRUE
            AND PM.Active = TRUE
    ),

    -- 2. CTE cho @TaskInforTemp
    TaskInforTemp AS (
        SELECT
            T.Id,
            P.RequestDetaiId,
            T.TaskName,
            CC.CostCenter,
            CC.CostCenterName,
            (
                SELECT COUNT(TaskAssignmentId)
                FROM SHTaskDeadline
                WHERE TaskAssignmentId = T.Id AND IsAuthorized = TRUE
            ) AS DeadlineNumber,
            T.StartDate AS FromDateT,
            T.CompletedDate,
            P.ToDateWF AS ToDateWF,
            P.ToDateDL
        FROM SHTaskAssignment T
        INNER JOIN Tmp_RequestPlanDetail P ON UPPER(T.Id) = UPPER(P.TaskId)
        JOIN AppUsers AU ON AU.UserName = T.PerformUser
        JOIN SHCostcenter CC ON CC.CostCenter = AU.costcenter AND CC.Active = TRUE
        WHERE T.IsActive = TRUE
        
        UNION ALL
        
        SELECT
            T.Id,
            R.RequestDetaiId,
            T.TaskName,
            CC.CostCenter,
            CC.CostCenterName,
            (
                SELECT COUNT(TaskAssignmentId)
                FROM SHTaskDeadline
                WHERE TaskAssignmentId = T.Id AND IsAuthorized = TRUE
            ) AS DeadlineNumber,
            T.StartDate AS FromDateT,
            T.CompletedDate,
            R.ToDateWF AS ToDateWF,
            R.ToDateDL
        FROM SHTaskAssignment T
        INNER JOIN Tmp_RequestPlanDetail R ON UPPER(T.frkParentId) = UPPER(R.TaskId)
        JOIN AppUsers AU ON AU.UserName = T.PerformUser
        JOIN SHCostcenter CC ON CC.CostCenter = AU.costcenter AND CC.Active = TRUE
        WHERE T.IsActive = TRUE
    ),
    
    -- 3. CTE cho Efficiency (EF) - Đánh giá hiệu quả
    EfficiencyData AS (
        SELECT
            T.RequestDetaiId, T.CostCenter, T.CostCenterName,
            COUNT(T.TaskName) AS NumT,
            SUM(CASE WHEN DATE_DIFF('day', T.FromDateT, T.ToDateDL) = 0 THEN 1 ELSE DATE_DIFF('day', T.FromDateT, T.ToDateDL) END) AS SNumDayWF,
            SUM(
                CASE
                    -- Tính số ngày vượt deadline
                    WHEN T.CompletedDate IS NULL AND DATE_DIFF('day', T.FromDateT, T.ToDateDL) = 0 THEN 1
                    WHEN T.CompletedDate IS NULL THEN DATE_DIFF('day', T.FromDateT, T.ToDateDL) -- Nếu chưa hoàn thành, tính là vượt toàn bộ thời gian dự kiến
                    ELSE (
                        CASE
                            WHEN DATE_DIFF('day', T.FromDateT, T.CompletedDate) - DATE_DIFF('day', T.FromDateT, T.ToDateDL) <= 0 THEN 0
                            ELSE DATE_DIFF('day', T.FromDateT, T.CompletedDate) - DATE_DIFF('day', T.FromDateT, T.ToDateDL)
                        END
                    )
                END
            ) AS SNumExceeded
        FROM TaskInforTemp T
        GROUP BY 1, 2, 3
    )

-- 4. FINAL SELECT
SELECT
    -- 1. Cột Metadata Incremental
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    RD.Id,
    RD.Costcenter,
    RD.RequestNumber,
    RD.RequestDate,
    C.CostCenterName,
    DT.Id AS DetaildId,
    DT.ItemCode,
    DT.ItemName,
    DT.IndustryCode,
    DT.PM,
    DT.ItemType,
    (CASE WHEN DT.Segmentation = 'LOW' THEN 'Thấp' WHEN DT.Segmentation = 'MID' THEN 'Trung' ELSE 'Cao' END) AS SegmentationName,
    (CASE WHEN DT.ItemType = 'MANUFACTURE' THEN 'Hàng sản xuất' ELSE 'Hàng OEM' END) AS ItemTypeName,
    COALESCE(TF.StartDate, DT.TargetFromDate) AS TargetFromDate,
    DT.TargetToDate,
    DT.ExtendDate,
    CAST(COALESCE(K.NREJECT, 0) + COALESCE(K.NAPPROVAL, 0) AS VARCHAR) || '/' || CAST(COALESCE(K.NTOTAL, 0) AS VARCHAR) AS StatusDetail,
    PM.Id AS RDPlanId,
    PM.RDPlanNumber,
    COALESCE(PM.PlanStatus, 'WAITSEND') AS RequestStatus,
    DI.FirsStockDate,
    DI.ItemName AS ItemNameErp,
    COALESCE(HD.NumHistoryDeadline, 0) AS NumHistoryDeadline,
    TI.CostCenter AS DLCosCenter,
    TI.DeadlineNumber,
    EF.CostCenter AS EFCosCenter,
    EF.CostCenterName AS EFCostCenterName,
    COALESCE(EF.NumT, 0) AS EFNumT,
    
    -- PercentEfficiency
    ABS(
        COALESCE(
            1.0 - ROUND(
                CAST(EF.SNumExceeded AS DOUBLE) / NULLIF(CAST(EF.SNumDayWF AS DOUBLE) + CAST(EF.SNumExceeded AS DOUBLE), 0), 2
            ), 1.0
        )
    ) * 100 AS PercentEfficiency,
    
    CI.ClassName,
    RD.ModifyDate AS MasterModifyDate, -- Dùng cho lọc Incremental
    DT.ModifyDate AS DetailModifyDate -- Dùng cho lọc Incremental

FROM
    RD_RequestMaster RD
    LEFT JOIN RD_RequestDetail DT ON DT.MasterId = RD.Id AND DT.Active = TRUE
    LEFT JOIN RD_RequestPlanMaster PM ON DT.Id = PM.RequestDetaiId
    INNER JOIN SHCostcenter C ON C.CostCenter = RD.Costcenter
    LEFT JOIN MDataItems DI ON DT.ItemCode = DI.ItemCode AND DT.ItemCode IS NOT NULL

    -- OUTER APPLY (K)
    LEFT JOIN LATERAL (
        SELECT
            I.ProcessId, COUNT(I.Id) AS NTOTAL, SUM(CASE WHEN I.ActionKey = 'REJECT' THEN 1 ELSE 0 END) AS NREJECT, SUM(CASE WHEN I.ActionKey = 'APPROVAL' THEN 1 ELSE 0 END) AS NAPPROVAL
        FROM SHWorkflowProcessUser I
        WHERE I.WFCode = 'YEU_CAU_PHAT_TRIEN_SAN_PHAM' AND I.ProcessLevel <> 3 AND I.ProcessId = PM.Id GROUP BY I.ProcessId
    ) K ON TRUE

    -- OUTER APPLY (HD)
    LEFT JOIN LATERAL (
        SELECT COUNT(RequestDetaiId) AS NumHistoryDeadline
        FROM RD_RequestDetailHistoryDeadline WHERE ExtendStatus = 'APPROVAL' AND RequestDetaiId = DT.Id GROUP BY RequestDetaiId
    ) HD ON TRUE

    -- OUTER APPLY (CI)
    LEFT JOIN LATERAL (
        SELECT IC.Description AS ClassName FROM ItemClasses IC WHERE DT.ClassCode = IC.ItemClassCode AND IC.ClassID = 1
    ) CI ON TRUE

    -- OUTER APPLY (TI)
    LEFT JOIN LATERAL (
        SELECT IT.CostCenter, SUM(IT.DeadlineNumber) AS DeadlineNumber
        FROM TaskInforTemp IT WHERE IT.RequestDetaiId = DT.Id AND IT.DeadlineNumber >= 2 GROUP BY IT.CostCenter
    ) TI ON TRUE

    -- OUTER APPLY (EF) - Thay thế bằng LEFT JOIN CTE
    LEFT JOIN EfficiencyData EF ON EF.RequestDetaiId = DT.Id

    -- OUTER APPLY (TF)
    LEFT JOIN LATERAL (
        SELECT TA.StartDate
        FROM RD_RequestPlanDetail RPD
        JOIN SHProcessTaskLink PTL ON RPD.Id = PTL.ProcessId
        JOIN SHTaskAssignment TA ON PTL.TaskId = TA.Id AND TA.IsActive = TRUE
        JOIN RD_RequestPlanMaster RPM ON RPD.PlanMasterId = RPM.Id AND RPD.Active = TRUE
        WHERE RPD.Active = TRUE AND RPM.RequestDetaiId = DT.Id ORDER BY TA.StartDate LIMIT 1
    ) TF ON TRUE

WHERE
    -- Lọc dữ liệu đang hoạt động
    RD.Active = TRUE
    AND DT.Active = TRUE
    AND PM.Active = TRUE
    AND PM.RDPlanNumber IS NOT NULL
    
    -- Lọc Incremental: Chỉ lấy các bản ghi đã được tạo hoặc sửa đổi gần đây
    {% if is_incremental() %}
        -- Lấy các bản ghi có ModifyDate lớn hơn ngày ModifyDate MAX trong bảng đích
        AND (
            RD.ModifyDate >= (SELECT MAX(t.MasterModifyDate) FROM {{ this }} t WHERE t.MasterModifyDate IS NOT NULL)
            OR DT.ModifyDate >= (SELECT MAX(t.DetailModifyDate) FROM {{ this }} t WHERE t.DetailModifyDate IS NOT NULL)
        )
    {% endif %}

ORDER BY RD.CreateDate DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}