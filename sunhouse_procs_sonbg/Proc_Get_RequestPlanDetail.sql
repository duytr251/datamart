{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng StepId làm unique_key cơ bản. Thêm data_date để phù hợp với config incremental.
        unique_key=['data_date', 'stepid'],
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH RECURSIVE
    -- 0. Define Sources (Cho phép tham chiếu ngắn gọn trong CTE)
    SHTaskAssignment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskAssignment') }}),
    RDRequestPlanDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__RD_RequestPlanDetail') }}),
    SHProcessTaskLink_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHProcessTaskLink') }}),
    SHTaskDeadline_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskDeadline') }}),
    WorkflowFixStepTemplate_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__WorkflowFixStepTemplate') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),

    -- 1. CTE 1: Base Task Link
    Tmp_RequestPlanDetail (PlanDetailID, TaskId, ToDate) AS ( 
        SELECT 
            R.Id,
            P.TaskId,
            R.ToDate
        FROM 
            RDRequestPlanDetail_Source R
            INNER JOIN SHProcessTaskLink_Source P ON R.Id = P.ProcessId
        WHERE 
            P.ProcessTableName IN ('RD_Request', 'RD_RequestPlanDetail')
    ),

    -- 2. CTE 2: Recursive Task Assignment
    Tmp_Result (Id, ToDate) AS ( 
        -- Anchor Member
        SELECT 
            T.Id,
            P.ToDate
        FROM 
            SHTaskAssignment_Source T
            INNER JOIN Tmp_RequestPlanDetail P ON UPPER(T.Id) = UPPER(P.TaskId)

        UNION ALL

        -- Recursive Member
        SELECT 
            T.Id,
            R.ToDate
        FROM 
            SHTaskAssignment_Source T
            INNER JOIN Tmp_Result R ON UPPER(T.frkParentId) = UPPER(R.Id)
    )

-- 3. FINAL SELECT
SELECT DISTINCT 
    -- 1. Cột Metadata Incremental
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    T.Id AS StepId,
    (
        SELECT COUNT(TaskAssignmentId) 
        FROM SHTaskDeadline_Source 
        WHERE TaskAssignmentId = T.Id AND IsAuthorized = TRUE
    ) AS DeadlineNumber,
    T.TaskName AS StepName,
    T.TaskRequirement,
    T.PerformUser AS UserName,
    T.frkParentId AS ParentId,
    T.ManagerUser,
    T.TaskStatus,
    T.AutoId,
    T.Priority,
    T.StartDate AS FromDate,
    T.EndDate AS ToDate,
    TR.ToDate AS ToDateWF,
    T.CreateDate,
    T.CreateBy,
    T.CompletedDate,
    DATE_DIFF('day', T.StartDate, T.EndDate) AS TotalDays,
    DATE_DIFF('day', T.StartDate, CURRENT_DATE) AS ToCurrentDays,
    T.IsCompleted,
    F.ProjectPhase,
    S.costcenter,
    CASE 
        WHEN T.IsCompleted = TRUE THEN DATE_DIFF('day', T.StartDate, T.CompletedDate)
        ELSE 0 
    END AS ToCompleteDays,
    T.ModifyDate AS TaskModifyDate -- Dùng cho lọc Incremental

FROM 
    SHTaskAssignment_Source T
INNER JOIN Tmp_Result TR ON TR.Id = T.Id
LEFT JOIN SHProcessTaskLink_Source L ON L.TaskId = T.Id
LEFT JOIN RDRequestPlanDetail_Source R ON R.Id = L.ProcessId
LEFT JOIN WorkflowFixStepTemplate_Source F ON F.Id = R.StepId
JOIN AppUsers_Source S ON S.UserName = T.PerformUser
WHERE 
    R.Active = TRUE
    
    -- Lọc Incremental (Dựa trên ModifyDate của bảng Task chính)
    {% if is_incremental() %}
        AND T.ModifyDate >= (SELECT MAX(t.TaskModifyDate) FROM {{ this }} t WHERE t.TaskModifyDate IS NOT NULL)
    {% endif %}

ORDER BY 
    ProjectPhase, FromDate
{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}