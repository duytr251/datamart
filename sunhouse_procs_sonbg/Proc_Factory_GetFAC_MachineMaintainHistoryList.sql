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
    FAC_MachineMaintainHistory_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FAC_MachineMaintainHistory') }}),
    FACMachine_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FACMachine') }}),
    SHCostcenter_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHCostcenter') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),
    SHWorkflowProcessUser_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHWorkflowProcessUser') }}),
    SHProcessTaskLink_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHProcessTaskLink') }}),
    SHTaskAssignment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskAssignment') }}),
    FAC_MachineMaintainProces_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__FAC_MachineMaintainProces') }}),

    -- Lọc Incremental cho bảng Master
    FilteredMaster AS (
        SELECT *
        FROM FAC_MachineMaintainHistory_Source A
        WHERE A.IsActive = TRUE
        
        -- Lọc Incremental (Chỉ lấy dữ liệu mới/thay đổi)
        {% if is_incremental() %}
            -- Sử dụng CreateDate/RequestTime để lọc dữ liệu mới
            AND A.CreateDate >= (SELECT MAX(t.CreateDate) FROM {{ this }} t WHERE t.CreateDate IS NOT NULL)
        {% endif %}
    )

-- SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    A.Id,
    A.MachineId,
    A.MachineName,
    A.RequestLevel,
    A.RequestNumber,
    -- Chuyển đổi COALESCE + CASE
    (CASE WHEN COALESCE(G.TotalFail, 0) > 0 THEN 'HandOverFail' ELSE A.RequestStatus END) AS RequestStatus,
    COALESCE(J.TaskRate, 0) AS TaskRate,
    A.RequestType,
    A.RequestKind,
    A.ApplyDate,
    A.RequestTime,
    A.NeedCompletedTime,
    A.FromCostcenter,
    A.FromUserName,
    A.ToUserName,
    A.ToCostcenter,
    A.RequestApproval,
    A.RequestApprovalBy,
    A.RequestApprovalDate,
    A.PhenomenonDescription,
    A.AnalyzeReason,
    A.HandlingSolution,
    A.EstimatedCompletionTime,
    A.RealityCompletedTime,
    A.RealityCompletedConfirm,
    A.ReasonGroups,
    A.PerformRemediation,
    A.RepairNotes,
    A.CostMaterial,
    A.CostManHour,
    A.CostOther,
    A.Quantity,
    A.UnitName,
    A.MachineStatus,
    A.NoteAterCompleted,
    A.OtherNote,
    A.DirectorNote,
    A.DirectorNoteBy,
    A.DirectorApproved,
    A.UsedConfirm,
    A.UsedRate,
    A.UsedNote,
    A.UsedConfirmBy,
    A.UsedConfirmDate,
    A.MachManApproved,
    A.MachManApprovedBy,
    A.MachManApprovedDate,
    A.HandOverApproved,
    A.HandOverApprovedBy,
    A.HandOverApprovedDate,
    A.IsActive,
    A.CreateDate,
    A.CreateBy,
    D.SerialNo,
    D.MachineName AS D_MachineName,
    COALESCE(G.TotalTask, 0) AS TotalTask,
    Sc.CostCenterName AS ToCostcenterName,
    ap.FullName AS UsedNameConfirmBy,
    COALESCE(I.TotalRecordingDiary, 0) AS TotalRecordingDiary,
    COALESCE(P.TotalPerson, 0) AS TotalPerson,
    COALESCE(P.PersonAction, 0) AS PersonAction,
    -- ProcessLevelName
    (CASE 
        WHEN K.ActionKey IS NULL THEN 'Kết thúc '
        WHEN K.ActionKey = 'WAIT' THEN 'Chờ ' || K.ProcessLevelName
        WHEN K.ActionKey = 'REJECT' THEN K.ProcessLevelName || ' từ chối' 
        ELSE 'Không xác định'
    END) AS ProcessLevelName,
    COALESCE(K.ProcessLevel, P.TotalPerson) AS ProcessLevel,
    COALESCE(K.ActionKey, 'APPROVAL') AS ActionKey,
    A.BeneficiaryCostCenter
FROM 
    FilteredMaster A
LEFT JOIN FACMachine_Source D ON A.MachineId = D.Id
LEFT JOIN SHCostcenter_Source sc ON A.ToCostcenter = SC.CostCenter
LEFT JOIN AppUsers_Source ap ON ap.UserName = A.UsedConfirmBy

-- P (Workflow Action Count)
LEFT JOIN LATERAL (
    SELECT 
        SUM(CASE WHEN S.ActionKey <> 'WAIT' THEN 1 ELSE 0 END) AS PersonAction,
        COUNT(S.ProcessId) AS TotalPerson 
    FROM SHWorkflowProcessUser_Source S 
    WHERE S.ProcessId = A.Id AND S.WFCode = 'BAO_CAO_SU_CO_TB'
    GROUP BY S.ProcessId -- Cần GROUP BY cho hàm COUNT/SUM
) P ON TRUE

-- K (Latest Wait/Reject Status)
LEFT JOIN LATERAL (
    SELECT U.ProcessLevelName, U.ProcessLevel, U.ActionKey 
    FROM SHWorkflowProcessUser_Source U 
    WHERE U.ActionKey IN ('WAIT','REJECT') AND U.ProcessId = A.Id AND U.WFCode = 'BAO_CAO_SU_CO_TB' 
    ORDER BY U.ProcessLevel ASC 
    LIMIT 1
) K ON TRUE

-- G (Task Count/Fail Count)
LEFT JOIN LATERAL (
    SELECT 
        COUNT(P.ProcessId) AS TotalTask,
        SUM(CASE WHEN T.TaskStatus = 'Fail' THEN 1 ELSE 0 END) AS TotalFail
    FROM SHProcessTaskLink_Source P
    JOIN SHTaskAssignment_Source T ON T.Id = P.TaskId
    WHERE P.ProcessId = CAST(A.Id AS VARCHAR)
    GROUP BY P.ProcessId -- Cần GROUP BY cho hàm COUNT/SUM
) G ON TRUE

-- I (Recording Diary Count)
LEFT JOIN LATERAL (
    SELECT COUNT(Id) AS TotalRecordingDiary
    FROM FAC_MachineMaintainProces_Source P
    WHERE A.Id = P.FMMHId
    GROUP BY P.FMMHId -- Cần GROUP BY cho hàm COUNT
) I ON TRUE

-- J (Task Rate)
LEFT JOIN LATERAL (
    SELECT T.TaskRate
    FROM SHProcessTaskLink_Source P
    JOIN SHTaskAssignment_Source T ON T.Id = P.TaskId
    WHERE P.ProcessId = CAST(A.Id AS VARCHAR) AND T.IsActive = TRUE
    ORDER BY T.CreateDate ASC
    LIMIT 1
) J ON TRUE

ORDER BY 
    A.RequestTime DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}