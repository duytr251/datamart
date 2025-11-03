{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Dùng Master ID và Detail ID (nếu có) làm unique_key
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
    QAS_RequestMaster_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__QAS_RequestMaster') }}),
    SHWorkflowProcessUser_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHWorkflowProcessUser') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),
    QAS_RequestDetail_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__QAS_RequestDetail') }}),
    Items_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_exact101__Items') }}),
    ItemClasses_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__ItemClasses') }}),
    Cicmpy_Consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__Cicmpy_Consolidated') }}),
    QAS_ReceiveProcess_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__QAS_ReceiveProcess') }}),

    -- 1. CTE: Tính toán Trạng thái Workflow
    WorkflowStatus AS (
        SELECT 
            I.ProcessId,
            COUNT(I.Id) AS NTOTAL,
            SUM(CASE WHEN I.ActionKey = 'REJECT' THEN 1 ELSE 0 END) AS NREJECT,
            SUM(CASE WHEN I.ActionKey = 'APPROVAL' THEN 1 ELSE 0 END) AS NAPPROVAL,
            SUM(CASE WHEN I.ActionKey IN ('REJECT', 'APPROVAL') THEN 1 ELSE 0 END) AS PROCESSED
        FROM 
            SHWorkflowProcessUser_Source I
        WHERE 
            I.WFCode = 'YEU_CAU_KIEM_THU' 
            AND I.ProcessLevel = 3
        GROUP BY 
            I.ProcessId
    )

-- 2. SELECT chính
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    A.Id,
    A.FullName,
    A.Email,
    A.RequestBy,
    A.RequestDate,
    A.Approval,
    A.ApprovalBy,
    A.Note,
    A.Active,
    A.CreateDate,
    A.CreateBy,
    A.RequestNumber,
    A.NeedDate,
    B.FullName AS RequestByName,
    B.costcenter,
    -- StatusDetail
    CAST(COALESCE(K.PROCESSED, 0) AS VARCHAR) || '/' || CAST(COALESCE(K.NTOTAL, 0) AS VARCHAR) AS StatusDetail,
    -- ItemName (Sử dụng alias "Description" vì T-SQL sử dụng nó)
    (CASE 
        WHEN C.RequesType IN ('TestPeriodicItem', 'TestPeriodicPart') THEN D."Description" 
        ELSE C.ItemCode 
    END) AS ItemName,
    C.RequesType,
    C.Reigon,
    C.IndustryCode,
    E.Description AS AssomentName,
    C.PM,
    C.Model,
    (CASE 
        WHEN C.RequesType IN ('TestPeriodicItem', 'TestPeriodicPart') THEN F.cmp_name 
        ELSE C.Vendor 
    END) AS Vendor,
    C.Quantity,
    C.Description AS DetailDescription, -- Đổi tên để tránh trùng với ItemName
    C.NoteDetails,
    A.RequestStatus,
    QP.Assessment,
    A.request_mode,
    A.ModifyDate AS MasterModifyDate -- Dùng cho lọc Incremental
FROM 
    QAS_RequestMaster_Source A
    JOIN AppUsers_Source B ON B.UserName = A.RequestBy
    LEFT JOIN QAS_RequestDetail_Source C ON C.MasterId = A.Id AND C.Active = TRUE
    LEFT JOIN Items_Source D ON D.ItemCode = C.ItemCode
    LEFT JOIN ItemClasses_Source E ON E.ItemClassCode = C.Assoment AND E.ClassID = 1
    LEFT JOIN Cicmpy_Consolidated_Source F 
        ON F.cmp_code = C.Vendor 
        AND F.Division = 101 
        AND COALESCE(F.DbSource, '') = '101'
    LEFT JOIN QAS_ReceiveProcess_Source QP ON QP.MasterId = A.Id
    LEFT JOIN WorkflowStatus K ON K.ProcessId = A.Id
WHERE 
    -- Lọc dữ liệu đang hoạt động
    A.Active = TRUE
    
    -- Lọc Incremental (Chỉ lấy các bản ghi đã được tạo hoặc sửa đổi gần đây)
    {% if is_incremental() %}
        -- Lấy các bản ghi có ModifyDate lớn hơn ngày ModifyDate MAX trong bảng đích
        AND A.ModifyDate >= (SELECT MAX(t.MasterModifyDate) FROM {{ this }} t WHERE t.MasterModifyDate IS NOT NULL)
    {% endif %}

ORDER BY 
    A.CreateDate DESC

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}