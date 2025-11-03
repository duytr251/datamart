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

WITH RECURSIVE
    -- 0. Define Sources
    SHTaskAssignment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskAssignment') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),
    
    -- 1. CTE: Loại bỏ các task là task cha của các task khác
    TaskParent (Id) AS (
        SELECT DISTINCT a.frkParentId AS Id
        FROM SHTaskAssignment_Source a
        WHERE a.IsActive = TRUE 
          AND a.TaskType = 'Project' 
          AND a.TaskStatus NOT IN ('Rejected','Fail') 
          AND a.frkParentId IS NOT NULL
    ),

    -- 2. CTE: Dữ liệu task đã được xử lý phân bổ giờ công
    TaskAssignmentTemp (Id, TaskName, PerformUser, FromDate, EndDate, frkParentId, CostcenterManagerUser, NumberWorkHour, HistoryNewProject, isTasK, CreateDate) AS (
        SELECT 
            a.Id,
            a.TaskName,
            a.PerformUser,
            a.StartDate AS FromDate,
            a.EndDate,
            a.frkParentId,
            
            -- Lấy CostCenter đầu tiên
            COALESCE(
                ELEMENT_AT(
                    FILTER(
                        SPLIT(COALESCE(a.BeneficiaryCostCenter, ''), ','),
                        x -> x <> ''
                    ), 1 
                ), 
                au.costcenter 
            ) AS CostcenterManagerUser, 

            -- Chia NumberWorkHour
            CASE 
                WHEN COALESCE(a.BeneficiaryCostCenter, '') <> '' 
                THEN ROUND(
                    a.NumberWorkHour / 
                    CAST(
                        CARDINALITY(
                            FILTER(
                                SPLIT(COALESCE(a.BeneficiaryCostCenter, ''), ','),
                                x -> x <> ''
                            )
                        ) 
                    AS DOUBLE), 2)
                ELSE a.NumberWorkHour 
            END AS NumberWorkHour,
            
            b.TaskName AS HistoryNewProject,
            TRUE AS isTasK,
            a.CreateDate
        FROM SHTaskAssignment_Source a
        LEFT JOIN SHTaskAssignment_Source b ON a.frkParentId = b.Id
        JOIN AppUsers_Source au ON a.ManagerUser = au.UserName
        JOIN AppUsers_Source pe ON a.PerformUser = pe.UserName
        
        WHERE a.IsActive = TRUE 
          AND pe.Region = 'NM'
          AND a.TaskType = 'Project'
          AND a.TaskStatus NOT IN ('Rejected','Fail') 
          AND NOT EXISTS(SELECT 1 FROM TaskParent t WHERE a.Id = t.Id)
        
          -- Lọc Incremental (chỉ lấy các task được tạo/sửa đổi gần đây)
          {% if is_incremental() %}
            AND a.CreateDate >= (SELECT MAX(t.CreateDate) FROM {{ this }} t WHERE t.CreateDate IS NOT NULL)
          {% endif %}
    ),

    -- 3. CTE Đệ quy: Ánh xạ mọi task đến task cha cấp 1 (RootId)
    TaskRootMapper (Id, RootId, RootBeneficiaryCostCenter, RootTaskStatus) AS (
        -- Anchor
        SELECT 
            a.Id,
            a.Id AS RootId,
            a.BeneficiaryCostCenter AS RootBeneficiaryCostCenter,
            a.TaskStatus AS RootTaskStatus
        FROM SHTaskAssignment_Source a
        WHERE a.frkParentId IS NULL 

        UNION ALL

        -- Recursive
        SELECT 
            c.Id,
            r.RootId,
            r.RootBeneficiaryCostCenter,
            r.RootTaskStatus
        FROM SHTaskAssignment_Source c
        JOIN TaskRootMapper r ON UPPER(c.frkParentId) = UPPER(r.Id) 
    )

-- 4. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    -- 2. Các cột dữ liệu
    T.Id,
    T.frkParentId,
    T.CostcenterManagerUser,
    T.PerformUser, 
    PU.FullName,
    T.NumberWorkHour,
    TRM.RootBeneficiaryCostCenter AS BeneficiaryCostCenter,
    TRM.RootTaskStatus AS TaskStatus,
    T.TaskName,
    T.FromDate,
    T.EndDate,
    T.HistoryNewProject,
    T.isTasK,
    tp.BeneficiaryCostCenter AS ParentBeneficiaryCostCenter,
    T.CreateDate
FROM TaskAssignmentTemp T
LEFT JOIN SHTaskAssignment_Source tp ON T.frkParentId = tp.Id AND tp.IsActive = TRUE
JOIN AppUsers_Source PU ON UPPER(T.PerformUser) = UPPER(PU.UserName)
LEFT JOIN TaskRootMapper TRM ON T.Id = TRM.Id

ORDER BY 
    T.EndDate, T.PerformUser desc

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}