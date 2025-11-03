{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là ngày tháng tổng hợp (MonthYear) và ngày ETL (data_date)
        unique_key=['data_date', 'monthyear'],
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
    SHProcessTaskLink_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHProcessTaskLink') }}),
    SHTaskAssignment_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__SHTaskAssignment') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashgextappdata__AppUsers') }}),
    
    -- 0.1 Logic Lọc Incremental cho bảng Task
    FilteredSHTaskAssignment AS (
        SELECT *
        FROM SHTaskAssignment_Source ta
        WHERE 
            ta.IsActive = TRUE
            AND ta.TaskStatus NOT IN ('Rejected','Fail')
            AND COALESCE(ta.BeneficiaryCostCenter, '') <> ''
            -- Lọc Incremental: Chỉ lấy các task kết thúc trong ngày ETL
            {% if is_incremental() %}
                AND ta.EndDate = CAST('{{ var("etl_date") }}' AS DATE)
            {% endif %}
    ),

    -- 1. CTE: Gom các task ban đầu
    tbTask_Fac AS (
        SELECT 
            ta.Id, ta.frkParentId, ta.TaskType, ta.NumberWorkHour, ta.BeneficiaryCostCenter, ta.EndDate, 'Link_Fac' AS TypeTask
        FROM FAC_MachineMaintainHistory_Source mmh
        JOIN SHProcessTaskLink_Source lnk ON lnk.ProcessId = mmh.Id AND lnk.ProcessTableName = 'FAC_MachineMaintainHistory'
        JOIN FilteredSHTaskAssignment ta ON ta.Id = lnk.TaskId
        WHERE mmh.IsActive = TRUE
    ),

    tbTask_Normal AS (
        SELECT 
            ta.Id, ta.frkParentId, ta.TaskType, ta.NumberWorkHour, ta.BeneficiaryCostCenter, ta.EndDate, 'Normal' AS TypeTask
        FROM FilteredSHTaskAssignment ta
        JOIN AppUsers_Source pe ON ta.PerformUser = pe.UserName
        WHERE 
            NOT EXISTS (
                SELECT 1 FROM tbTask_Fac WHERE UPPER(tbTask_Fac.Id) = UPPER(ta.Id)
            )
            AND ta.TaskType = 'Project'
            AND pe.Region = 'NM'
    ),

    -- 2. Gộp các task và tìm các task cha
    tbTask_Union AS (
        SELECT * FROM tbTask_Fac
        UNION ALL
        SELECT * FROM tbTask_Normal
    ),

    TaskParent AS (
        SELECT DISTINCT frkParentId AS Id
        FROM tbTask_Union
        WHERE TaskType = 'Project'
            AND frkParentId IS NOT NULL
    ),

    -- 3. Lọc bỏ task cha (Tương đương logic DELETE FROM #tbTask)
    tbTask_Final_Roots AS (
        SELECT t.*
        FROM tbTask_Union t
        LEFT JOIN TaskParent tp ON t.Id = tp.Id
        WHERE tp.Id IS NULL
    ),

    -- 4. CTE Đệ quy (Lấy tất cả các task con từ task gốc)
    Descendants (Id, frkParentId, TaskType, NumberWorkHour, BeneficiaryCostCenter, EndDate, TypeTask) AS ( 
        -- Anchor
        SELECT 
            Id, frkParentId, TaskType, NumberWorkHour, BeneficiaryCostCenter, EndDate, TypeTask
        FROM tbTask_Final_Roots
        WHERE COALESCE(NumberWorkHour, 0) > 0

        UNION ALL

        -- Recursive
        SELECT 
            c.Id, c.frkParentId, c.TaskType, c.NumberWorkHour, c.BeneficiaryCostCenter, c.EndDate, d.TypeTask
        FROM SHTaskAssignment_Source c -- Join ngược lại với toàn bộ bảng Task
        JOIN Descendants d ON UPPER(c.frkParentId) = UPPER(d.Id) 
        WHERE
            c.IsActive = TRUE
            AND c.TaskStatus NOT IN ('Rejected','Fail')
            AND COALESCE(c.NumberWorkHour, 0) > 0
    ),

    -- 5. Lọc duy nhất
    FilteredDescendants AS (
        SELECT DISTINCT *
        FROM Descendants
    ),

    -- 6. Tách CostCenter và tính toán
    Expanded AS (
        SELECT
            th.Id,
            th.TaskType,
            th.TypeTask,
            cent.Data AS CostCenter,
            
            CASE 
                WHEN th.TypeTask = 'Link_Fac' 
                THEN CAST(ROUND(th.NumberWorkHour / NULLIF(cc.CountCenter, 0), 2) AS DECIMAL(10,2))
                ELSE 0 
            END AS WorkshopRepairServiceHours,

            CASE 
                WHEN cent.Data <> 'NM-KTTB' -- Hard-coded @MainCostCenter
                AND th.TaskType = 'Normal' 
                THEN CAST(ROUND(th.NumberWorkHour / NULLIF(cc.CountCenter, 0), 2) AS DECIMAL(10,2)) 
                ELSE 0 
            END AS ProjectServiceHours,

            CASE 
                WHEN cent.Data = 'NM-KTTB' AND th.TaskType = 'Normal' 
                THEN CAST(ROUND(th.NumberWorkHour / NULLIF(cc.CountCenter, 0), 2) AS DECIMAL(10,2)) 
                ELSE 0 
            END AS TotalHoursMainCostCenter,
            
            -- MonthYear
            DATE_FORMAT(th.EndDate, '%m/%Y') AS MonthYear

        FROM FilteredDescendants th
        CROSS JOIN LATERAL (
            SELECT 
                SPLIT(COALESCE(th.BeneficiaryCostCenter, ''), ',') AS items
        ) s
        CROSS JOIN LATERAL (
            SELECT 
                CAST(CARDINALITY(s.items) AS DOUBLE) AS CountCenter,
                s.items
        ) cc
        CROSS JOIN UNNEST(cc.items) AS cent(Data)
        WHERE TRIM(cent.Data) <> ''
    ),

    -- 7. Tổng hợp
    Aggregated AS (
        SELECT
            MonthYear,
            COALESCE(SUM(WorkshopRepairServiceHours), 0) AS WorkshopRepairServiceHours,
            COALESCE(SUM(TotalHoursMainCostCenter), 0) AS TotalHoursMainCostCenter,
            COALESCE(SUM(ProjectServiceHours), 0) AS ProjectServiceHours
        FROM Expanded
        GROUP BY MonthYear
    )
    
-- 8. Kết quả cuối cùng
SELECT
    '{{ var("etl_date") }}' AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,
    
    'Phòng kỹ thuật thiết bị' AS CostCenterName,
    'NM-KTTB' AS Describe,
    Agg.MonthYear,
    Agg.WorkshopRepairServiceHours,
    Agg.TotalHoursMainCostCenter,
    Agg.ProjectServiceHours,
    (Agg.WorkshopRepairServiceHours + Agg.TotalHoursMainCostCenter + Agg.ProjectServiceHours) AS totalWorkHoursAllKttbStaff,
    
    CASE 
        WHEN (Agg.WorkshopRepairServiceHours + Agg.TotalHoursMainCostCenter + Agg.ProjectServiceHours) > 0 THEN
            CAST(
                (
                    (Agg.WorkshopRepairServiceHours + Agg.ProjectServiceHours) - Agg.TotalHoursMainCostCenter
                )
                / (Agg.WorkshopRepairServiceHours + Agg.TotalHoursMainCostCenter + Agg.ProjectServiceHours) * 100 
            AS INTEGER)
        ELSE 0 
    END AS actualCompletionRate
FROM Aggregated Agg
ORDER BY PARSE_DATETIME('01/' || Agg.MonthYear, 'dd/MM/yyyy')

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}