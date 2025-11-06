{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        -- Unique key là Ngày ETL, CostCenter, ChainCode, và ItemCode
        unique_key=['data_date', 'costcenter', 'chaincode', 'itemcode'], 
        views_enabled=False,
        properties = {
            "partitioning": "ARRAY['data_date']"
        }
    )
}}

{% set query %}

WITH
    -- 0. Define Sources
    DMSPgCustomer_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSPgCustomer') }}),
    SH_ItemCoverTarget_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SH_ItemCoverTarget') }}),
    DMSDebtorItem_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSDebtorItem') }}),
    Cicmpy_Consolidated_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__Cicmpy_Consolidated') }}),
    DMSSellOut_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSSellOut') }}),
    MDataItems_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__MDataItems') }}),
    SHCusReg_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__SHCusReg') }}),
    AppUsers_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__AppUsers') }}),
    DMSChainCodeItemState_Source AS (SELECT * FROM {{ source('dp_warehouse_staging', 'stg_appdatashGextappdata__DMSChainCodeItemState') }}),

    -- 1. CTE: Tương đương với @tempCmp (Lấy khách hàng PG hợp lệ)
    tempCmp AS (
        SELECT DISTINCT P.cmp_wwn
        FROM DMSPgCustomer_Source P
    ),

    -- 2. CTE: Tương đương với @Temp (Lấy mục tiêu che phủ và UNPIVOT)
    Temp AS (
        SELECT 
            p.CostCenter, p.ChainCode, p.cmp_wwn, p.ItemCode, p.ItemCover, upvt.UserName, upvt.JobType
        FROM (
            SELECT 
                Costcenter, SMonth, SYear, ChainCode, cmp_wwn, ItemCode, ItemCover, GSBH, SOS, RSM, GDK
            FROM SH_ItemCoverTarget_Source T
            
            -- Lọc Incremental (Chỉ lấy mục tiêu của tháng/năm ETL)
            {% if is_incremental() %}
                AND T.SMonth = CAST(MONTH(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
                AND T.SYear = CAST(YEAR(CAST('{{ var("etl_date") }}' AS DATE)) AS INT)
            {% endif %}
        ) p 
        CROSS JOIN LATERAL (
            VALUES 
                (p.GSBH, 'GSBH'),
                (p.SOS, 'SOS'),
                (p.RSM, 'RSM'),
                (p.GDK, 'GDK')
        ) AS upvt (UserName, JobType)
    ),

    -- 3. CTE: Tương đương với Subquery (B) (Gộp DMSDebtorItem và DMSSellOut)
    B AS (
        SELECT 
            BG.CostCenter, BG.ChainCode, BG.ItemCode, I.ItemName, I.Class_01, 
            I.IndustryCode, I.UserYesNo_05, I.UserField_06, SUM(BG.CountDeb) AS CountDeb
        FROM (
            SELECT 
                I.CostCenter, C.textfield11 AS ChainCode, I.ItemCode, 
                COUNT(DISTINCT C.cmp_wwn) AS CountDeb 
            FROM DMSDebtorItem_Source I
            JOIN Cicmpy_Consolidated_Source C ON C.cmp_wwn = I.cmp_wwn
            WHERE I.Active = TRUE
            GROUP BY I.CostCenter, C.textfield11, I.ItemCode

            UNION 
            
            SELECT DISTINCT S.Costcenter, C.textfield11 AS ChainCode, S.ItemCode, 0 AS CountDeb
            FROM DMSSellOut_Source S
            JOIN Cicmpy_Consolidated_Source C ON C.cmp_wwn = S.cmp_wwn
        ) BG
        JOIN MDataItems_Source I ON I.ItemCode = BG.ItemCode
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ),

    -- 4. CTE: Tương đương Subquery (R) (Phủ thực hiện của PC)
    R AS (
        SELECT 
            D.ChainCode, D.Costcenter, D.ItemCode,
            SUM(D.TotalNumCoverPG) AS TotalNumCoverPG, SUM(D.CurTotalNumCoverPG) AS CurTotalNumCoverPG,
            SUM(CASE WHEN D.siz_code = 'LARGE' THEN D.TotalNumCoverPG ELSE 0 END) AS TotalNumCoverPG_Large,
            SUM(CASE WHEN D.siz_code = 'MEDIUM' THEN D.TotalNumCoverPG ELSE 0 END) AS TotalNumCoverPG_Medium,
            SUM(CASE WHEN D.siz_code = 'SMALL' THEN D.TotalNumCoverPG ELSE 0 END) AS TotalNumCoverPG_Small,
            SUM(CASE WHEN D.siz_code = 'LARGE' THEN D.TotalNumCoverPG * 3 WHEN D.siz_code = 'MEDIUM' THEN D.TotalNumCoverPG * 2 WHEN D.siz_code = 'SMALL' THEN D.TotalNumCoverPG ELSE 0 END) AS TotalNumCoverPG_Commute
        FROM (
            SELECT 
                P.Costcenter, C.siz_code, C.textfield11 AS ChainCode, P.ItemCode, 
                SUM(CASE WHEN P.ShowroomStock > 0 THEN 1 ELSE 0 END) AS TotalNumCoverPG,
                SUM(CASE WHEN P.CurShowRoomStock > 0 THEN 1 ELSE 0 END) AS CurTotalNumCoverPG
            FROM (
                SELECT 
                    S.ItemCode, S.cmp_wwn, S.Costcenter, S.ShowroomStock,
                    -- Giả định CurShowRoomStock là giá trị mới nhất (không cần subquery trong Trino)
                    S.ShowroomStock AS CurShowRoomStock 
                FROM DMSSellOut_Source S
                WHERE S.ShowroomStock > 0
                  AND EXISTS(SELECT 1 FROM tempCmp P WHERE p.cmp_wwn = S.cmp_wwn) 
                -- Lọc Incremental cho DMSSellOut (Nếu cần, nhưng DMSSellOut thường là view snapshot)
                GROUP BY S.ItemCode, S.cmp_wwn, S.Costcenter, S.ShowroomStock
            ) P
            JOIN Cicmpy_Consolidated_Source C ON C.cmp_wwn = P.cmp_wwn
            GROUP BY P.Costcenter, C.siz_code, C.textfield11, P.ItemCode
        ) D
        GROUP BY D.Costcenter, D.ItemCode, D.ChainCode
    ),

    -- 5. CTE: Tương đương Subquery (N) (Phủ kế hoạch ở điểm có PC)
    N AS (
        SELECT 
            C.textfield11 AS ChainCode, DI.costcenter, DI.ItemCode,
            COUNT(DI.cmp_wwn) AS TotalNumDebPG,
            SUM(CASE WHEN C.siz_code = 'LARGE' THEN 1 ELSE 0 END) AS TotalNumDebPG_Large,
            SUM(CASE WHEN C.siz_code = 'MEDIUM' THEN 1 ELSE 0 END) AS TotalNumDebPG_Medium,
            SUM(CASE WHEN C.siz_code = 'SMALL' THEN 1 ELSE 0 END) AS TotalNumDebPG_Small,
            SUM(CASE WHEN C.siz_code = 'LARGE' THEN 3 WHEN C.siz_code = 'MEDIUM' THEN 2 WHEN C.siz_code = 'SMALL' THEN 1 ELSE 0 END) AS TotalNumDebPG_Commute 
        FROM DMSDebtorItem_Source DI
        JOIN Cicmpy_Consolidated_Source C ON C.cmp_wwn = DI.cmp_wwn
        JOIN tempCmp D ON D.cmp_wwn = DI.cmp_wwn 
        WHERE DI.Active = TRUE
        GROUP BY 1, 2, 3
    ),
    
    -- 6. CTE: Tương đương Subquery (Z) (Tổng số điểm của chuỗi)
    Z AS (
        SELECT 
            G.CostCenter, G.ChainCode, COUNT(G.cmp_wwn) AS NumDebChain
        FROM (
            SELECT DISTINCT R.CostCenter, C.textfield11 AS ChainCode, C.cmp_wwn 
            FROM Cicmpy_Consolidated_Source C
            JOIN SHCusReg_Source R ON R.cmp_wwn = C.cmp_wwn
        ) G
        WHERE G.ChainCode IS NOT NULL
        GROUP BY 1, 2
    ),

    -- 7. CTE: Tương đương Subquery (K) (Tổng số điểm có PC)
    K AS (
        SELECT 
            V.costcenter, V.ChainCode, COUNT(V.cmp_wwn) AS NumDebPG
        FROM (
            SELECT DISTINCT U.costcenter, C.textfield11 AS ChainCode, P.cmp_wwn
            FROM DMSPgCustomer_Source P
            JOIN AppUsers_Source U ON P.PGCode = U.UserName
            JOIN Cicmpy_Consolidated_Source C ON C.cmp_wwn = P.cmp_wwn
        ) V
        WHERE V.ChainCode IS NOT NULL
        GROUP BY 1, 2
    )

-- 8. SELECT cuối cùng
SELECT 
    -- 1. Cột Metadata Incremental
    CAST('{{ var("etl_date") }}' AS DATE) AS data_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm,

    -- 2. Các cột dữ liệu
    B.CostCenter, B.ChainCode, B.ItemCode, REPLACE(B.ItemName, '"', '') AS ItemName,
    B.Class_01, B.IndustryCode,
    (CASE WHEN B.UserYesNo_05 = TRUE THEN 'OFF' ELSE B.UserField_06 END) AS UserField_06,
    COALESCE(B.CountDeb, 0) AS NumDeb,
    CAST(COALESCE(R.TotalNumCoverPG, 0) AS DOUBLE) AS TotalNumCoverPG,
    CAST(COALESCE(R.CurTotalNumCoverPG, 0) AS DOUBLE) AS CurTotalNumCoverPG,
    CAST(COALESCE(R.TotalNumCoverPG_Large, 0) AS DOUBLE) AS TotalNumCoverPG_Large,
    CAST(COALESCE(R.TotalNumCoverPG_Medium, 0) AS DOUBLE) AS TotalNumCoverPG_Medium,
    CAST(COALESCE(R.TotalNumCoverPG_Small, 0) AS DOUBLE) AS TotalNumCoverPG_Small,
    CAST(COALESCE(R.TotalNumCoverPG_Commute, 0) AS DOUBLE) AS TotalNumCoverPG_Commute,
    CAST(COALESCE(N.TotalNumDebPG, 0) AS DOUBLE) AS TotalNumDebPG,
    CAST(COALESCE(N.TotalNumDebPG_Large, 0) AS DOUBLE) AS TotalNumDebPG_Large,
    CAST(COALESCE(N.TotalNumDebPG_Medium, 0) AS DOUBLE) AS TotalNumDebPG_Medium,
    CAST(COALESCE(N.TotalNumDebPG_Small, 0) AS DOUBLE) AS TotalNumDebPG_Small,
    CAST(COALESCE(N.TotalNumDebPG_Commute, 0) AS DOUBLE) AS TotalNumDebPG_Commute,
    CAST(COALESCE(Z.NumDebChain, 0) AS INTEGER) AS NumDebChain,
    CAST(COALESCE(K.NumDebPG, 0) AS INTEGER) AS NumDebPG,
    CAST(COALESCE(R.TotalNumCoverPG, 0) AS DOUBLE) AS NumCover,
    E.ItemState,
    -- Tính KPITargetCover
    (SELECT SUM(T.ItemCover) 
     FROM Temp t 
     WHERE T.ItemCode = B.ItemCode 
       AND T.Costcenter = B.CostCenter 
       AND T.ChainCode = COALESCE(B.ChainCode, '')
    ) AS KPITargetCover,

    -- CỘT "COVER"
    B.CostCenter AS CostCenter_Cover, B.ChainCode AS ChainCode_Cover, B.IndustryCode AS IndustryCode_Cover, B.Class_01 AS ItemClass_Cover, B.ItemCode AS ItemCode_Cover

FROM B
LEFT JOIN R ON R.Costcenter = B.Costcenter AND R.ChainCode = B.ChainCode AND R.ItemCode = B.ItemCode
LEFT JOIN N ON N.ChainCode = B.ChainCode AND N.costcenter = B.CostCenter AND N.ItemCode = B.ItemCode
LEFT JOIN Z ON Z.ChainCode = B.ChainCode AND Z.Costcenter = B.CostCenter
LEFT JOIN K ON K.ChainCode = B.ChainCode AND K.costcenter = B.CostCenter
LEFT JOIN DMSChainCodeItemState_Source E ON E.CostCenter = B.costcenter AND E.ChainCode = B.ChainCode AND E.ItemCode = B.ItemCode
ORDER BY 
    B.CostCenter, B.ChainCode, B.ItemCode

{% endset %}

{% if is_incremental() %}
    {{ query }}
{% else %}
    {{ query }}
{% endif %}