--------------------------- 任务分发表 ---------------------------
CREATE TABLE task_distribution
(
    city_type         NVARCHAR(10),           -- 城市类型
    city_id           NVARCHAR(50)  NOT NULL, -- 城市ID
    city              NVARCHAR(30),           -- 城市
    data_month        NVARCHAR(30)  NOT NULL, -- 数据日期（月份）
    project_id        NVARCHAR(100),          -- 项目id
    project_name      NVARCHAR(100),          -- 项目名称
    newhouse_name     NVARCHAR(500),          -- 新房名称
    newhouse_id       NVARCHAR(2000),         -- 新房id
    newcode           NVARCHAR(2000),         -- 新房newcode
    property_type_num INT,                    -- 物业类型套数
    total_num         INT,                    -- 总套数
    property_type     NVARCHAR(20)  NOT NULL, -- 物业类型
    city_ranking      INT,                    -- 城市排名
    person_in_charge  NVARCHAR(20)  NOT NULL, -- 负责人
    return_date       DATE,                   -- 回收日期
    is_cross          INT,                    -- 是否交叉调研
    data_dept         NVARCHAR(20),           -- 数据部门（住宅/指数）
    distribute_date   DATE,                   -- 任务分发时间
    unique_field      NVARCHAR(3000),         -- 唯一主键=city_id+data_month+project_id/newcode+property_type+person_in_charge
    PRIMARY KEY (unique_field)
);

--------------------------- 结果回收表 ---------------------------
CREATE TABLE result_recycle
(
    city_id                 NVARCHAR(50)  NOT NULL, -- 城市ID
    city                    NVARCHAR(30),           -- 城市
    data_month              NVARCHAR(30)  NOT NULL, -- 数据日期（月份）
    project_id              NVARCHAR(100),          -- 项目id
    project_name            NVARCHAR(100),          -- 项目名称
    newhouse_name           NVARCHAR(500),          -- 新房名称
    newhouse_id             NVARCHAR(2000),         -- 新房id
    newcode                 NVARCHAR(2000),         -- 新房newcode
    property_type_num       INT,                    -- 物业类型套数
    total_num               INT,                    -- 总套数
    property_type           NVARCHAR(20)  NOT NULL, -- 物业类型
    city_ranking            INT,                    -- 城市排名
    decoration_price        DECIMAL(18, 2),         -- 精装价格
    rough_price             DECIMAL(18, 2),         -- 毛坯价格
    decoration_of_main_sale NVARCHAR(20),           -- 主力在售装修情况
    chain_ratio             DECIMAL(6, 2),          -- 环比（当月价格较上月变动）
    discount                NVARCHAR(100),          -- 优惠情况
    sale_status             NVARCHAR(50),           -- 销售状态
    comment                 NVARCHAR(200),          -- 备注
    person_in_charge        NVARCHAR(20)  NOT NULL, -- 负责人
    return_date             DATE,                   -- 回收日期
    survey_method           NVARCHAR(50),           -- 调研方式
    is_cross                INT,                    -- 是否交叉调研（1--是；0--否）
    data_dept               NVARCHAR(20),           -- 数据部门（住宅/指数）
    price_difference        DECIMAL(10, 2),         -- 价格差异
    price_diff_ratio        DECIMAL(6, 2),          -- 价格差异百分比
    is_abnormal             INT,                    -- 是否异常（1--是；0--否）
    admitted_price          DECIMAL(18, 2),         -- 采信价格
    import_date             DATE,                   -- 导入时间
    is_recheck              INT,                    -- 是否复核（1--是；0--否）
    recheck_person          NVARCHAR(20),           -- 复核人员
    recheck_price           DECIMAL(18, 2),         -- 复核价格
    update_time             DATETIME2,              -- 更新时间
    unique_field            NVARCHAR(3000),         -- 唯一主键=city_id+data_month+project_id/newcode+property_type+person_in_charge
    PRIMARY KEY (unique_field)
);

--------------------------- 标准价格字典表 ---------------------------
CREATE TABLE price_dict
(
    city              NVARCHAR(30),   -- 城市
    city_id           NVARCHAR(50),   -- 城市ID
    sProperty_name    NVARCHAR(100),  -- 总期名称
    sProperty_id      NVARCHAR(100),  -- 总期ID
    app_date          NVARCHAR(20),   -- 适用时间
    property_type_new NVARCHAR(20),   -- 细分物业类型
    admitted_price    DECIMAL(18, 2), -- 采信价格
    price_source      NVARCHAR(20),   -- 来源
    modify_type       NVARCHAR(10),   -- 修改类型
    comment           NVARCHAR(100),  -- 备注
    import_date       DATE,           -- 导入时间
    update_time       DATETIME2,      -- 更新时间
    PRIMARY KEY (city_id, sProperty_id, property_type_new, app_date)
);

SELECT CONSTRAINT_NAME
FROM INFORMATION_SCHEMTABLE_CONSTRAINTS
WHERE TABLE_NAME = 'result_recycle' AND CONSTRAINT_TYPE = 'PRIMARY KEY';

ALTER TABLE task_distribution
    ALTER COLUMN project_id NVARCHAR(100);
ALTER TABLE result_recycle
    ALTER COLUMN chain_ratio DECIMAL(6, 2);

UPDATE task_distribution
SET unique_field = CONCAT(city_id, data_month, project_id, property_type, person_in_charge)
WHERE 1 = 1;
UPDATE result_recycle
SET unique_field = CONCAT(city_id, data_month, project_id, property_type, person_in_charge)
WHERE 1 = 1;

ALTER TABLE result_recycle
    DROP CONSTRAINT PK_result_recycle;
ALTER TABLE result_recycle
    ADD CONSTRAINT PK_result_recycle PRIMARY KEY (unique_field);


DROP TABLE price_dict;

DELETE
FROM task_distribution
WHERE distribute_date = '2024-08-19';

SELECT *
FROM task_distribution
WHERE distribute_date = '2024-08-05' AND project_name = N'华润置地·九悦';

SELECT DISTINCT a.unique_field,
                a.city,
                ISNULL(a.project_name, a.newhouse_name) new_project_name,
                a.data_month,
                a.property_type,
                a.decoration_of_main_sale,
                a.decoration_price,
                a.rough_price,
                a.admitted_price,
                a.price_difference,
                a.price_diff_ratio,
                a.sale_status,
                a.person_in_charge,
                a.data_dept
FROM result_recycle a
INNER JOIN (SELECT city,
                   ISNULL(project_name, newhouse_name) project_name,
                   property_type
            FROM result_recycle
            WHERE data_month = '2024-08'
              AND is_cross = 1
              AND is_abnormal = 1) b
ON a.city = b.city AND a.project_name = b.project_name AND a.property_type = b.property_type
WHERE a.data_month = '2024-09'
  AND a.is_cross = 1
ORDER BY a.person_in_charge, a.price_diff_ratio DESC ;

SELECT *
FROM result_recycle
WHERE data_dept = '住宅' AND FORMAT(import_date, 'yyyy-MM') = '2024-10';

UPDATE price_dict
SET import_date = '2024-09-20'
WHERE import_date = '2024-09-23';

DELETE
FROM result_recycle
WHERE import_date = '2024-11-05';

DELETE
FROM price_dict
WHERE import_date = '2024-11-05';

SELECT *
FROM price_dict
WHERE city_id = 'ec224a51-6810-4885-8852-caaa';

DELETE
FROM price_dict
WHERE import_date = '2024-08-19';

UPDATE price_dict
SET import_date = '2024-08-05'
WHERE import_date = '2024-08-06';

-- 查询某个表的所有字段名
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMCOLUMNS
WHERE TABLE_NAME = 'result_recycle';


