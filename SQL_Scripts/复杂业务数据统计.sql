-- 按“城市、项目名称、物业类型”统计最近3个月的调研价格
WITH RecentData AS (SELECT city,
                           project_name,
                           property_type,
                           data_month,
                           admitted_price,
                           ROW_NUMBER() OVER (PARTITION BY city, project_name,
                               property_type ORDER BY data_month DESC) AS rn
                    FROM result_recycle
                    WHERE data_dept = N'住宅'
                      AND data_month <> '2024-10'
                      AND admitted_price IS NOT NULL),
     FilteredProject AS (SELECT city,
                                project_name,
                                property_type
                         FROM RecentData
                         WHERE rn <= 3
                         GROUP BY city, project_name, property_type
                         HAVING COUNT(*) >= 2 -- 找出至少有最近2个月价格的项目
     ),
     FinalResult AS (SELECT DISTINCT rd.city,
                                     rd.project_name,
                                     rd.property_type,
                                     rd.data_month,
                                     rd.admitted_price,
                                     MAX(rd.data_month) OVER (PARTITION BY rd.city, rd.project_name,
                                         rd.property_type) max_month
                     FROM RecentData rd
                              JOIN FilteredProject fp
                                   ON rd.city = fp.city
                                       AND rd.project_name = fp.project_name
                                       AND rd.property_type = fp.property_type
                     WHERE rd.rn <= 3)
SELECT city,
       project_name,
       property_type,
       data_month,
       admitted_price
FROM FinalResult
WHERE max_month = '2024-09'
ORDER BY city, project_name, property_type, data_month DESC;


-- 交叉调研项目价格异常统计
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
            WHERE data_month = '2024-09'
              AND is_cross = 1
              AND is_abnormal = 1) b
ON a.city = b.city AND a.project_name = b.project_name AND a.property_type = b.property_type
WHERE a.data_month = '2024-09'
  AND a.data_dept = N'住宅'
  AND a.is_cross = 1
ORDER BY a.city, new_project_name, a.person_in_charge, a.price_diff_ratio DESC ;
