-- 按“城市、项目名称、物业类型”统计最近3个月的调研价格
WITH RecentData AS (SELECT city,
                           project_name,
                           property_type,
                           data_month,
                           admitted_price,
                           ROW_NUMBER() OVER (PARTITION BY city, project_name, property_type ORDER BY data_month DESC) AS rn
                    FROM result_recycle
                    WHERE data_dept = N'住宅'
                      AND admitted_price IS NOT NULL),
     FilteredProject AS (SELECT city,
                                project_name,
                                property_type
                         FROM RecentData
                         WHERE rn <= 3
                         GROUP BY city, project_name, property_type
                         HAVING COUNT(*) >= 2 -- 找出至少有最近2个月价格的项目
     )
SELECT DISTINCT rd.city,
                rd.project_name,
                rd.property_type,
                rd.data_month,
                rd.admitted_price
FROM RecentData rd
         JOIN FilteredProject fp
              ON rd.city = fp.city
                  AND rd.project_name = fp.project_name
                  AND rd.property_type = fp.property_type
WHERE rn <= 3
ORDER BY rd.city, rd.project_name, rd.property_type, rd.data_month DESC;