-- 工作量统计
SELECT a.person_in_charge "负责人", a.cnt "调研项目总量", (a.cnt -ISNULL(b.cnt, 0)) "有效调研项目数量"
FROM (SELECT person_in_charge, COUNT(unique_field) cnt
      FROM result_recycle
      WHERE data_month = '2024-09'
      GROUP BY person_in_charge) a
         LEFT JOIN
     (SELECT person_in_charge, COUNT(unique_field) cnt
      FROM result_recycle
      WHERE data_month = '2024-09'
        AND price_diff_ratio > 20.0
      GROUP BY person_in_charge) b
     ON a.person_in_charge = b.person_in_charge;

SELECT *
FROM result_recycle
WHERE data_month = '2024-09' AND data_dept = '指数' AND person_in_charge = '陈晨';