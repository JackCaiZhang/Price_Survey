-- 结果回收表更新触发器：更新“update_time”字段值
CREATE TRIGGER update_result_recycle
    ON result_recycle
    AFTER UPDATE
    AS
BEGIN
    UPDATE result_recycle
    SET update_time = GETDATE()
    FROM inserted
    WHERE result_recycle.city_id = inserted.city_id
      AND result_recycle.project_id = inserted.project_id
      AND result_recycle.property_type = inserted.property_type
      AND result_recycle.person_in_charge = inserted.person_in_charge;
END


-- 价格字典表更新触发器：更新“update_time”字段值
CREATE TRIGGER update_price_dict
    ON price_dict
    AFTER UPDATE
    AS
BEGIN
    UPDATE price_dict
    SET update_time = GETDATE()
    FROM inserted
    WHERE price_dict.city_id = inserted.city_id
      AND price_dict.sProperty_id = inserted.sProperty_id
      AND price_dict.app_date = inserted.app_date
      AND price_dict.property_type_new = inserted.property_type_new;
END