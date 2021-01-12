
CREATE TEMPORARY FUNCTION adamic_udtf as "match.adamic.AdamicUDTF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";
CREATE TEMPORARY FUNCTION adamic_udaf as "match.adamic.AdamicUDAF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";

CREATE TEMPORARY FUNCTION collect_udaf as "edu.wzm.hive.udaf.GenericUDAFCollect" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";


select adamic_udtf(items) as (item, item_neighbors)
from
(
    select user_id, concat_ws(',', collect_list(concat(item_id,':',uv))) as items
    from
    (
        select user_id,item_id, count(distinct user_id) over(partition by item_id) as uv
        from (select * from ml_1m_data_train limit 10000) t
    ) t
    group by user_id
) t
;


CREATE TEMPORARY FUNCTION adamic_udtf as "match.adamic.AdamicUDTF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";
CREATE TEMPORARY FUNCTION adamic_udaf as "match.adamic.AdamicUDAF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";

select split(item,':')[0] as item_id, adamic_udaf(item_neighbors, cast(split(item,':')[1] as bigint))
from
(
    select adamic_udtf(items) as (item, item_neighbors)
    from
    (
        select user_id, concat_ws(',', collect_list(concat(item_id,':',uv))) as items
        from
        (
            select *
                , count(distinct user_id) over(partition by item_id) as uv
            from
            (
                select user_id,item_id
                    , row_number() over(partition by user_id order by rand()) as seq
                from (select * from ml_1m_data_train) t
            ) t
            where seq <= 300
        ) t
        group by user_id
    ) t
) t
group by item
;