use practical_exercise_1;

INVALIDATE METADATA practical_exercise_1.user;
INVALIDATE METADATA practical_exercise_1.activitylog;
INVALIDATE METADATA practical_exercise_1.user_upload_dump;


CREATE TABLE if not exists practical_exercise_1.user_report(
user_id int,
total_updates bigint,
total_inserts bigint,
total_deletes bigint,
last_activity_type string,
upload_count bigint,
is_active boolean
);

INVALIDATE METADATA practical_exercise_1.user_report;


INSERT OVERWRITE user_report select 
u.id as user_id,
COALESCE(totals.total_updates,0) as total_updates,
COALESCE(totals.total_inserts,0) as total_inserts,
COALESCE(totals.total_deletes,0) as total_deletes,
last_activity.last_activity_type,
COALESCE(uploads.upload_count,0) as upload_count,
CASE when active.is_active=1 then cast(1 as boolean) else cast(0 as boolean) end as is_active
from
user u 
left join
(
    select user_id, 
    sum(case when type = 'UPDATE' then 1 else 0 end) as total_updates,
    sum(case when type = 'DELETE' then 1 else 0 end) as total_deletes,
    sum(case when type = 'INSERT' then 1 else 0 end) as total_inserts
    from activitylog group by user_id
) totals 
on u.id = totals.user_id
left join
(
    select user_id, type as last_activity_type from 
    (select user_id, type, `timestamp`, rank() over (partition by user_id order by `timestamp` desc) as rank from activitylog) ranked_types
    WHERE ranked_types.rank = 1
) last_activity 
on u.id = last_activity.user_id
left join
(
    select user_id,count(*) as upload_count from user_upload_dump group by user_id
) uploads
on u.id = uploads.user_id
left join
(
    select user_id,
    max(case when datediff(from_unixtime(unix_timestamp()), from_unixtime(`timestamp`))<=2 then 1 else 0 end) as is_active
    from activitylog
    group by user_id
) active
on u.id = active.user_id;
