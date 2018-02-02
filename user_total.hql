use practical_exercise_1;

INVALIDATE METADATA practical_exercise_1.user_total;
INVALIDATE METADATA practical_exercise_1.user;

INSERT INTO TABLE user_total 
select from_unixtime(unix_timestamp()) as time_ran, cast(uc.total_users as INT) as total_users, 
cast((uc.total_users- coalesce(prev.prev_total,0)) as INT) as users_added from
(select count(*) as total_users from user) uc
join 
(select max(total_users) as prev_total from user_total) prev;