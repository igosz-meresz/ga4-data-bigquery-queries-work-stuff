-- subquery to define static/or dynamic start and end date for the whole query
with date_range as (
    select
        '20220210' as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
),

-- subquery to prepare and calculate user data
user as(
    select
        user_pseudo_id,
        case
            when (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') = 1 then 'new visitor'
            when (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') > 1 then 'returning visitor'
            else null end as user_type,
        (select value.int_value from unnest(event_params) where event_name = 'sessions_start' and key = 'ga_session_number') as count_of_sessions,
        count(distinct user_pseudo_id) as users,
        count(distinct case when (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') = 1 then user_pseudo_id else null end) as new_users,
        count(distinct case when event_name = 'session_start' and (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') = 1 then concat(user_pseudo_id, cast(event_timestamp as string)) end) / count(distinct case when event_name = 'session_start' then concat(user_pseudo_id, cast(event_timestamp as string)) end) as percentage_new_sessions,
        count(distinct case when event_name = 'session_start' then concat(user_pseudo_id, cast(event_timestamp as string)) end) / count(distinct user_pseudo_id) as number_of_sessions_per_user
    from
        `ga4-data-344909.analytics_**********.events_*`,
        date_range
    where
        _table_suffix between date_range.start_date and date_range.end_date
    group by
        user_pseudo_id,
        user_type,
        count_of_sessions
)

-- main query
select
    -- user type (dimension | a boolean, either new visitor or returning)
    user_type,
    -- count of sessions (dimension | the session index for a user, each session from a unique user will get its own incremental index starting from 1 for the first session)
    count_of_sessions,
    -- user (metric | total number of active users)
    sum(users) as users,
    -- new_users (metric | the number of users who interacted with your site for the first time)
    sum(new_users) as new_users,
    -- % new sessions (metric | the percentage of sessions by users who had never visited before)
    avg(percentage_new_sessions) as percentage_new_sessions,
    -- number of sessions per user (metric | the total number of sessions divided by the total number of users)
    avg(number_of_sessions_per_user) as number_of_sessions_per_user
from
    user
where
    user_type is not null
group by 
    user_type,
    count_of_sessions
order by
    users desc