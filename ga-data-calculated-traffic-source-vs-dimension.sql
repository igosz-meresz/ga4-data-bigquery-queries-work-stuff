-- subquery to set the start and end date once for the whole query
with date_range as (
select
    '20220210' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date),

-- subquery to prepare and calculate traffic source data based on user and session id
traffic as (
select
    user_pseudo_id,
    session_id,
    session_number,
    concat(source,' / ',medium) as source_medium_session,
    case when campaign_session is null then '(direct)' else campaign_session end as campaign_session,
    full_referrer,
    -- definitions of the channel grouping based on the source / medium of every session
    case
        when source = '(direct)' and (medium = '(not set)' or medium = '(none)') then 'Direct'
        when medium = 'organic' then 'Organic Search'
        when regexp_contains(medium, r'^(social|social-network|social-media|sm|social network|social media)$') then 'Social'
        when medium = 'email' then 'Email'
        when medium = 'affiliate' then 'Affiliates'
        when medium = 'referral' then 'Referral'
        when regexp_contains(medium, r'^(cpc|ppc|paidsearch)$') then 'Paid Search'
        when regexp_contains(medium, r' ^(cpv|cpa|cpp|content-text)$') then 'Other Advertising'
        when regexp_contains(medium, r'^(display|cpm|banner)$') then 'Display'
        else '(Other)' end as default_channel_grouping_session,
    source_medium_user,
    campaign_user,
    default_channel_grouping_user
from (
    select
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'ga_session_id') as session_id,
        (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'ga_session_number') as session_number,
        event_timestamp,
        rank() over (partition by user_pseudo_id, (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'ga_session_id') order by event_timestamp) as rank,
        case when (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'source') is null then '(direct)' else (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'source') end as source,
        case when (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'medium') is null then '(none)' else (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'medium') end as medium,
        (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'campaign') as campaign_session,
        (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_referrer') as full_referrer,
        concat(traffic_source.source,' / ',traffic_source.medium) as source_medium_user,
        traffic_source.name as campaign_user,
        -- definitions of the channel grouping based on the source / medium of a user's first session
        case
            when traffic_source.source = '(direct)' and (traffic_source.medium = '(not set)' or traffic_source.medium = '(none)') then 'Direct'
            when traffic_source.medium = 'organic' then 'Organic search'
            when regexp_contains(traffic_source.medium, r'^(social|social-network|social-media|sm|social network|social media)$') then 'Social'
            when traffic_source.medium = 'email' then 'Email'
            when traffic_source.medium = 'affiliate' then 'Affiliates'
            when traffic_source.medium = 'referral' then 'Referral'
            when regexp_contains(traffic_source.medium, r'^(cpc|ppc|paidsearch)$') then 'Paid Search'
            when regexp_contains(traffic_source.medium, r' ^(cpv|cpa|cpp|content-text)$') then 'Other Advertising'
            when regexp_contains(traffic_source.medium, r'^(display|cpm|banner)$') then 'Display'
            else '(Other)' end as default_channel_grouping_user
    from
        `ga4-data-344909.analytics_**********.events_*`,
        date_range
    where
        _table_suffix between date_range.start_date and date_range.end_date
        and (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'ga_session_id') is not null)
where
    rank = 1)

-- main query
select
    -- user default channel grouping (dimension | the channel group associated with an user's first session)
    default_channel_grouping_user,
    -- user source / medium (dimension | the referral source and type associated with an user's first session)
    source_medium_user,
    -- user campaign (dimension | the value of a campaign associated with an user's first session)
    campaign_user,
    -- session default channel grouping (dimension | the channel group associated with a session)
    default_channel_grouping_session,
    -- session source / medium (dimension | the referral source and type associated with a session)
    source_medium_session,
    -- session campaign (dimension | the value of a campaign associated with a session)
    campaign_session,
    -- session full referrer (dimension | the full referring url of a session, including the hostname and path)
    full_referrer
from
    traffic