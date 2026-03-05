declare excluded_dts array<date> default [date('2026-01-24'),date('2026-01-25'),date('2026-01-31'), date('2026-02-01'),date('2999-12-31')]; -- days to exclude from baseline (like big event days)
declare z_threshold float64 default 2.5;

/*
Looks back X months and averages the same day of week dates to get a baseline
Compares current period to the baseline to check for large spikes/drops in volume
*/

with txn as
    (
        select
            txn.src_system_id
            , case
                when txn.gateway_cd in ('cljvv4pluxdo', 'i3z3apzipbp7', 'wf3nj05ig027', 'o8kozk9x9qb0') then 'US'
                when txn.gateway_cd in ('p3oy7jtrnbzu', 'jt7jdrftfjyv') then 'AU'
                when txn.gateway_cd in ('ob5kihh2l5ht', 'ob5lkextdfrs') then 'LATAM'
                when txn.gateway_cd in ('qpdcpwym9258', 'qwazckp2zkjj', 't0gcwmn4afqk') then 'UK'
                when txn.gateway_cd = 'obkq1bdafejc' then 'BR'
                when txn.gateway_cd = 'inkqcylsbc8b' then 'CA'
                when txn.gateway_cd in ('sj7av2xpiqik', 'rnuns7r579dp', 'rnuoyv9lls4l', 'rnum8iuze17e', 'rnunet43fjj0', 'rnuplcrtgiuv', 'rnupcldy1szk') then 'GSA_DE_AT_CH'
                when txn.gateway_cd = 'pnpypk0ag0nv' then 'MX'
                when txn.gateway_cd = 'rs7oav8d5e0f' then 'FR'
                when txn.gateway_cd in ('qpdel7kior3j', 'qwazjh3p96l7') then 'IE'
                when txn.gateway_cd in ('reyg6kvzzovm', 'ra6ffzgs9s8q', 't0h4mkezczbd') then 'IT'
                else txn.gateway_cd
            end as gateway_country
            , txn.account_cd
            , txn.subscription_guid
            , invoice_guid
            , transaction_guid
            , trans_dt
            , trans_dt_ut
            , origin_desc
            , trans_type_desc
            , trans_status_desc
            , trans_amt
            , tax_amt
            , txn.currency_cd
            , country_cd
            , trans_gateway_type_desc
            , gateway_cd
            , gateway_error_cd
            , failure_type
            , payment_method_desc
            , cc_type_desc
            , cc_first_6_nbr
            , card_brand_nm
            , card_type_cd
            , card_level_cd
            , card_issuer_nm
            , card_issuing_country_cd
            , avs_result_cd
            , approval_desc
            , trans_msg_desc
            , reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            -- and txn.src_system_id = 115
            and txn.trans_dt >= date_add(current_date(), interval -12 month)
            and txn.trans_dt <= current_date()
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            -- and txn.cc_first_6_nbr in ('601100', '601101', '414720')
            and txn.payment_method_desc = 'Credit Card'
            -- and ifnull(avs_result_cd,'') not in ('Y','X','V') -- not passed AVS
            -- and avs_result_cd = 'N' -- failed AVS

    )

, run_dates as
    (
        select run_dt
        from unnest(generate_date_array(date_sub(current_date(), interval 6 month), current_date())) as run_dt
    )

, baseline_dates as
    (
        select
            rd.run_dt
            , bd as baseline_dt
        from run_dates rd
        , unnest(generate_date_array(date_sub(rd.run_dt, interval 6 month), date_sub(rd.run_dt, interval 1 day))) as bd
        where 1=1
            and extract(dayofweek from bd) = extract(dayofweek from rd.run_dt)
    )

, full_dts as 
    (
        select
            run_dt as dt
        from baseline_dates

        union distinct

        select
            baseline_dt as dt
        from baseline_dates
    )

, unique_bins as
    (
        select distinct
            src_system_id
            , gateway_country
            , cc_first_6_nbr
        from txn
        where 1=1
    )

, bins_and_dates as
    (
        select
            ub.src_system_id
            , ub.gateway_country
            , ub.cc_first_6_nbr
            , fd.dt
        from unique_bins ub
        cross join full_dts fd
        where 1=1
    )

, daily_volume_all as
    (
        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            
            , count(distinct account_cd) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then account_cd else null end) as daily_success_ct
            , count(distinct case when trans_status_desc = 'declined' then account_cd else null end) as daily_decline_ct

            , count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as daily_success_avs_fail_ct
        from txn
        where 1=1
        group by all
    )

-- select
--     *
-- from daily_volume_all
-- where 1=1
--     and cc_first_6_nbr = '601100'
--     and src_system_id = 115
-- order by 1, 2, 3, 4 desc

, dates_with_volume as
    (
        select
            bd.src_system_id
            , bd.gateway_country
            , bd.cc_first_6_nbr
            , bd.dt

            , coalesce(dva.daily_ct, 0) as daily_ct
            , coalesce(dva.daily_success_ct, 0) as daily_success_ct
            , coalesce(dva.daily_decline_ct, 0) as daily_decline_ct

            , coalesce(dva.daily_success_avs_fail_ct, 0) as daily_success_avs_fail_ct
        from bins_and_dates bd
        left join daily_volume_all dva
            on bd.src_system_id = dva.src_system_id
            and bd.gateway_country = dva.gateway_country
            and bd.cc_first_6_nbr = dva.cc_first_6_nbr
            and bd.dt = dva.trans_dt
        where 1=1
    )

-- select
--     *
-- from dates_with_volume
-- where 1=1
--     and cc_first_6_nbr = '601100'
-- order by 1, 2, 3, 4 desc

, baseline_with_volume as
    (
        select
            dwv.src_system_id
            , dwv.gateway_country
            , dwv.cc_first_6_nbr
            , bd.run_dt
            , bd.baseline_dt -- same dow day for past x months
            , dwv.daily_ct
            , dwv.daily_success_ct
            , dwv.daily_success_avs_fail_ct
            , dwv.daily_decline_ct
        from baseline_dates bd
        left join dates_with_volume dwv
            on bd.baseline_dt = dwv.dt
        where 1=1
    )

-- select
--     *
-- from baseline_with_volume
-- where 1=1
--     and cc_first_6_nbr = '601100'
-- order by 1, 2, 3, 4 desc

, dow_baseline as
    (
        select
            src_system_id   
            , gateway_country
            , cc_first_6_nbr
            , run_dt

            , cast(avg(daily_ct) as int64) as baseline_avg_ct
            , cast(stddev_samp(daily_ct) as int64) as baseline_stddev_ct

            , cast(avg(daily_success_ct) as int64) as baseline_success_avg_ct
            , cast(stddev_samp(daily_success_ct) as int64) as baseline_success_stddev_ct

            , cast(avg(daily_decline_ct) as int64) as baseline_decline_avg_ct
            , cast(stddev_samp(daily_decline_ct) as int64) as baseline_decline_stddev_ct

            , cast(avg(daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_avg_ct
            , cast(stddev_samp(daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_stddev_ct
        from baseline_with_volume bwv
        where 1=1
            -- and dt not in (date('2026-01-24'),date('2026-01-25'),date('2026-01-31'), date('2026-02-01'),date('2026-02-28'))
            -- and dt not in unnest(excluded_dts)
        group by all
    )

-- select
--     *
-- from dow_baseline
-- where 1=1
--     and cc_first_6_nbr = '601100'
-- order by 1,2,3, 4 desc

, chg_chk as
    (
        select
            dwv.src_system_id
            , dwv.gateway_country
            , dwv.cc_first_6_nbr
            , dwv.dt as trans_dt

            , bl.baseline_avg_ct
            , bl.baseline_stddev_ct
            , dwv.daily_ct
            , cast((dwv.daily_ct - bl.baseline_avg_ct) as integer) as vol_diff
            , round((dwv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0), 2) as vol_z_score

            , bl.baseline_success_avg_ct
            , bl.baseline_success_stddev_ct
            , dwv.daily_success_ct as daily_success_ct
            , cast((dwv.daily_success_ct - bl.baseline_success_avg_ct) as integer) as success_diff
            , round((dwv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0), 2) as success_z_score

            , bl.baseline_decline_avg_ct
            , bl.baseline_decline_stddev_ct
            , dwv.daily_decline_ct
            , cast((dwv.daily_decline_ct - bl.baseline_decline_avg_ct) as integer) as decline_diff
            , round((dwv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0), 2) as decline_z_score

            , bl.baseline_success_avs_fail_avg_ct
            , bl.baseline_success_avs_fail_stddev_ct
            , dwv.daily_success_avs_fail_ct
            , cast((dwv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) as integer) as success_avs_fail_diff
            , round((dwv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0), 2) as success_avs_fail_z_score

        from dates_with_volume dwv
        join dow_baseline bl
            on dwv.src_system_id = bl.src_system_id
            and dwv.dt = bl.run_dt
            and dwv.gateway_country = bl.gateway_country
            and dwv.cc_first_6_nbr = bl.cc_first_6_nbr
        where 1=1
    )

, stacked as
    (
        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'total_volume' as flag_type
            , baseline_avg_ct
            , baseline_stddev_ct
            , daily_ct
            , vol_diff
            , vol_z_score
            , case
                when vol_z_score > 0 then 'large_increase'
                when vol_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_avg_ct >= 500
            -- and chg_flag is not null
            and abs(vol_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'decline' as flag_type
            , baseline_decline_avg_ct as baseline_avg_ct
            , baseline_decline_stddev_ct as baseline_stddev_ct
            , daily_decline_ct as daily_ct
            , decline_diff as vol_diff
            , decline_z_score as vol_z_score
            , case
                when decline_z_score > 0 then 'large_increase'
                when decline_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_decline_avg_ct >= 500
            -- and decline_chg_flag is not null
            and abs(decline_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'success' as flag_type
            , baseline_success_avg_ct as baseline_avg_ct
            , baseline_success_stddev_ct as baseline_stddev_ct
            , daily_success_ct as daily_ct
            , success_diff as vol_diff
            , success_z_score as vol_z_score
            , case
                when success_z_score > 0 then 'large_increase'
                when success_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_success_avg_ct >= 500
            -- and success_chg_flag is not null
            and abs(success_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'success_avs_fail' as flag_type
            , baseline_success_avs_fail_avg_ct as baseline_avg_ct
            , baseline_success_avs_fail_stddev_ct as baseline_stddev_ct
            , daily_success_avs_fail_ct as daily_ct
            , success_avs_fail_diff as vol_diff
            , success_avs_fail_z_score as vol_z_score
            , case
                when success_avs_fail_z_score > 0 then 'large_increase'
                when success_avs_fail_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_success_avs_fail_avg_ct >= 500
            -- and success_avs_fail_chg_flag is not null
            and abs(success_avs_fail_z_score) >= z_threshold
    )

select
    *
    , current_date as tableau_end_dt
    , date_add(current_date, interval -3 month) as tableau_start_dt
from stacked
where 1=1
    -- and cc_first_6_nbr = '601100'
    -- and src_system_id = 115
order by 1, 2, 4 desc, vol_z_score desc
