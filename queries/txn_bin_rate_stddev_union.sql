declare excluded_dts array<date> default [date('2026-01-24'),date('2026-01-25'),date('2026-01-31'), date('2026-02-01'),date('2999-12-31')]; -- days to exclude from baseline (like big event days)
declare z_threshold float64 default 2.5;

declare run_mths int64 default 6;

/*
Rate-based: for each day, baseline = avg & stddev of bin's rate (share of segment txns) over all OTHER days in the period (leave-one-out).
Compares current day's rate to that baseline; no day-of-week logic.
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
            and txn.trans_dt >= date_add(current_date(), interval -run_mths*2 month)
            and txn.trans_dt <= current_date()
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            and txn.payment_method_desc = 'Credit Card'
    )

, run_dates as
    (
        select run_dt
        from unnest(generate_date_array(date_add(current_date(), interval -run_mths month), current_date())) as run_dt
    )

, baseline_dates as
    (
        select
            rd.run_dt
            , bd as baseline_dt
        from run_dates rd
        , unnest(generate_date_array(date_add(rd.run_dt, interval -run_mths month), date_sub(rd.run_dt, interval 1 day))) as bd
        where 1=1
    )

-- select
--     *
-- from baseline_dates
-- order by 1 desc, 2 desc

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

, daily_totals as
    (
        select
            src_system_id
            , gateway_country
            , trans_dt
            , count(distinct account_cd) as daily_total_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then account_cd else null end) as daily_total_success_ct
            , count(distinct case when trans_status_desc = 'declined' then account_cd else null end) as daily_total_decline_ct
            , count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as daily_total_success_avs_fail_ct
        from txn
        where 1=1
        group by 1, 2, 3
    )

, daily_bin_volume as
    (
        select
            bd.src_system_id
            , bd.gateway_country
            , bd.cc_first_6_nbr
            , bd.dt
                        
            , coalesce(count(distinct account_cd), 0) as daily_bin_ct
            , coalesce(count(distinct case when trans_status_desc in ('success', 'void') then account_cd else null end), 0) as daily_bin_success_ct
            , coalesce(count(distinct case when trans_status_desc = 'declined' then account_cd else null end), 0) as daily_bin_decline_ct

            , coalesce(count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end), 0) as daily_bin_success_avs_fail_ct
        from bins_and_dates bd
        left join txn
            on bd.src_system_id = txn.src_system_id
            and bd.gateway_country = txn.gateway_country
            and bd.cc_first_6_nbr = txn.cc_first_6_nbr
            and bd.dt = txn.trans_dt
        where 1=1
        group by all
    )

, daily_bin_rates as
    (
        select
            dbv.src_system_id
            , dbv.gateway_country
            , dbv.dt
            , dbv.cc_first_6_nbr

            , dt.daily_total_ct
            , dbv.daily_bin_ct
            , round(dbv.daily_bin_ct / nullif(dt.daily_total_ct, 0), 4) as daily_bin_rate
            
            , dt.daily_total_success_ct
            , dbv.daily_bin_success_ct
            , round(dbv.daily_bin_success_ct / nullif(dt.daily_total_success_ct, 0), 5) as daily_bin_success_rate

            , dt.daily_total_decline_ct
            , dbv.daily_bin_decline_ct
            , round(dbv.daily_bin_decline_ct / nullif(dt.daily_total_decline_ct, 0), 5) as daily_bin_decline_rate
            
            , dt.daily_total_success_avs_fail_ct
            , dbv.daily_bin_success_avs_fail_ct
            , round(dbv.daily_bin_success_avs_fail_ct / nullif(dt.daily_total_success_avs_fail_ct, 0), 4) as daily_bin_success_avs_fail_rate
        from daily_bin_volume dbv
        join daily_totals dt
            on dbv.src_system_id = dt.src_system_id
            and dbv.gateway_country = dt.gateway_country
            and dbv.dt = dt.trans_dt
        where 1=1
        group by all
    )

-- select
--     *
-- from daily_bin_rates
-- where 1=1
--     and cc_first_6_nbr = '601100'
--     and src_system_id = 115
-- order by 1, 2, 3, 4 desc

, baseline_vals as
    (
        select
            dbr.src_system_id
            , dbr.gateway_country
            , dbr.cc_first_6_nbr
            , bd.run_dt 
            , bd.baseline_dt -- list of dates over the past x months (excluding run_dt)

            , dbr.daily_total_ct
            , dbr.daily_bin_ct
            , dbr.daily_bin_rate

            , dbr.daily_total_success_ct
            , dbr.daily_bin_success_ct
            , dbr.daily_bin_success_rate
            
            , dbr.daily_total_decline_ct
            , dbr.daily_bin_decline_ct
            , dbr.daily_bin_decline_rate

            , dbr.daily_total_success_avs_fail_ct
            , dbr.daily_bin_success_avs_fail_ct
            , dbr.daily_bin_success_avs_fail_rate

        from baseline_dates bd
        left join daily_bin_rates dbr
            on bd.baseline_dt = dbr.dt
        where 1=1
    )


, rate_baseline as
    (
        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , run_dt

            , max(daily_total_ct) as daily_total_ct
            , cast(avg(daily_bin_ct) as int64) as baseline_avg_ct
            , round(avg(daily_bin_rate), 4) as baseline_avg_rate
            , round(stddev_samp(daily_bin_rate), 4) as baseline_rate_stddev

            , max(daily_total_success_ct) as daily_total_success_ct
            , cast(avg(daily_bin_success_ct) as int64) as baseline_avg_success_ct
            , round(avg(daily_bin_success_rate), 4) as baseline_avg_rate_success
            , round(stddev_samp(daily_bin_success_rate), 4) as baseline_rate_success_stddev

            , max(daily_total_decline_ct) as daily_total_decline_ct
            , cast(avg(daily_bin_decline_ct) as int64) as baseline_avg_decline_ct
            , round(avg(daily_bin_decline_rate), 4) as baseline_avg_rate_decline
            , round(stddev_samp(daily_bin_decline_rate), 4) as baseline_rate_decline_stddev

            , max(daily_total_success_avs_fail_ct) as daily_total_success_avs_fail_ct
            , cast(avg(daily_bin_success_avs_fail_ct) as int64) as baseline_avg_success_avs_fail_ct
            , round(avg(daily_bin_success_avs_fail_rate), 4) as baseline_avg_rate_success_avs_fail
            , round(stddev_samp(daily_bin_success_avs_fail_rate), 4) as baseline_rate_success_avs_fail_stddev
        from baseline_vals 
        where 1=1
            -- and (run_dt not in (select * from unnest(excluded_dts)))
        group by all
    )

, chg_chk as
    (
        select
            d.src_system_id
            , d.gateway_country
            , d.cc_first_6_nbr
            , d.dt as trans_dt

            , bl.daily_total_ct
            , bl.baseline_avg_ct
            , bl.baseline_avg_rate
            , bl.baseline_rate_stddev
            , d.daily_bin_ct
            , d.daily_bin_rate
            , round((d.daily_bin_rate - bl.baseline_avg_rate), 4) as rate_diff
            , round((d.daily_bin_rate - bl.baseline_avg_rate) / nullif(bl.baseline_rate_stddev, 0), 2) as vol_z_score

            , bl.daily_total_success_ct
            , bl.baseline_avg_success_ct
            , bl.baseline_avg_rate_success
            , bl.baseline_rate_success_stddev
            , d.daily_bin_success_ct
            , d.daily_bin_success_rate
            , round((d.daily_bin_success_rate - bl.baseline_avg_rate_success), 4) as rate_diff_success
            , round((d.daily_bin_success_rate - bl.baseline_avg_rate_success) / nullif(bl.baseline_rate_success_stddev, 0), 2) as success_z_score

            , bl.daily_total_decline_ct
            , bl.baseline_avg_decline_ct
            , bl.baseline_avg_rate_decline
            , bl.baseline_rate_decline_stddev
            , d.daily_bin_decline_ct
            , d.daily_bin_decline_rate
            , round((d.daily_bin_decline_rate - bl.baseline_avg_rate_decline), 4) as rate_diff_decline
            , round((d.daily_bin_decline_rate - bl.baseline_avg_rate_decline) / nullif(bl.baseline_rate_decline_stddev, 0), 2) as decline_z_score

            , bl.daily_total_success_avs_fail_ct
            , bl.baseline_avg_success_avs_fail_ct
            , bl.baseline_avg_rate_success_avs_fail
            , bl.baseline_rate_success_avs_fail_stddev
            , d.daily_bin_success_avs_fail_ct
            , d.daily_bin_success_avs_fail_rate
            , round((d.daily_bin_success_avs_fail_rate - bl.baseline_avg_rate_success_avs_fail), 4) as rate_diff_success_avs_fail
            , round((d.daily_bin_success_avs_fail_rate - bl.baseline_avg_rate_success_avs_fail) / nullif(bl.baseline_rate_success_avs_fail_stddev, 0), 2) as success_avs_fail_z_score

        from daily_bin_rates d
        join rate_baseline bl
            on d.src_system_id = bl.src_system_id
            and d.gateway_country = bl.gateway_country
            and d.cc_first_6_nbr = bl.cc_first_6_nbr
            and d.dt = bl.run_dt
        where 1=1
            and bl.baseline_avg_ct >= 300
    )

, stacked as
    (
        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'total_volume' as flag_type
            , daily_total_ct
            , baseline_avg_ct
            , baseline_avg_rate 
            , baseline_rate_stddev 
            , daily_bin_ct
            , daily_bin_rate
            , rate_diff 
            , vol_z_score
            , case
                when vol_z_score > 0 then 'large_increase'
                when vol_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            and abs(vol_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'success' as flag_type
            , daily_total_success_ct
            , baseline_avg_success_ct 
            , baseline_avg_rate_success 
            , baseline_rate_success_stddev 
            , daily_bin_success_ct
            , daily_bin_success_rate
            , rate_diff_success
            , success_z_score 
            , case
                when success_z_score > 0 then 'large_increase'
                when success_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            and abs(success_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'decline' as flag_type
            , daily_total_decline_ct
            , baseline_avg_decline_ct 
            , baseline_avg_rate_decline
            , baseline_rate_decline_stddev
            , daily_bin_decline_ct 
            , daily_bin_decline_rate
            , rate_diff_decline
            , decline_z_score 
            , case
                when decline_z_score > 0 then 'large_increase'
                when decline_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            and abs(decline_z_score) >= z_threshold

        union all

        select
            src_system_id
            , gateway_country
            , cc_first_6_nbr
            , trans_dt
            , 'success_avs_fail' as flag_type
            , daily_total_success_avs_fail_ct
            , baseline_avg_success_avs_fail_ct  
            , baseline_avg_rate_success_avs_fail 
            , baseline_rate_success_avs_fail_stddev 
            , daily_bin_success_avs_fail_ct
            , daily_bin_success_avs_fail_rate   
            , rate_diff_success_avs_fail
            , success_avs_fail_z_score
            , case
                when success_avs_fail_z_score > 0 then 'large_increase'
                when success_avs_fail_z_score < 0 then 'large_decrease'
                else null
            end as chg_flag
        from chg_chk
        where 1=1
            and abs(success_avs_fail_z_score) >= z_threshold
    )

select
    *
    , current_date as tableau_end_dt
    , date_add(current_date(), interval -6 month) as tableau_start_dt
from stacked
where 1=1
    and trans_dt < date(datetime(current_timestamp, 'America/Los_Angeles'))
order by 1, 2, 4 desc, vol_z_score desc
