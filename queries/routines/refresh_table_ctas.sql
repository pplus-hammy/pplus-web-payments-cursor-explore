-- BigQuery stored procedure: full-refresh a table each run via CREATE OR REPLACE TABLE AS SELECT.
--
-- Before deploying, replace every occurrence of TARGET_TABLE with your table name
-- (e.g. dashboard_anomaly_daily) and replace the SELECT body with your source query.
-- Remove or uncomment partition/cluster lines as needed.
--
-- Deploy: run this entire script once in the BigQuery console (or `bq query --use_legacy_sql=false`).
-- Test:   call `i-dss-streaming-data.payment_ops_sandbox.refresh_TARGET_TABLE`();
--
-- Source query example: queries/dashboard_anomaly_bin_stddev.sql
--   Replace DECLARE variables with concrete expressions or add procedure arguments.

create or replace procedure `i-dss-streaming-data.payment_ops_sandbox.refresh_d2c_fraud_email_name`()
begin
    create or replace table `i-dss-streaming-data.payment_ops_sandbox.d2c_fraud_email_name`
    -- partition by src_system_id
    as
    with acct_bi as 
        (
            select
                115 as src_system_id
                , acct.account_cd
                , lower(acct.email_address_desc) as acct_email
                , lower(split(acct.email_address_desc,'@')[0]) as email_base
                , lower(split(acct.email_address_desc,'@')[1]) as email_domain
                , case 
                    when acct.email_address_desc like '%+%' then lower(split(split(acct.email_address_desc,'@')[0], '+')[0])
                    when regexp_contains(split(acct.email_address_desc, '@')[0], r'[0-9]') then nullif(trim(lower(regexp_extract(split(acct.email_address_desc, '@')[0], r'^([^0-9]*)'))),'')
                    else null
                end as email_pre_plus_nbr
                -- , lower(regexp_extract(split(acct.email_address_desc, '@')[0], r'^([^0-9]*)')) as email_pre_numbers
                , lower(acct.first_nm) as acct_first_nm
                , lower(acct.last_nm) as acct_last_nm
                , lower(ifnull(trim(acct.first_nm),'') || ' ' || ifnull(trim(acct.last_nm),'')) as acct_full_nm
                , lower(trim(acct.address_1_desc || ' ' || ifnull(trim(acct.address_2_desc),''))) as acct_addr
                , acct.src_created_dt_ut as acct_created_ts
                , acct.src_changed_dt_ut as acct_changed_ts
                , lower(bi.email_address_desc) as bi_email -- doesn't seem to get updated from creation.  acct_email is the current on the account
                , lower(bi.first_nm) as bi_first_nm
                , lower(bi.last_nm) as bi_last_nm
                , lower(ifnull(trim(bi.first_nm),'') || ' ' || ifnull(trim(bi.last_nm),'')) as bi_full_nm
                , lower(trim(bi.address_1_desc || ' ' || ifnull(trim(bi.address_2_desc),''))) as bi_addr
                , bi.src_created_dt_ut as bi_created_ts
                , bi.src_changed_dt_ut as bi_changed_ts
            from i-dss-dw-hc.d_recurly_user_billing.recurly_entertainment_accounts_pii_dim acct
            left join i-dss-dw-hc.d_recurly_user_billing.recurly_entertainment_billing_pii_dim bi
                on acct.account_cd = bi.account_cd
            where 1=1
                and acct.email_address_desc not like '%redacted%'
                and bi.email_address_desc not like '%redacted%'
                and date(acct.src_created_dt_ut) >= date('2025-01-01')
                and date(bi.src_created_dt_ut) >= date('2025-01-01')

            union all

            select
                134 as src_system_id
                , acct.account_cd
                , lower(acct.email_address_desc) as acct_email
                , lower(split(acct.email_address_desc,'@')[0]) as email_base
                , lower(split(acct.email_address_desc,'@')[1]) as email_domain
                , case 
                    when acct.email_address_desc like '%+%' then lower(split(split(acct.email_address_desc,'@')[0], '+')[0])                
                    when regexp_contains(split(acct.email_address_desc, '@')[0], r'[0-9]') then nullif(trim(lower(regexp_extract(split(acct.email_address_desc, '@')[0], r'^([^0-9]*)'))),'')
                    else null
                end as email_pre_plus_nbr
                -- , lower(regexp_extract(split(acct.email_address_desc, '@')[0], r'^([^0-9]*)')) as email_pre_numbers
                , lower(acct.first_nm) as acct_first_nm
                , lower(acct.last_nm) as acct_last_nm
                , lower(ifnull(trim(acct.first_nm),'') || ' ' || ifnull(trim(acct.last_nm),'')) as acct_full_nm
                , lower(trim(acct.address_1_desc || ' ' || ifnull(trim(acct.address_2_desc),''))) as acct_addr
                , acct.src_created_dt_ut as acct_created_ts
                , acct.src_changed_dt_ut as acct_changed_ts
                , lower(bi.email_address_desc) as bi_email
                , lower(bi.first_nm) as bi_first_nm
                , lower(bi.last_nm) as bi_last_nm
                , lower(ifnull(trim(bi.first_nm),'') || ' ' || ifnull(trim(bi.last_nm),'')) as bi_full_nm
                , lower(trim(bi.address_1_desc || ' ' || ifnull(trim(bi.address_2_desc),''))) as bi_addr
                , bi.src_created_dt_ut as bi_created_ts
                , bi.src_changed_dt_ut as bi_changed_ts
            from i-dss-dw-hc.d_recurly_user_billing.recurly_entertainment_can_accounts_pii_dim acct
            left join i-dss-dw-hc.d_recurly_user_billing.recurly_entertainment_can_billing_pii_dim bi
                on acct.account_cd = bi.account_cd
            where 1=1
                and acct.email_address_desc not like '%redacted%'
                and bi.email_address_desc not like '%redacted%'
                and date(acct.src_created_dt_ut) >= date('2025-01-01')
                and date(bi.src_created_dt_ut) >= date('2025-01-01')
        )

    , hex_email as
        (
            select
                src_system_id
                , account_cd
                , acct_email
                , regexp_extract(email_base, r'^[a-z]{4,}([0-9a-f]{10})$') as email_hex
                , acct_created_ts
            from acct_bi
            where 1=1
                and regexp_contains(email_base, r'^[a-z]{4,10}[0-9a-f]{10}$') 
                -- and regexp_contains(email_base, r'^[a-z]{12}[0-9]{6}$')
        )

    , acct_name_counts as
        (
            select
                a.src_system_id
                , a.acct_full_nm
                , date(a.acct_created_ts) as anchor_dt
                , count(distinct b.account_cd) as acct_ct
                , min(b.acct_created_ts) as min_dt
                , max(b.acct_created_ts) as max_dt
                , min(b.account_cd) as ex1
                , max(b.account_cd) as ex2
            from acct_bi a
            inner join acct_bi b
                on b.src_system_id = a.src_system_id
                and b.account_cd != a.account_cd
                and a.acct_full_nm = b.acct_full_nm
                and date(b.acct_created_ts) between date_sub(date(a.acct_created_ts), interval 20 day) and date_add(date(a.acct_created_ts), interval 20 day)
            where 1=1
                and nullif(trim(a.acct_full_nm),'') is not null
                and ifnull(trim(a.acct_full_nm),'') != 'sky customer'
                and ifnull(trim(a.acct_full_nm),'') != 'showtime user'
            group by all
            having acct_ct >= 10
        )

    , bi_name_counts as
        (
            select
                a.src_system_id
                , a.bi_full_nm
                , date(a.acct_created_ts) as anchor_dt
                , count(distinct b.account_cd) as acct_ct
                , min(b.acct_created_ts) as min_dt
                , max(b.acct_created_ts) as max_dt
                , min(b.account_cd) as ex1
                , max(b.account_cd) as ex2
            from acct_bi a
            inner join acct_bi b
                on b.src_system_id = a.src_system_id
                and b.account_cd != a.account_cd
                and a.bi_full_nm = b.bi_full_nm
                and date(b.acct_created_ts) between date_sub(date(a.acct_created_ts), interval 20 day) and date_add(date(a.acct_created_ts), interval 20 day)
            where 1=1
                and nullif(trim(a.bi_full_nm),'') is not null
                and ifnull(trim(a.bi_full_nm),'') != 'sky customer'
                and ifnull(trim(a.acct_full_nm),'') != 'showtime user'
            group by all
            having acct_ct >= 10
        )

    , email_base_counts as 
        (
            select
                a.src_system_id
                , a.email_pre_plus_nbr
                , a.email_domain
                , date(a.acct_created_ts) as anchor_dt
                , count(distinct b.account_cd) as acct_ct
                , min(b.acct_created_ts) as min_dt
                , max(b.acct_created_ts) as max_dt
                , min(b.account_cd) as ex1
                , max(b.account_cd) as ex2
            from acct_bi a
            inner join acct_bi b
                on b.src_system_id = a.src_system_id
                and b.account_cd != a.account_cd
                and a.email_pre_plus_nbr = b.email_pre_plus_nbr
                and a.email_domain = b.email_domain
                and date(b.acct_created_ts) between date_sub(date(a.acct_created_ts), interval 20 day) and date_add(date(a.acct_created_ts), interval 20 day)
                and lower(trim(b.email_pre_plus_nbr)) not like '%' || lower(trim(b.acct_first_nm)) || '%'
                and lower(trim(b.email_pre_plus_nbr)) not like '%' || lower(trim(b.bi_first_nm)) || '%'
                and lower(trim(b.acct_first_nm)) not like '%' || lower(trim(b.email_pre_plus_nbr)) || '%'
                and lower(trim(b.bi_first_nm)) not like '%' || lower(trim(b.email_pre_plus_nbr)) || '%'
                and lower(trim(b.email_pre_plus_nbr)) not like '%' || lower(trim(b.acct_last_nm)) || '%'
                and lower(trim(b.email_pre_plus_nbr)) not like '%' || lower(trim(b.bi_last_nm)) || '%'
                and not regexp_contains(b.email_base, r'^[a-z]{4,10}[0-9a-f]{10}$')
            where 1=1
                and a.email_pre_plus_nbr is not null
                and length(a.email_pre_plus_nbr) >= 3
                and lower(trim(a.email_pre_plus_nbr)) not like '%' || lower(trim(a.acct_first_nm)) || '%'
                and lower(trim(a.email_pre_plus_nbr)) not like '%' || lower(trim(a.bi_first_nm)) || '%'
                and lower(trim(a.acct_first_nm)) not like '%' || lower(trim(a.email_pre_plus_nbr)) || '%'
                and lower(trim(a.bi_first_nm)) not like '%' || lower(trim(a.email_pre_plus_nbr)) || '%'
                and lower(trim(a.email_pre_plus_nbr)) not like '%' || lower(trim(a.acct_last_nm)) || '%'
                and lower(trim(a.email_pre_plus_nbr)) not like '%' || lower(trim(a.bi_last_nm)) || '%'
                and not regexp_contains(a.email_base, r'^[a-z]{4,10}[0-9a-f]{10}$')
            group by all
            having acct_ct >= 10
        )

    select 
        ab.src_system_id
        , ab.account_cd
        , ab.acct_created_ts
        , date(ab.acct_created_ts) as acct_created_dt
        , max(case when he.src_system_id is not null then 1 else 0 end) as hex_email_chk
        , max(case when anc.src_system_id is not null then anc.acct_ct else 0 end) as acct_name_chk
        , max(case when bnc.src_system_id is not null then bnc.acct_ct else 0 end) as bi_name_chk
        , max(case when ebc.src_system_id is not null then ebc.acct_ct else 0 end) as email_base_chk
        -- , max(case when adc.src_system_id is not null then 1 else 0 end) as email_domain_chk
    from acct_bi ab
    left join hex_email he
        on he.src_system_id = ab.src_system_id
        and he.account_cd = ab.account_cd
    left join acct_name_counts anc
        on anc.src_system_id = ab.src_system_id
        and anc.acct_full_nm = ab.acct_full_nm
        and date(ab.acct_created_ts) between date(anc.min_dt) and date(anc.max_dt)
    left join bi_name_counts bnc
        on bnc.src_system_id = ab.src_system_id
        and bnc.bi_full_nm = ab.bi_full_nm
        and date(ab.acct_created_ts) between date(bnc.min_dt) and date(bnc.max_dt)
    left join email_base_counts ebc
        on ebc.src_system_id = ab.src_system_id
        and ebc.email_pre_plus_nbr = ab.email_pre_plus_nbr
        and ebc.email_domain = ab.email_domain
        and date(ab.acct_created_ts) between date(ebc.min_dt) and date(ebc.max_dt)
        -- and lower(trim(ebc.email_pre_plus_nbr)) != lower(trim(ab.acct_first_nm))
        -- and lower(trim(ebc.email_pre_plus_nbr)) != lower(trim(ab.bi_first_nm))
        and lower(trim(ebc.email_pre_plus_nbr)) not like '%' || lower(trim(ab.acct_first_nm)) || '%'
        and lower(trim(ebc.email_pre_plus_nbr)) not like '%' || lower(trim(ab.bi_first_nm)) || '%'
        and lower(trim(ab.acct_first_nm)) not like '%' || lower(trim(ebc.email_pre_plus_nbr)) || '%'
        and lower(trim(ab.bi_first_nm)) not like '%' || lower(trim(ebc.email_pre_plus_nbr)) || '%'
        and lower(trim(ebc.email_pre_plus_nbr)) not like '%' || lower(trim(ab.acct_last_nm)) || '%'
        and lower(trim(ebc.email_pre_plus_nbr)) not like '%' || lower(trim(ab.bi_last_nm)) || '%'
    where 1=1
        and date(ab.acct_created_ts) >= date('2025-04-01')

    group by all
    having hex_email_chk = 1 or acct_name_chk > 40 or bi_name_chk > 40 or email_base_chk > 10
    order by acct_created_ts desc
    ;
end;

