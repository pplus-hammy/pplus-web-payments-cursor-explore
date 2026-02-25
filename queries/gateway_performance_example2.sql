declare dt_filter date;
declare src_id int64;
set dt_filter = date('2025-03-01');
set src_id = 115;

with sub as 
    (
        select
            case
                when sub.frn = 1 and sub.trial_end_dt_ut is null then 'original_dtp' -- should only be people who previously had an in-app subscription 
                when sub.frn = 1 and sub.trial_end_dt_ut is not null then 'original_trial'
                when sub.frn > 1 and sub.trial_end_dt_ut is null then 'winback_dtp'
                when sub.frn > 1 and sub.trial_end_dt_ut is not null then 'winback_trial'
                else 'other'
            end as sub_category
            , case
                when datetime(sub.expiration_dt_ut, 'America/New_York') between date_add(date_trunc(current_datetime('America/New_York'), DAY), INTERVAL -1 SECOND) and datetime('2999-12-01 23:59:59') then 'canceled'
                when datetime(sub.expiration_dt_ut, 'America/New_York') < date_trunc(current_datetime('America/New_York'), DAY) then 'expired'
                when sub.expiration_dt_ut = timestamp('2999-12-31 23:59:59 UTC') then 'active'
                else sub.status_desc
            end as status_desc
            , sub.* except (status_desc)
            , coalesce(lag(sub.expiration_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, sub.expiration_dt_ut), timestamp('1900-01-01 00:00:01 UTC')) as prior_expiration
            , max(case when sub.frn = 1 then sub.activate_dt_ut else null end) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, sub.expiration_dt_ut) as original_activation
        from
            (
                select
                    sub.src_system_id
                    , sub.account_cd
                    , sub.subscription_guid
                    , sub.plan_cd
                    , sub.plan_nm
                    , trim(split(regexp_replace(sub.plan_nm, r'\([^()]*\)',''), ' -')[offset(0)]) as base_plan_nm
                    , regexp_extract(sub.plan_nm, r'\((.*?)\)') as plan_qualifier
                    , case
                        when lower(sub.plan_cd) like '%monthly%' then 'monthly'
                        when lower(sub.plan_cd) like '%annual%' then 'annual'
                        else 'other'
                    end as plan_dur
                    , sub.unit_amt
                    , sub.status_desc
                    , sub.creation_dt_ut
                    , sub.activate_dt_ut
                    , sub.trial_start_dt_ut
                    , sub.trial_end_dt_ut
                    , sub.curr_period_start_dt_ut
                    , sub.curr_period_end_dt_ut
                    , sub.cancel_dt_ut
                    , case
                        when sub.expiration_dt_ut is null and lower(sub.plan_cd) like '%monthly%' and date(sub.curr_period_end_dt_ut) <= date_add(current_date, interval -2 MONTH)
                            then sub.curr_period_end_dt_ut
                        when sub.expiration_dt_ut is null and lower(sub.plan_cd) like '%annual%' and date(sub.curr_period_end_dt_ut) <= date_add(current_date, interval -2 MONTH) -- should it be longer?
                            then sub.curr_period_end_dt_ut
                        when sub.expiration_dt_ut is null
                            then timestamp('2999-12-31 23:59:59 UTC')
                        else sub.expiration_dt_ut
                    end as expiration_dt_ut
                    -- , ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) as expiration_dt_ut
                    , case
                        when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval -150 SECOND) 
                        else date_add(sub.activate_dt_ut, interval -120 SECOND) 
                    end as pre_activate
                    , case
                        when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval 150 SECOND) 
                        else date_add(sub.activate_dt_ut, interval 120 SECOND) 
                    end as post_activate
                    , date_add(sub.trial_end_dt_ut, interval -120 SECOND) as pre_trial_end
                    , date_add(sub.trial_end_dt_ut, interval 120 SECOND) as post_trial_end
                    , row_number() over (partition by sub.src_system_id, sub.account_cd order by sub.creation_dt_ut, sub.activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as frn
                    , row_number() over (partition by sub.src_system_id, sub.account_cd order by sub.creation_dt_ut desc, sub.activate_dt_ut desc, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) desc) as lrn
                    
                    , lag(sub.plan_nm) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as prior_plan
                    , coalesce(lag(sub.activate_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))), timestamp('1900-01-01 00:00:01 UTC')) as prior_activation

                    , lead(sub.plan_nm) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as next_plan
                    , coalesce(lead(sub.activate_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))), timestamp('2999-12-31 23:59:59 UTC')) as next_activation
                from i-dss-streaming-data.payment_ops_vw.recurly_subscription_dim sub
                where 1=1
                    and sub.src_system_id = src_id
            ) sub
        where 1=1
    )

, adj as
    (
        select distinct
            adj.src_system_id
            , adj.account_cd
            , adj.subscription_guid
            , adj.invoice_guid
            , adj.invoice_nbr
            , adj.invoice_billed_dt_ut
            , nullif(trim(adj.invoice_type_cd),'') as invoice_type_cd
            , adj.invoice_state_desc
            , adj.invoice_closed_dt_ut
            , adj.coupon_cd
            , sum(adj.adj_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_amt
            , sum(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_total_amt
        from i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
        where 1=1
            -- and adj.creation_dt >= date('2025-03-01')
            and adj.creation_dt >= date_add(dt_filter, interval -1 MONTH)
            and adj.src_system_id = src_id
            -- and adj.invoice_type_cd = 'renewal'
    )

, txn as
    (
        select distinct
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
            -- , txn.gateway_cd
            , txn.account_cd
            , nullif(trim(txn.subscription_guid),'') as txn_subscription_guid
            , txn.invoice_guid
            , txn.trans_dt
            , txn.trans_dt_ut
            , datetime(txn.trans_dt_ut, 'America/New_York') as trans_dt_et
            , txn.transaction_guid
            , txn.origin_desc
            , txn.trans_type_desc
            , txn.trans_status_desc
            , case
                when txn.trans_status_desc in ('success','void') then 'success'
                when txn.trans_status_desc = 'declined' then txn.trans_status_desc
                else txn.trans_status_desc
            end as trans_status_desc_2
            , txn.trans_amt 
            , txn.tax_amt 
            , txn.currency_cd 
            , txn.country_cd
            , txn.trans_gateway_type_desc
            -- , txn.reference_cd
            , case  
                when txn.trans_gateway_type_desc = 'cybersource' and txn.reference_cd like '%;%;%' then split(txn.reference_cd,';')[1]
                else txn.reference_cd
            end as reference_cd
            , txn.cc_type_desc
            , txn.cc_first_6_nbr
            , txn.approval_desc
            , txn.trans_msg_desc
            , txn.failure_type
            , txn.gateway_error_cd
            -- , case
            --     when (txn.gateway_error_cd = '481' or (txn.gateway_error_cd = 'Refused' and txn.trans_msg_desc = 'FRAUD') then 'gateway_rule_decline'
            --     else null
            -- end as rule_decline_chk
            , case
                when txn.trans_type_desc != 'verify' and nullif(trim(txn.subscription_guid),'') is not null
                    then 'purch_on_sub'
                when txn.trans_type_desc = 'verify' 
                        or (txn.trans_type_desc != 'verify' and nullif(trim(txn.subscription_guid),'') is null)
                    then 'verify_or_purch_pre_sub'
            end as txn_type
            , dense_rank() over (partition by txn.src_system_id, txn.account_cd, txn.invoice_guid, case when txn.trans_status_desc in ('success','void') then 'success' when txn.trans_status_desc = 'declined' then txn.trans_status_desc else txn.trans_status_desc end order by txn.trans_dt_ut desc) as latest_txn
            , dense_rank() over (partition by txn.src_system_id, txn.account_cd, case when trans_type_desc = 'verify' then 'verify' else null end, txn.trans_dt, case when txn.trans_status_desc in ('success','void') then 'success' when txn.trans_status_desc = 'declined' then txn.trans_status_desc else txn.trans_status_desc end order by txn.trans_dt_ut desc) as verify_latest_txn
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.src_system_id = src_id
            -- and txn.trans_dt >= date('2025-04-01')
            and txn.trans_dt >= dt_filter
            and txn.trans_type_desc in ('purchase','verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.trans_gateway_type_desc in ('adyen', 'cybersource')
            and ifnull(trim(txn.failure_type),'') not in ('fraud_velocity', 'gateway_timeout') -- recurly velocity limit that prevents the txn from going to gateway
            -- and ifnull(trim(txn.failure_type),'') not in ('fraud_gateway') -- decision manager/rule rejects
            and 
                (
                    nullif(trim(reference_cd),'') is not null -- reference cd has the gateway transaction id (psp for adyen), if it's empty the transaction probably did not go to the gateway
                    or
                    ifnull(trim(txn.approval_desc), '') = 'Collected by Vindicia' -- or, if ref_cd is blank but it's collected by vindicia, include
                )
            -- and case
            --     when (txn.gateway_error_cd = '481' or (txn.gateway_error_cd = 'Refused' and txn.trans_msg_desc = 'FRAUD') then 'gateway_rule_decline'
            --     else null
            -- end is null -- filter out any decision manager/rules at gateway to decline txns
    )
, cybs_decl as
    (
        select
            merchantid
            , requestid
            , issuerresponsecode
            , replace(lower(case
                when issuerresponsecode = 'O' then 'No Response sent'
                when cardtype = 'Visa' and issuerresponsecode = '000' then 'Successful approval/completion or that V.I.P. PIN verification is successful'
                when cardtype = 'Visa' and issuerresponsecode = '001' then 'Refer to card issuer'
                when cardtype = 'Visa' and issuerresponsecode = '002' then 'Refer to card issuer, special condition'
                when cardtype = 'Visa' and issuerresponsecode = '003' then 'Invalid merchant or service provider'
                when cardtype = 'Visa' and issuerresponsecode = '004' then 'Pickup card'
                when cardtype = 'Visa' and issuerresponsecode = '005' then 'Do not honor'
                when cardtype = 'Visa' and issuerresponsecode = '006' then 'Error'
                when cardtype = 'Visa' and issuerresponsecode = '007' then 'Pickup card, special condition (other than lost/stolen card)'
                when cardtype = 'Visa' and issuerresponsecode = '010' then 'Partial Approval'
                when cardtype = 'Visa' and issuerresponsecode = '011' then 'V.I.P. approval'
                when cardtype = 'Visa' and issuerresponsecode = '012' then 'Invalid transaction'
                when cardtype = 'Visa' and issuerresponsecode = '013' then 'Invalid amount (currency conversion field overflow); or amount exceeds maximum for card program'
                when cardtype = 'Visa' and issuerresponsecode = '014' then 'Invalid account number (no such number)'
                when cardtype = 'Visa' and issuerresponsecode = '015' then 'No such issuer'
                when cardtype = 'Visa' and issuerresponsecode = '019' then 'Re-enter transaction'
                when cardtype = 'Visa' and issuerresponsecode = '021' then 'No action taken (unable to back out prior transaction)'
                when cardtype = 'Visa' and issuerresponsecode = '025' then 'Unable to locate record in file, or account number is missing from the inquiry'
                when cardtype = 'Visa' and issuerresponsecode = '028' then 'File is temporarily unavailable'
                when cardtype = 'Visa' and issuerresponsecode = '039' then 'No credit account'
                when cardtype = 'Visa' and issuerresponsecode = '041' then 'Pickup card (lost card)'
                when cardtype = 'Visa' and issuerresponsecode = '043' then 'Pickup card (stolen card)'
                when cardtype = 'Visa' and issuerresponsecode = '046' then 'Closed account'
                when cardtype = 'Visa' and issuerresponsecode = '051' then 'Insufficient funds'
                when cardtype = 'Visa' and issuerresponsecode = '052' then 'No checking account'
                when cardtype = 'Visa' and issuerresponsecode = '053' then 'No savings account'
                when cardtype = 'Visa' and issuerresponsecode = '054' then 'Expired card'
                when cardtype = 'Visa' and issuerresponsecode = '055' then 'Incorrect PIN'
                when cardtype = 'Visa' and issuerresponsecode = '057' then 'Transaction not permitted to cardholder'
                when cardtype = 'Visa' and issuerresponsecode = '058' then 'Transaction not allowed at terminal'
                when cardtype = 'Visa' and issuerresponsecode = '059' then 'Suspected fraud'
                when cardtype = 'Visa' and issuerresponsecode = '061' then 'Exceeds approval amount limit'
                when cardtype = 'Visa' and issuerresponsecode = '062' then 'Restricted card (for example, in Country Exclusion table)'
                when cardtype = 'Visa' and issuerresponsecode = '063' then 'Security violation'
                when cardtype = 'Visa' and issuerresponsecode = '064' then 'Transaction does not fulfill AML requirement'
                when cardtype = 'Visa' and issuerresponsecode = '065' then 'Exceeds withdrawal frequency limit'
                when cardtype = 'Visa' and issuerresponsecode = '070' then 'PIN data required'
                when cardtype = 'Visa' and issuerresponsecode = '075' then 'Allowable number of PIN-entry tries exceeded'
                when cardtype = 'Visa' and issuerresponsecode = '076' then 'Unable to locate previous message (no match on retrieval reference number)'
                when cardtype = 'Visa' and issuerresponsecode = '077' then 'Previous message located for a repeat or reversal, but repeat or reversal data inconsistent with original message'
                when cardtype = 'Visa' and issuerresponsecode = '078' then 'Blocked, first used — Transaction from new cardholder, and card not properly unblocked'
                when cardtype = 'Visa' and issuerresponsecode = '079' then 'Transaction reversed'
                when cardtype = 'Visa' and issuerresponsecode = '080' then 'Visa transactions: credit issuer unavailable. Private label: invalid date'
                when cardtype = 'Visa' and issuerresponsecode = '081' then 'PIN cryptographic error found (error found by VIC security module during PIN decryption)'
                when cardtype = 'Visa' and issuerresponsecode = '082' then 'Negative Online CAM, dCVV, iCVV, or CVV results. Or Offline PIN authentication interrupted'
                when cardtype = 'Visa' and issuerresponsecode = '06P' then 'Verification data failed'
                when cardtype = 'Visa' and issuerresponsecode = '085' then 'No reason to decline request for account number verification, address verification, CVV2 verification, or credit voucher or merchandise return'
                when cardtype = 'Visa' and issuerresponsecode = '086' then 'Cannot verify PIN'
                when cardtype = 'Visa' and issuerresponsecode = '091' then 'Issuer unavailable or switch inoperative (STIP not applicable or available for this transaction). Issuers can respond with this code, which V.I.P. passes to the acquirer without invoking stand-in processing (STIP). Causes decline at POS.'
                when cardtype = 'Visa' and issuerresponsecode = '092' then 'Financial institution or intermediate network facility cannot be found for routing'
                when cardtype = 'Visa' and issuerresponsecode = '093' then 'Transaction cannot be completed; violation of law'
                when cardtype = 'Visa' and issuerresponsecode = '094' then 'Duplicate transaction. Transaction submitted containing values in tracing data fields that duplicate values in a previous transaction.'
                when cardtype = 'Visa' and issuerresponsecode = '096' then 'System malfunction; System malfunction or certain field error conditions'
                when cardtype = 'Visa' and issuerresponsecode = '01A' then 'Additional customer authentication required'
                when cardtype = 'Visa' and issuerresponsecode = '0B1' then 'Surcharge amount not permitted on Visa cards (U.S. acquirers only)'
                when cardtype = 'Visa' and issuerresponsecode = '0N0' then 'Force STIP'
                when cardtype = 'Visa' and issuerresponsecode = '0N3' then 'Cash service not available'
                when cardtype = 'Visa' and issuerresponsecode = '0N4' then 'Cashback request exceeds issuer limit'
                when cardtype = 'Visa' and issuerresponsecode = '0N7' then 'Decline for CVV2 failure'
                when cardtype = 'Visa' and issuerresponsecode = '0N8' then 'Transaction amount exceeds pre-authorized approval amount'
                when cardtype = 'Visa' and issuerresponsecode = '0P2' then 'Invalid biller information'
                when cardtype = 'Visa' and issuerresponsecode = '0P5' then 'PIN Change/Unblock request declined'
                when cardtype = 'Visa' and issuerresponsecode = '0P6' then 'Unsafe PIN'
                when cardtype = 'Visa' and issuerresponsecode = '0Q1' then 'Card authentication failed or Offline PIN authentication interrupted'
                when cardtype = 'Visa' and issuerresponsecode = '0R0' then 'Stop Payment Order'
                when cardtype = 'Visa' and issuerresponsecode = '0R1' then 'Revocation of Authorization Order'
                when cardtype = 'Visa' and issuerresponsecode = '0R3' then 'Revocation of All Authorizations Order'
                when cardtype = 'Visa' and issuerresponsecode = '0Z3' then 'Unable to go online; declined'
                when cardtype = 'Visa' and issuerresponsecode = '0XA' then 'Forward to issuer'
                when cardtype = 'Visa' and issuerresponsecode = '0XD' then 'Forward to issuer'
                when cardtype = 'Visa' and issuerresponsecode = '09G' then 'Blocked by cardholder/contact cardholder'
                when cardtype = 'Visa' and issuerresponsecode = '05C' then 'Do Not Honor'
                when cardtype = 'Visa' and issuerresponsecode = '072' then 'No Authorization'
                when cardtype = 'MasterCard' and issuerresponsecode = '000' then 'Approved or completed successfully'
                when cardtype = 'MasterCard' and issuerresponsecode = '001' then 'Refer to card issuer'
                when cardtype = 'MasterCard' and issuerresponsecode = '003' then 'Invalid merchant'
                when cardtype = 'MasterCard' and issuerresponsecode = '004' then 'Capture card'
                when cardtype = 'MasterCard' and issuerresponsecode = '005' then 'Do not honor'
                when cardtype = 'MasterCard' and issuerresponsecode = '006' then 'Error'
                when cardtype = 'MasterCard' and issuerresponsecode = '008' then 'Honor with ID'
                when cardtype = 'MasterCard' and issuerresponsecode = '010' then 'Partial Approval'
                when cardtype = 'MasterCard' and issuerresponsecode = '012' then 'Invalid transaction'
                when cardtype = 'MasterCard' and issuerresponsecode = '013' then 'Invalid amount'
                when cardtype = 'MasterCard' and issuerresponsecode = '014' then 'Invalid card number'
                when cardtype = 'MasterCard' and issuerresponsecode = '015' then 'Invalid issuer'
                when cardtype = 'MasterCard' and issuerresponsecode = '017' then 'Customer cancellation'
                when cardtype = 'MasterCard' and issuerresponsecode = '025' then 'Unable to locate record on file (no action taken)'
                when cardtype = 'MasterCard' and issuerresponsecode = '026' then 'Record not in active status'
                when cardtype = 'MasterCard' and issuerresponsecode = '027' then 'Issuer File Update field edit error'
                when cardtype = 'MasterCard' and issuerresponsecode = '028' then 'Record permanently deleted'
                when cardtype = 'MasterCard' and issuerresponsecode = '029' then 'Delete request less than 540 days'
                when cardtype = 'MasterCard' and issuerresponsecode = '030' then 'Format error'
                when cardtype = 'MasterCard' and issuerresponsecode = '032' then 'Partial reversal'
                when cardtype = 'MasterCard' and issuerresponsecode = '034' then 'Suspect Fraud'
                when cardtype = 'MasterCard' and issuerresponsecode = '040' then 'Requested function not supported'
                when cardtype = 'MasterCard' and issuerresponsecode = '041' then 'Lost card'
                when cardtype = 'MasterCard' and issuerresponsecode = '043' then 'Stolen card'
                when cardtype = 'MasterCard' and issuerresponsecode = '051' then 'Insufficient funds/over credit limit'
                when cardtype = 'MasterCard' and issuerresponsecode = '054' then 'Expired card'
                when cardtype = 'MasterCard' and issuerresponsecode = '055' then 'Invalid PIN'
                when cardtype = 'MasterCard' and issuerresponsecode = '057' then 'Transaction not permitted to issuer/cardholder'
                when cardtype = 'MasterCard' and issuerresponsecode = '058' then 'Transaction not permitted to acquirer/terminal'
                when cardtype = 'MasterCard' and issuerresponsecode = '061' then 'Exceeds withdrawal amount limit'
                when cardtype = 'MasterCard' and issuerresponsecode = '062' then 'Restricted card'
                when cardtype = 'MasterCard' and issuerresponsecode = '063' then 'Security violation'
                when cardtype = 'MasterCard' and issuerresponsecode = '065' then 'Exceeds withdrawal count limit OR Identity Check Soft-Decline of EMV 3DS Authentication (merchant should resubmit authentication with 3DSv1)'
                when cardtype = 'MasterCard' and issuerresponsecode = '068' then 'Response received late'
                when cardtype = 'MasterCard' and issuerresponsecode = '070' then 'Contact card issuer'
                when cardtype = 'MasterCard' and issuerresponsecode = '071' then 'PIN not changed'
                when cardtype = 'MasterCard' and issuerresponsecode = '072' then 'Account not activated'
                when cardtype = 'MasterCard' and issuerresponsecode = '075' then 'Allowable number of PIN tries exceeded'
                when cardtype = 'MasterCard' and issuerresponsecode = '076' then 'Invalid/nonexistent “To Account” specified'
                when cardtype = 'MasterCard' and issuerresponsecode = '077' then 'Invalid/nonexistent “From Account” specified'
                when cardtype = 'MasterCard' and issuerresponsecode = '078' then 'Invalid/nonexistent account specified (general)'
                when cardtype = 'MasterCard' and issuerresponsecode = '079' then 'Life cycle'
                when cardtype = 'MasterCard' and issuerresponsecode = '080' then 'Duplicate add, action not performed'
                when cardtype = 'MasterCard' and issuerresponsecode = '081' then 'Domestic Debit Transaction Not Allowed (Regional use only)'
                when cardtype = 'MasterCard' and issuerresponsecode = '082' then 'Policy'
                when cardtype = 'MasterCard' and issuerresponsecode = '083' then 'Fraud/Security'
                when cardtype = 'MasterCard' and issuerresponsecode = '084' then 'Invalid Authorization Life Cycle'
                when cardtype = 'MasterCard' and issuerresponsecode = '085' then 'Not declined. Valid for all zero amount transactions'
                when cardtype = 'MasterCard' and issuerresponsecode = '086' then 'PIN validation not possible'
                when cardtype = 'MasterCard' and issuerresponsecode = '087' then 'Purchase Amount only, no cash back allowed'
                when cardtype = 'MasterCard' and issuerresponsecode = '088' then 'Cryptographic failure'
                when cardtype = 'MasterCard' and issuerresponsecode = '089' then 'Unacceptable PIN - Transaction declined - Retry'
                when cardtype = 'MasterCard' and issuerresponsecode = '091' then 'Authorization platform or issuer system inoperative'
                when cardtype = 'MasterCard' and issuerresponsecode = '092' then 'Unable to route transaction'
                when cardtype = 'MasterCard' and issuerresponsecode = '094' then 'Duplicate transmission detected'
                when cardtype = 'MasterCard' and issuerresponsecode = '096' then 'System Error'
                when cardtype = 'American Express' and issuerresponsecode = '000' then 'Approved'
                when cardtype = 'American Express' and issuerresponsecode = '001' then 'Approve with ID'
                when cardtype = 'American Express' and issuerresponsecode = '002' then 'Partial Approval (Prepaid Cards only)'
                when cardtype = 'American Express' and issuerresponsecode = '100' then 'Deny'
                when cardtype = 'American Express' and issuerresponsecode = '101' then 'Expired Card/Invalid Expiration Date'
                when cardtype = 'American Express' and issuerresponsecode = '106' then 'Exceeded PIN attempts'
                when cardtype = 'American Express' and issuerresponsecode = '107' then 'Please Call Issuer'
                when cardtype = 'American Express' and issuerresponsecode = '109' then 'Invalid Merchant'
                when cardtype = 'American Express' and issuerresponsecode = '110' then 'Invalid Amount'
                when cardtype = 'American Express' and issuerresponsecode = '111' then 'Invalid Account/Invalid MICR (Travelers Cheque)'
                when cardtype = 'American Express' and issuerresponsecode = '115' then 'Requested function not supported'
                when cardtype = 'American Express' and issuerresponsecode = '116' then 'Not Sufficient Funds'
                when cardtype = 'American Express' and issuerresponsecode = '117' then 'Invalid PIN'
                when cardtype = 'American Express' and issuerresponsecode = '119' then 'Cardmember not enrolled/not permitted'
                when cardtype = 'American Express' and issuerresponsecode = '121' then 'Limit Exceeded'
                when cardtype = 'American Express' and issuerresponsecode = '122' then 'Invalid card security code (a.k.a, CID, 4DBC, 4CSC)'
                when cardtype = 'American Express' and issuerresponsecode = '125' then 'Invalid effective date'
                when cardtype = 'American Express' and issuerresponsecode = '130' then 'Additional customer identification required'
                when cardtype = 'American Express' and issuerresponsecode = '181' then 'Format Error'
                when cardtype = 'American Express' and issuerresponsecode = '183' then 'Invalid currency code'
                when cardtype = 'American Express' and issuerresponsecode = '187' then 'Deny - New card issued'
                when cardtype = 'American Express' and issuerresponsecode = '189' then 'Deny - Canceled or Closed Merchant/SE'
                when cardtype = 'American Express' and issuerresponsecode = '194' then 'Invalid Country Code'
                when cardtype = 'American Express' and issuerresponsecode = '200' then 'Deny - Pick up card'
                when cardtype = 'American Express' and issuerresponsecode = '400' then 'Reversal Accepted'
                when cardtype = 'American Express' and issuerresponsecode = '900' then 'Accepted - ATC Synchronization'
                when cardtype = 'American Express' and issuerresponsecode = '909' then 'System Malfunction (Cryptographic error)'
                when cardtype = 'American Express' and issuerresponsecode = '912' then 'Issuer not available'
                when cardtype = 'Discover' and issuerresponsecode = '000' then 'Approved or completed successfully'
                when cardtype = 'Discover' and issuerresponsecode = '003' then 'Invalid Merchant'
                when cardtype = 'Discover' and issuerresponsecode = '004' then 'Capture Card'
                when cardtype = 'Discover' and issuerresponsecode = '005' then 'Do not honor'
                when cardtype = 'Discover' and issuerresponsecode = '007' then 'Pick-up Card, special condition'
                when cardtype = 'Discover' and issuerresponsecode = '010' then 'Approved for partial amount'
                when cardtype = 'Discover' and issuerresponsecode = '011' then 'Approved'
                when cardtype = 'Discover' and issuerresponsecode = '012' then 'Invalid transaction'
                when cardtype = 'Discover' and issuerresponsecode = '013' then 'Invalid amount'
                when cardtype = 'Discover' and issuerresponsecode = '014' then 'Invalid Card Number'
                when cardtype = 'Discover' and issuerresponsecode = '019' then 'Re-enter transaction'
                when cardtype = 'Discover' and issuerresponsecode = '030' then 'Format error'
                when cardtype = 'Discover' and issuerresponsecode = '031' then 'Bank not supported by switch'
                when cardtype = 'Discover' and issuerresponsecode = '038' then 'Allowable PIN tries exceeded'
                when cardtype = 'Discover' and issuerresponsecode = '039' then 'No credit Account'
                when cardtype = 'Discover' and issuerresponsecode = '040' then 'Requested function not supported'
                when cardtype = 'Discover' and issuerresponsecode = '041' then 'Lost Card'
                when cardtype = 'Discover' and issuerresponsecode = '043' then 'Stolen Card'
                when cardtype = 'Discover' and issuerresponsecode = '051' then 'Decline'
                when cardtype = 'Discover' and issuerresponsecode = '053' then 'No savings Account'
                when cardtype = 'Discover' and issuerresponsecode = '054' then 'Expired Card'
                when cardtype = 'Discover' and issuerresponsecode = '055' then 'Invalid PIN'
                when cardtype = 'Discover' and issuerresponsecode = '056' then 'No Card record'
                when cardtype = 'Discover' and issuerresponsecode = '057' then 'Transaction not permitted to Issuer/Cardholder'
                when cardtype = 'Discover' and issuerresponsecode = '058' then 'Transaction not permitted to Acquirer/terminal'
                when cardtype = 'Discover' and issuerresponsecode = '059' then 'Suspected fraud'
                when cardtype = 'Discover' and issuerresponsecode = '060' then 'Card acceptor contact Acquirer'
                when cardtype = 'Discover' and issuerresponsecode = '061' then 'Exceeds withdrawal amount limit'
                when cardtype = 'Discover' and issuerresponsecode = '062' then 'Restricted Card'
                when cardtype = 'Discover' and issuerresponsecode = '063' then 'Security violation'
                when cardtype = 'Discover' and issuerresponsecode = '064' then 'Original amount incorrect'
                when cardtype = 'Discover' and issuerresponsecode = '065' then 'Exceeds withdrawal count limit'
                when cardtype = 'Discover' and issuerresponsecode = '066' then 'Card Acceptor call Acquirers security dept'
                when cardtype = 'Discover' and issuerresponsecode = '067' then 'Hard capture (requires ATM pick-up)'
                when cardtype = 'Discover' and issuerresponsecode = '068' then 'Response received too late'
                when cardtype = 'Discover' and issuerresponsecode = '075' then 'Allowable number of PIN tries exceeded'
                when cardtype = 'Discover' and issuerresponsecode = '076' then 'Invalid/nonexistent “to” Account specified'
                when cardtype = 'Discover' and issuerresponsecode = '077' then 'Invalid/nonexistent “from” Account specified'
                when cardtype = 'Discover' and issuerresponsecode = '078' then 'Invalid/nonexistent Account specified (general)'
                when cardtype = 'Discover' and issuerresponsecode = '083' then 'Domain Restriction Controls Failure'
                when cardtype = 'Discover' and issuerresponsecode = '085' then 'No reason to decline'
                when cardtype = 'Discover' and issuerresponsecode = '087' then 'Network unavailable'
                when cardtype = 'Discover' and issuerresponsecode = '091' then 'Authorization system or Issuer system inoperative'
                when cardtype = 'Discover' and issuerresponsecode = '092' then 'Unable to route transaction'
                when cardtype = 'Discover' and issuerresponsecode = '093' then 'Transaction cannot be completed, violation of law'
                when cardtype = 'Discover' and issuerresponsecode = '094' then 'Duplicate transmission detected'
                when cardtype = 'Discover' and issuerresponsecode = '096' then 'System malfunction'
                when cardtype = 'Discover' and issuerresponsecode = '01A' then 'Customer Authentication Required (Decline)'
                when cardtype = 'Discover' and issuerresponsecode = '0N1' then 'System up'
                when cardtype = 'Discover' and issuerresponsecode = '0N2' then 'Soft down'
                when cardtype = 'Discover' and issuerresponsecode = '0N3' then 'System down'
                when cardtype = 'Discover' and issuerresponsecode = '0N7' then 'Decline for AVS or CID mismatch'
                when cardtype = 'Discover' and issuerresponsecode = '0P5' then 'PIN Change/Unblock failed'
                when cardtype = 'Discover' and issuerresponsecode = '0P6' then 'New PIN not accepted'
                else 'other'
            end),' ', '_') as decline_desc
        from i-dss-streaming-data.payment_ops_sandbox.cybs_txn
        where 1=1
            and date(requestdate) >= date('2025-05-15')
            and overallrcode = 0
            and merchantid = 'cbsi_entertainment'
    )
, txn_inv_sub as
    (
        select
            txn.*
            -- add earliest & most recent decline info
            -- , txn.trans_msg_desc
            -- , txn.failure_type
            -- , txn.gateway_error_cd
            , adj.invoice_nbr
            , adj.adj_amt
            , adj.invoice_billed_dt_ut
            , adj.invoice_state_desc
            , adj.invoice_closed_dt_ut
            , adj.invoice_type_cd
            , sub.subscription_guid as sub_guid
            , datetime(sub.activate_dt_ut, 'America/New_York') as activate_dt_et
            , coalesce(cybs_decl.issuerresponsecode, txn.gateway_error_cd) as decline_cd
            , coalesce(case 
                        when txn.gateway_error_cd = '481' and trans_gateway_type_desc = 'cybersource' then 'fraud_gateway' 
                        when cybs_decl.decline_desc is not null then cybs_decl.decline_desc
                        else null
                    end, txn.failure_type) as decline_desc
        from txn
        left join adj
            on adj.src_system_id = txn.src_system_id
            and adj.account_cd = txn.account_cd
            and adj.invoice_guid = txn.invoice_guid
        left join sub
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and sub.subscription_guid = txn.txn_subscription_guid
        left join cybs_decl
            on cybs_decl.requestid = txn.reference_cd
        where 1=1
    )

, first_and_ct as
    (
        select
            x.*
            , case
                when txn_type = 'purch_on_sub'
                    then max(case when trans_status_desc_2 = 'success' then 1 else 0 end) over (partition by src_system_id, account_cd, txn_type, invoice_guid)
                when txn_type = 'verify_or_purch_pre_sub'
                    then max(case when trans_status_desc_2 = 'success' then 1 else 0 end) over (partition by src_system_id, account_cd, trans_type_desc, dt_first_attempt)
            end as has_success
            , case
                when txn_type = 'purch_on_sub'
                    then max(case when trans_status_desc_2 = 'declined' then 1 else 0 end) over (partition by src_system_id, account_cd, txn_type, invoice_guid)
                when txn_type = 'verify_or_purch_pre_sub'
                    then max(case when trans_status_desc_2 = 'declined' then 1 else 0 end) over (partition by src_system_id, account_cd, trans_type_desc, dt_first_attempt) -- use trans_type_desc as partition since it can include purch or verify depending on subscription_guid being populated or not
            end as has_declined
        from    
            (
                select distinct
                    src_system_id
                    , account_cd
                    , invoice_guid
                    , transaction_guid
                    , txn_type
                    , trans_type_desc
                    , trans_status_desc_2
                    , case
                        when txn_type = 'purch_on_sub'
                            then min(trans_dt) over (partition by src_system_id, account_cd, txn_type, invoice_guid)
                        when txn_type = 'verify_or_purch_pre_sub' 
                            then trans_dt
                    end as dt_first_attempt
                    , case
                        when txn_type = 'purch_on_sub'
                            then count(transaction_guid) over (partition by src_system_id, account_cd, txn_type, invoice_guid, trans_status_desc_2)
                        when txn_type = 'verify_or_purch_pre_sub' 
                            then count(transaction_guid) over (partition by src_system_id, account_cd, trans_type_desc, trans_dt, trans_status_desc_2) -- use trans_type_desc as partition since it can include purch or verify depending on subscription_guid being populated or not
                    end as attempts
                    , case
                        when txn_type = 'purch_on_sub'
                        -- -- -- (ifnull(trim(failure_type),'') in ('fraud_gateway') and trans_msg_desc in ('FRAUD','The order has been rejected by Decision Manager'))
                        -- -- -- (gateway_error_cd = '481' and trans_gateway_type_desc = 'cybersource') or (ifnull(trim(failure_type),'') in ('fraud_gateway') and trans_gateway_type_desc = 'adyen')
                            then count(case when ifnull(trim(failure_type),'') = 'fraud_gateway' then null else transaction_guid end) over (partition by src_system_id, account_cd, txn_type, invoice_guid, trans_status_desc_2)
                        when txn_type = 'verify_or_purch_pre_sub' 
                            then count(case when ifnull(trim(failure_type),'') = 'fraud_gateway' then null else transaction_guid end) over (partition by src_system_id, account_cd, trans_type_desc, trans_dt, trans_status_desc_2) -- use trans_type_desc as partition since it can include purch or verify depending on subscription_guid being populated or not
                    end as attempts_no_fraud
                from txn_inv_sub
                where 1=1
                    and 
                        (
                            (
                                txn_type = 'purch_on_sub'
                                and
                                    (
                                        date(activate_dt_et) >= date('2025-04-01')
                                        or 
                                        sub_guid is null
                                    )
                            )
                            or
                            txn_type = 'verify_or_purch_pre_sub'
                        )
            ) x
        where 1=1
            and dt_first_attempt >= date('2025-05-15')
    )

, txn_classify as
    (
        select distinct
            txn.* 
            , case
                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                        and 
                            (
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and
                                        (
                                            (
                                                txn.trans_dt_ut between sub.prior_activation and sub.post_activate
                                                and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                            )
                                            or
                                            eversub.account_cd is null
                                        )
                                )
                                or
                                (
                                    txn.trans_status_desc in ('success', 'void')
                                    and txn.trans_dt_ut between sub.prior_activation and sub.post_activate 
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                            )
                    then 'auth_initial'

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and
                            (
                                (
                                    txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                    and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND)
                                    -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE) 
                                    and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                )
                            )
                    then 'auth_restart'

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                    then 'auth_card-update'
                
                when txn.trans_type_desc = 'verify'
                    then 'auth_other'
/*
^^^ subscription based classification, auth/verify
*/
            -- single transactions for a direct to pay activation
                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'purchase' 
                        and
                            (
                                (
                                    txn.trans_status_desc in ('success', 'void', 'declined')
                                    and txn.trans_dt_ut between sub.prior_activation and sub.post_activate 
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and eversub.account_cd is null
                                )
                            )
                    then 'purch_initial'

            -- all transactions associated with invoice for direct to pay (weird process that activates a trial subscription, changes trial to a few minutes, then a renewal invoice generated and it's more like trial to paid)
            -- 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e95d4afdfcedc647f7340acab2559
                -- when txn.origin_desc in ('token_api', 'api', 'recurring') 
                        -- and txn.trans_type_desc = 'purchase'
                    when txn.trans_type_desc = 'purchase'
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.activate_dt_ut and date_add(sk.post_activate, interval 8 MINUTE)
                        and sk.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                        -- and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                        -- and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                    then 'purch_initial_trial_removed' 

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'purchase' 
                        and 
                            (
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and
                                        (
                                            txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                            and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                                        )
                                        or
                                        (
                                            txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND) 
                                            -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE) 
                                            and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                        )
                                )
                                or
                                (
                                    txn.trans_status_desc in ('success', 'void')
                                    and
                                        (
                                            txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                            -- txn.trans_dt_ut between date_add(sub.prior_expiration, interval -30 SECOND) and sub.post_activate 
                                            and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                        )
                                )
                            )
                    then 'purch_restart'

            -- all transactions associated with invoice for direct to pay (weird process that activates a trial subscription, changes trial to a few minutes, then a renewal invoice generated and it's more like trial to paid)
             -- 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e861e51eb6ff1f1f1c74f278c8a31
                -- when txn.origin_desc in ('token_api', 'api', 'recurring') 
                        -- and txn.trans_type_desc = 'purchase'
                    when txn.trans_type_desc = 'purchase'
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.activate_dt_ut and date_add(sk.post_activate, interval 8 MINUTE)
                        and sk.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                        -- and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                        -- and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                    then 'purch_restart_trial_removed' 
/*
^^^ subscription based classification, direct to paid
*/
                when txn.trans_type_desc = 'purchase' 
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.pre_trial_end and sk.post_trial_end 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                    then 'purch_trial-to-paid'  -- invoice within a few minutes of trial ending

                when txn.trans_type_desc = 'purchase' 
                        and txn.invoice_type_cd in ('renewal', 'immediate_change')
                        and txn.invoice_billed_dt_ut between sk.post_trial_end and date_add(sk.post_trial_end, interval 75 MINUTE)
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                    then 'purch_trial-upgrade-to-paid' -- upgrading from essential to premium while in trial causes trial to end & invoiced for premium amt (takes like an hour for the invoice to get generated)
/*
^^^ subscription based classification, trial to paid
*/
                when txn.invoice_type_cd = 'renewal'
                    then 'recurring' -- renewal invoice (any txn type associated with it will be recurring)

                -- when txn.origin_desc in ('token_api') and txn.trans_type_desc = 'purchase' and adj.invoice_type_cd = 'renewal'
                --         and txn.trans_status_desc in ('success', 'void', 'declined')
                --     then 'purch_dunning-cust'

                -- when txn.origin_desc in ('external_recovery') and txn.trans_type_desc = 'purchase' and adj.invoice_type_cd = 'renewal' 
                --         and txn.trans_status_desc in ('success', 'void', 'declined')
                --     then 'purch_dunning-vindicia'

                -- when txn.origin_desc = 'recurring' and txn.inv_to_txn_mins > 5
                --     then 'purch_dunning-automated'

                when txn.origin_desc = 'force_collect' and txn.trans_type_desc = 'purchase' 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                    then 'purch_auto-card-update'

                -- when txn.origin_desc in ('recurly_admin_sub_ch', 'api_sub_change')
                when txn.trans_type_desc = 'purchase'
                        and 
                            (
                                txn.invoice_type_cd = 'immediate_change'
                                or
                                txn.origin_desc in ('recurly_admin_sub_ch', 'api_sub_change')
                            )
                    then 'purch_sub_change'

                else 'unclassified_' || txn.origin_desc || '_' || ifnull(txn.invoice_type_cd,'')
                -- else null
            end as txn_classify
            -- , ref.transaction_guid as refund_guid
            -- , ref.trans_dt_ut as refund_ts
            -- , ref.trans_amt as refund_amt
            -- , ref.tax_amt as refund_tax
            -- , ref.currency_cd as refund_currency
            -- , sub.* except (subscription_guid, activate_dt_ut, src_system, src_system_id, account_cd)

            , max(coalesce(txn.txn_subscription_guid, sub.subscription_guid, sk.subscription_guid)) over (partition by txn.src_system_id, txn.transaction_guid) as subscription_guid
            , max(coalesce(sub.plan_cd, sk.plan_cd)) over (partition by txn.src_system_id, txn.transaction_guid) as plan_cd
            , max(coalesce(sub.plan_nm, sk.plan_nm)) over (partition by txn.src_system_id, txn.transaction_guid) as plan_nm
            , max(coalesce(sub.plan_dur, sk.plan_dur)) over (partition by txn.src_system_id, txn.transaction_guid) as plan_dur
            , max(coalesce(sub.original_activation, sk.original_activation)) over (partition by txn.src_system_id, txn.transaction_guid) as original_activation
            , max(coalesce(sub.creation_dt_ut, sk.creation_dt_ut)) over (partition by txn.src_system_id, txn.transaction_guid) as creation_dt_ut
            , max(coalesce(sub.activate_dt_ut, sk.activate_dt_ut)) over (partition by txn.src_system_id, txn.transaction_guid) as activate_dt_ut
            , max(coalesce(sub.trial_start_dt_ut, sk.trial_start_dt_ut)) over (partition by txn.src_system_id, txn.transaction_guid) as trial_start_dt_ut
            , max(coalesce(sub.trial_end_dt_ut, sk.trial_end_dt_ut)) over (partition by txn.src_system_id, txn.transaction_guid) as trial_end_dt_ut
            , max(coalesce(sub.expiration_dt_ut, sk.expiration_dt_ut)) over (partition by txn.src_system_id, txn.transaction_guid) as expiration_dt_ut
            , max(coalesce(sub.prior_expiration, sk.prior_expiration)) over (partition by txn.src_system_id, txn.transaction_guid) as prior_expiration
            , max(coalesce(sub.next_activation, sk.next_activation)) over (partition by txn.src_system_id, txn.transaction_guid) as next_activation

            -- , coalesce(txn.txn_subscription_guid, sub.subscription_guid, sk.subscription_guid) as subscription_guid
            -- , coalesce(sub.plan_cd, sk.plan_cd) as plan_cd
            -- , coalesce(sub.plan_nm, sk.plan_nm) as plan_nm
            -- , coalesce(sub.plan_dur, sk.plan_dur) as plan_dur
            -- , coalesce(sub.original_activation, sk.original_activation) as original_activation
            -- , coalesce(sub.creation_dt_ut, sk.creation_dt_ut) as creation_dt_ut
            -- , coalesce(sub.activate_dt_ut, sk.activate_dt_ut) as activate_dt_ut
            -- , coalesce(sub.trial_start_dt_ut, sk.trial_start_dt_ut) as trial_start_dt_ut
            -- , coalesce(sub.trial_end_dt_ut, sk.trial_end_dt_ut) as trial_end_dt_ut
            -- , coalesce(sub.expiration_dt_ut, sk.expiration_dt_ut) as expiration_dt_ut
            -- , coalesce(sub.prior_expiration, sk.prior_expiration) as prior_expiration
            -- , coalesce(sub.next_activation, sk.next_activation) as next_activation
            
        from txn_inv_sub txn
        left join sub eversub
            on eversub.src_system_id = txn.src_system_id
            and eversub.account_cd = txn.account_cd
        left join sub -- using subscription dates associated with the account rather than joining on subscription_guid to ensure declines before activation are categorized
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and
                (
                    ( --success near an activation where subsn guids match (purch) or no subsn guid (verify)
                        txn.trans_dt_ut between sub.pre_activate and sub.post_activate
                        and txn.trans_status_desc in ('success','void')
                        and txn.origin_desc in ('token_api', 'api')
                        and
                            (
                                (
                                    txn.trans_type_desc = 'purchase' 
                                    and txn.txn_subscription_guid = sub.subscription_guid
                                )
                                or
                                (
                                    txn.trans_type_desc = 'verify'
                                    and ifnull(trim(txn.txn_subscription_guid),'') = ''
                                )
                            )
                    )
                    or
                    ( -- declines 
                        ifnull(trim(txn.txn_subscription_guid),'') = ''
                        and txn.origin_desc in ('token_api', 'api')
                        and
                            (
                                (
                                    -- purchase declined before initial activation or verify, shouldn't be a subsn guid since no success 
                                    txn.trans_dt_ut between sub.prior_activation and sub.post_activate
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    -- decline on restart attempt that eventually is successful
                                    txn.trans_dt_ut between date_add(sub.prior_expiration, interval -30 SECOND) and sub.post_activate 
                                    and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    -- purchase declined on restart attempt that is not ever successful (no next_activation)
                                    txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND)
                                    -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE)
                                    and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                )
                            )
                        and
                            (
                                (
                                    txn.trans_type_desc = 'purchase'
                                    and txn.trans_status_desc = 'declined'
                                )
                                or
                                (
                                    txn.trans_type_desc = 'verify'
                                    and txn.trans_status_desc in ('void','declined')
                                )
                            )
                    )
                    -- or
                    -- ( 
                    --     txn.origin_desc in ('recurring') 
                    --     and txn.trans_type_desc = 'purchase' 
                    --     and txn.trans_status_desc in ('success', 'void', 'declined')
                    --     and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                    --     and 
                    --         (
                    --             -- restart 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e861e51eb6ff1f1f1c74f278c8a31
                    --             sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                    --             or
                    --             -- initial 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e95d4afdfcedc647f7340acab2559
                    --             sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                    --         )
                    -- )
                )
        left join sub sk -- join using subscription guid for all others
            on sk.src_system_id = txn.src_system_id
            and sk.account_cd = txn.account_cd
            and sk.subscription_guid = txn.txn_subscription_guid
        -- left join inv
        --     on inv.account_cd = txn.account_cd
        --     and inv.src_system_id = txn.src_system_id
        --     and inv.billed_dt_ut between sub.pre_trial_end and sub.post_trial_end 
        --     and inv.invoice_guid = txn.invoice_guid
        -- left join txn ref
        --     on ref.account_cd = txn.account_cd
        --     and ref.src_system_id = txn.src_system_id
        --     and ref.orig_transaction_guid = txn.transaction_guid
        --     and ref.trans_type_desc = 'refund'
        where 1=1
            -- and txn.trans_type_desc in ('purchase','verify')
            -- and txn.trans_status_desc in ('success', 'void', 'declined')
    ) 

, txn_row_nums as
    (
        select
            txn.*
            , fac.dt_first_attempt
            , fac.attempts
            , fac.attempts_no_fraud
            , fac.has_success
            , fac.has_declined
            , case
                when txn.txn_type = 'purch_on_sub'
                    then row_number() over (partition by txn.src_system_id, txn.account_cd, txn.invoice_guid, txn.trans_status_desc_2 order by txn.trans_dt_ut desc)
                when txn.txn_type = 'verify_or_purch_pre_sub'
                    then row_number() over (partition by txn.src_system_id, txn.account_cd, txn.trans_type_desc, fac.dt_first_attempt, txn.trans_status_desc_2 order by txn.trans_dt_ut desc) -- use trans_type_desc as partition since it can include purch or verify depending on subscription_guid being populated or not
            end as row_status
            , case
                when txn.txn_type = 'purch_on_sub'
                    then row_number() over (partition by txn.src_system_id, txn.account_cd, fac.dt_first_attempt, txn.trans_status_desc_2 order by txn.trans_dt_ut desc)
                when txn.txn_type = 'verify_or_purch_pre_sub'
                    then 0
            end as row_dt_status
        from txn_classify txn
        join first_and_ct fac
            on fac.src_system_id = txn.src_system_id
            and fac.account_cd = txn.account_cd
            and fac.transaction_guid = txn.transaction_guid
            -- and fac.invoice_guid = txn.invoice_guid
            -- -- and fac.dt_first_attempt = txn.trans_dt
            -- and fac.trans_status_desc_2 = txn.trans_status_desc_2
            -- and fac.cc_first_6_nbr = txn.cc_first_6_nbr
        where 1=1
            and
                (
                    (
                        txn.txn_type = 'purch_on_sub'
                        and txn.latest_txn = 1
                    )
                    or
                    (
                        txn.txn_type = 'verify_or_purch_pre_sub'
                        and txn.verify_latest_txn = 1
                    )
                )
    )

, cts as
    (
        select
            trn.* except (txn_classify)
            , case
                when trn.txn_type = 'verify_or_purch_pre_sub' and trn.trans_status_desc_2 = 'success' and trn.row_status = 1 then 1
                when trn.txn_type = 'verify_or_purch_pre_sub' and trn.trans_status_desc_2 = 'declined' and trn.has_success = 0 and trn.row_status = 1 then 1
                when trn.txn_type = 'purch_on_sub' and trn.trans_status_desc_2 = 'success' and trn.row_dt_status = 1 then 1
                when trn.txn_type = 'purch_on_sub' and trn.trans_status_desc_2 = 'declined' and trn.has_success = 0 and trn.row_dt_status = 1 then 1
                else 0
            end as account_count
            , case
                when txn_classify like 'auth%' then 'trial_verify' -- all verify
                -- when txn_classify in ('auth_initial','auth_restart') then 'trial_verify'
                -- when txn_classify like 'auth%' then 'verify_other' 
                when txn_classify like 'purch_trial%' then 'trial_to_paid'
                when txn_classify in ('purch_initial', 'purch_initial_trial_removed', 'purch_restart', 'purch_restart_trial_removed', 'purch_auto-card-update', 'purch_sub-change') then 'direct_to_paid' -- include sub change?
                -- when txn_classify in ('purch_initial', 'purch_initial_trial_removed', 'purch_restart', 'purch_restart_trial_removed', 'purch_auto-card-update') then 'direct_to_paid' 
                -- when txn_classify = 'purch_sub-change' then 'sub_change'
                when txn_classify = 'recurring' then 'renewals'
                else txn_classify
            end as txn_classify
        from txn_row_nums trn
        where 1=1
    )

-- select
--     transaction_guid 
--     , reference_cd
--     , txn_classify
--     , origin
--     , account_cd
--     , subscription_guid
--     , invoice_guid
--     , invoice_nbr
--     , trans_dt
--     , first_attempt_dt 
--     , trans_dt_et
--     , case
--         when src_system_id = 115 then 'Domestic'
--     end as region
--     , trans_type_desc
--     , trans_status_desc_2
--     , trans_gateway_type_desc
--     , failure_type
--     , cc_first_6_nbr
--     , plan_dur as plan_cd_duration
--     , adj_amt as invoice_amt
--     , attempts
--     , trial_start_dt 
--     , gateway_cd
--     , gateway_error_cd
--     , has_success
--     , account_count
-- from cts
-- where 1=1
--     and trans_gateway_type_desc in ('adyen','cybersource')
--     and txn_classify in ('trial_verify', 'direct_to_paid', 'renewals', 'trial_to_paid')


select
    date_trunc(dt_first_attempt, MONTH) as mth
    -- , date_trunc(dt_first_attempt, WEEK) as wk
    -- ,
    -- dt_first_attempt
    , trans_gateway_type_desc
    , txn_classify
    -- , case 
    --     when origin_desc in ('recurring','force_collect', 'api_sub_change') then 'contAuth'
    --     else 'eCommerce' 
    -- end as origin
    , coalesce(sum(account_count),0) as order_ct
    , coalesce(sum(case when trans_status_desc_2 = 'success' then account_count else null end),0) as order_success_ct
    , coalesce(sum(case when trans_status_desc_2 = 'declined' then account_count else null end),0) as order_declined_ct
    , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then account_count else null end),0), sum(account_count))),5) as order_success_pct

    , coalesce(sum(attempts),0) as txn_ct
    , coalesce(sum(case when trans_status_desc_2 = 'success' then attempts else null end),0) as txn_success_ct
    , coalesce(sum(case when trans_status_desc_2 = 'declined' then attempts else null end),0) as txn_declined_ct
    , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then attempts else null end),0), sum(attempts))),5) as txn_success_pct

    , coalesce(sum(attempts_no_fraud),0) as no_fraud_txn_ct
    , coalesce(sum(case when trans_status_desc_2 = 'success' then attempts_no_fraud else null end),0) as no_fraud_txn_success_ct
    , coalesce(sum(case when trans_status_desc_2 = 'declined' then attempts_no_fraud else null end),0) as no_fraud_txn_declined_ct
    , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then attempts_no_fraud else null end),0), sum(attempts_no_fraud))),5) as no_fraud_txn_success_pct

    , min(case when trans_status_desc_2 = 'success' then reference_cd else null end) as success_ref_ex1
    , max(case when trans_status_desc_2 = 'success' then reference_cd else null end) as success_ref_ex2
    , min(case when trans_status_desc_2 = 'declined' then reference_cd else null end) as declined_ref_ex1
    , max(case when trans_status_desc_2 = 'declined' then reference_cd else null end) as declined_ref_ex2

    , min(case when origin_desc in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'success' then reference_cd else null end) as contauth_success_ref_ex1
    , max(case when origin_desc in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'success' then reference_cd else null end) as contauth_success_ref_ex2
    , min(case when origin_desc in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'declined' then reference_cd else null end) as contauth_declined_ref_ex1
    , max(case when origin_desc in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'declined' then reference_cd else null end) as contauth_declined_ref_ex2

    , min(case when origin_desc not in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'success' then reference_cd else null end) as ecomm_success_ref_ex1
    , max(case when origin_desc not in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'success' then reference_cd else null end) as ecomm_success_ref_ex2
    , min(case when origin_desc not in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'declined' then reference_cd else null end) as ecomm_declined_ref_ex1
    , max(case when origin_desc not in ('recurring','force_collect', 'api_sub_change') and trans_status_desc_2 = 'declined' then reference_cd else null end) as ecomm_declined_ref_ex2
from cts
where 1=1
    and trans_gateway_type_desc in ('adyen','cybersource')
    and txn_classify in ('trial_verify', 'direct_to_paid', 'renewals', 'trial_to_paid')
group by all
order by 1,2,3,4,5,6


/*
bins
*/
-- select
--     *
-- from
--     (
--         select
--             date_trunc(dt_first_attempt, MONTH) as mth
--             -- , date_trunc(dt_first_attempt, WEEK) as wk
--             -- ,
--             -- dt_first_attempt
--             , trans_gateway_type_desc
--             , txn_classify
--             , cc_first_6_nbr
--             -- , case 
--             --     when origin_desc in ('recurring','force_collect', 'api_sub_change') then 'contAuth'
--             --     else 'eCommerce' 
--             -- end as origin
--             , coalesce(sum(account_count),0) as order_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'success' then account_count else null end),0) as order_success_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'declined' then account_count else null end),0) as order_declined_ct
--             , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then account_count else null end),0), sum(account_count))),5) as order_success_pct

--             , coalesce(sum(attempts),0) as txn_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'success' then attempts else null end),0) as txn_success_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'declined' then attempts else null end),0) as txn_declined_ct
--             , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then attempts else null end),0), sum(attempts))),5) as txn_success_pct

--             , coalesce(sum(attempts_no_fraud),0) as no_fraud_txn_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'success' then attempts_no_fraud else null end),0) as no_fraud_txn_success_ct
--             , coalesce(sum(case when trans_status_desc_2 = 'declined' then attempts_no_fraud else null end),0) as no_fraud_txn_declined_ct
--             , round((safe_divide(coalesce(sum(case when trans_status_desc_2 = 'success' then attempts_no_fraud else null end),0), sum(attempts_no_fraud))),5) as no_fraud_txn_success_pct

--         from cts
--         where 1=1
--             and trans_gateway_type_desc in ('adyen','cybersource')
--             and txn_classify in ('trial_verify', 'direct_to_paid', 'renewals', 'trial_to_paid')
--         group by all
--     ) x
-- where 1=1
-- qualify row_number() over (partition by mth, trans_gateway_type_desc, txn_classify order by txn_ct desc) <= 50
-- -- qualify row_number() over (partition by mth, trans_gateway_type_desc, txn_classify order by no_fraud_txn_ct desc) <= 50
-- order by 1,2,3,4,5,6




/*
decline codes
*/
-- select
--     *
-- from
--     (
--         select
--             date_trunc(dt_first_attempt, MONTH) as mth
--             -- , date_trunc(dt_first_attempt, WEEK) as wk
--             -- ,
--             -- dt_first_attempt
--             , trans_gateway_type_desc
--             , txn_classify
--            -- , cc_type_desc
--             , decline_cd
--             , decline_desc
--             -- , case 
--             --     when origin_desc in ('recurring','force_collect', 'api_sub_change') then 'contAuth'
--             --     else 'eCommerce' 
--             -- end as origin
--             , coalesce(sum(account_count),0) as order_ct

--             , coalesce(sum(attempts),0) as txn_ct

--            -- , coalesce(sum(attempts_no_fraud),0) as no_fraud_txn_ct
--             , min(transaction_guid) as ex1
--             , max(transaction_guid) as ex2              

--         from cts
--         where 1=1
--             and trans_gateway_type_desc in ('adyen','cybersource')
--             and txn_classify in ('trial_verify', 'direct_to_paid', 'renewals', 'trial_to_paid')
--             and trans_status_desc_2 = 'declined'
--         group by all
--     ) x
-- where 1=1
-- qualify row_number() over (partition by mth, trans_gateway_type_desc, txn_classify order by txn_ct desc) <= 10
-- -- qualify row_number() over (partition by mth, trans_gateway_type_desc, txn_classify order by no_fraud_txn_ct desc) <= 10
-- order by 1,2,3,4,5,6