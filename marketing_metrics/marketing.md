

-- marketing_agg_cpc_costs
-- drop table if exists product_analytics.marketing_agg_cpc_costs;
-- create table if not exists product_analytics.marketing_agg_cpc_costs ENGINE = MergeTree() order by(assumeNotNull(dt_c)) AS
select inflow.id                                                                                      id,
        dt_c,
        ga.partner_name                                                                                partner_name,
        if(inflow.id is null, ga.cost_per_day, 0)                                                   as cost_wo_leads,
        count(inflow.id)                                                                               over (partition by toDate(date_requested),partner_name ) as qty_id_p_name_day,
        if(qty_id_p_name_day = 0, 0, if(inflow.id is null, 0, ga.cost_per_day) / qty_id_p_name_day) as cost_per_day
 from es_report.inflow inflow
          full join product_analytics.marketing_partner p on inflow.id = p.id
          full outer join (SELECT coalesce(toDate(ga_date), today() + interval 1 year) as dt_c,
                                  case
                                      when c.ga_campaign like '%_YT_%'
                                          then 'AdWords YouTube'
                                      when c.ga_campaign like '%_UA_%'
                                          then 'MOB-Android app'
                                      when lower(c.ga_campaign) like '%credit_line%'
                                          then 'AdWords Credit line'
                                      when lower(c.ga_campaign) like '%app%'
                                          and c.ga_campaign like '%MMES%'
                                          then 'MOB-Android app'
                                      when lower(c.ga_campaign) like '%brand%'
                                          and c.ga_campaign like '%MMES%'
                                          then 'AdWords Brand'
                                      when c.ga_campaign like '%MMES%'
                                          then 'AdWords Generic'
                                      when lower(c.ga_campaign) like '%brand%'
                                          and lower(c.ga_campaign) like 'es_%'
                                          then 'AdWords Brand'
                                      when lower(c.ga_campaign) like '%awareness%'
                                          and lower(c.ga_campaign) like 'es_%'
                                          then 'AdWords Awareness'
                                      when lower(c.ga_campaign) like 'es_%'
                                          then 'AdWords Generic'
                                      when not (c.ga_campaign like '%MMES%')
                                          then 'AdWords Generic'
                                      else 'no cpc split' end                          as partner_name,
                                  sum(toFloat64(ga_adCost))                            as cost_per_day
                           FROM es_airbyte_google_analytics.ga_costs c
                           WHERE ga_source = 'google'
                             and ga_medium = 'cpc'
                             and toFloat64(ga_adCost) > 0
                           GROUP BY dt_c, partner_name
                           UNION ALL
                           SELECT coalesce(toDate(TimePeriod), today() + interval 1 year) as dt_c,
                                  'CPC - Bing BRAND'                                      as partner_name,
                                  sum(Spend)                                              as cost_per_day
                           FROM es_airbyte_bing_ads.account_performance_report_daily
                           GROUP BY TimePeriod) ga
                          on ga.dt_c = toDate(inflow.date_requested) and
                             ga.partner_name = p.partner_name
     settings join_use_nulls = 1

--marketing_cpc_costs
--drop table if exists product_analytics.marketing_cpc_costs;
--create table if not exists product_analytics.marketing_cpc_costs ENGINE = MergeTree() order by(assumeNotNull(dt_c)) AS
SELECT coalesce(toDate(ga_date), today() + interval 1 year) as dt_c,
                                             case
                                                 when c.ga_campaign like '%_YT_%'
                                                     then 'AdWords YouTube'
                                                 when c.ga_campaign like '%_UA_%'
                                                     then 'MOB-Android app'
                                                 when lower(c.ga_campaign) like '%credit_line%'
                                                     then 'AdWords Credit line'
                                                 when lower(c.ga_campaign) like '%app%'
                                                     and c.ga_campaign like '%MMES%'
                                                     then 'MOB-Android app'
                                                 when lower(c.ga_campaign) like '%brand%'
                                                     and c.ga_campaign like '%MMES%'
                                                     then 'AdWords Brand'
                                                 when c.ga_campaign like '%MMES%'
                                                     then 'AdWords Generic'
                                                 when lower(c.ga_campaign) like '%brand%'
                                                     and lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Brand'
                                                 when lower(c.ga_campaign) like '%awareness%'
                                                     and lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Awareness'
                                                 when lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Generic'
                                                 when not (c.ga_campaign like '%MMES%')
                                                     then 'AdWords Generic'
                                                 else 'no cpc split' end                          as partner_name,
                                             sum(toFloat64(ga_adCost))                            as cost_per_day
                                      FROM es_airbyte_google_analytics.ga_costs c
                                      WHERE ga_source = 'google'
                                        and ga_medium = 'cpc'
                                        and toFloat64(ga_adCost) > 0
                                      GROUP BY dt_c, partner_name
                                      UNION ALL
                                      SELECT coalesce(toDate(TimePeriod), today() + interval 1 year) as dt_c,
                                             'CPC - Bing BRAND'                                      as partner_name,
                                             sum(Spend)                                              as cost_per_day
                                      FROM es_airbyte_bing_ads.account_performance_report_daily
                                      GROUP BY TimePeriod
-- partner
-- drop table if exists product_analytics.marketing_partner;
-- create table if not exists product_analytics.marketing_partner ENGINE = MergeTree() order by(assumeNotNull(id)) AS
select inflow.id                                                                                     id
           , toDate(date_requested)                                                                        date_requested
           , initial_amount
           , if(group_name = 'APP MObile' and partner_name in
                                              ('Exponea sms', 'Exponea email', 'Mobile push', 'Web push',
                                               'Weblayer banner', 'In Apps banner'), 'CRM', group_name) as group_name
           , issued_ind
           , type_of_borrower
           , borrower_category
           , inflow.partner_id                                                                             partner_id
           , coalesce(case
                          when isNull(p.partner_table_name)
                              then tr_source.medium
                          when (isNull(tr_source.medium) and isNull(p.partner_table_name)) or
                               p.partner_table_name in ('MOB-Android app', 'MOB-iOS app')
                              then coalesce(pushes, banners, p.partner_table_name)
                          else p.partner_table_name end, 'organic')                                     as partner_name
           , count(id)                                                                                     over(partition by toDate(date_requested), partner_name) as qty_id_p_name_day
      from es_report.inflow inflow
               left join(SELECT case
                                    when name = 'AdWords'
                                        or name = 'Adwords AF'
                                        or name = 'AdWords Generic'
                                        then 'AdWords Generic'
                                    when name = 'GOOGLE ADWORDS BRAND'
                                        or name = 'AdWords Moneyman Brand'
                                        then 'AdWords Brand'
                                    when name = 'GOOGLE ADWORDS AWARENESS'
                                        then 'AdWords Awareness'
                                    when name = 'GOOGLE ADWORDS CREDIT LINE'
                                        then 'AdWords Credit line'
                                    when name = 'GOOGLE ADWORDS YOUTUBE'
                                        then 'AdWords YouTube'
                                    when name = 'AFF-Goprestamo'
                                        then 'Goprestamo CPC'
                                    when name = 'IDFinance Spain S.A.U'
                                        then 'Goprestamo CPC'
                                    when name = 'AFF-  Adservice'
                                        then 'AFF - Adservice'
                                    else pp.name
                                    end as partner_table_name,
                                partner_lead_id,
                                pg.name as group_name
                         FROM es_debezium.partner pp
                                  left join es_debezium.partner_group pg
                                            on pp.partner_group_id = pg.id) p
                        on p.partner_lead_id = inflow.partner_id
               left join(select id,
                                argMin(case
                                           when medium = 'cpc' and
                                                source = 'google' and
                                                campaign LIKE '%_YT_%'
                                               then 'AdWords YouTube'
                                           when medium = 'cpc' and
                                                source = 'google' and
                                                lower(campaign) LIKE '%brand%'
                                               then 'AdWords Brand'
                                           when medium = 'cpc' and
                                                source = 'google' and
                                                lower(campaign) LIKE '%awareness%'
                                               then 'AdWords Awareness'
                                           when medium = 'cpc' and
                                                source = 'google' and
                                                lower(campaign) LIKE '%credit_line%'
                                               then 'AdWords credit line'
                                           when medium = 'cpc' and
                                                source = 'google'
                                               then 'AdWords Generic'
                                           when medium = 'affiliate' and
                                                source = 'credy'
                                               then 'AFF-Credy-NO-API'
                                           when medium = 'affiliate' and
                                                source = 'solcredito'
                                               then 'AFF - Solcredito API'
                                           when medium = 'affiliate' and
                                                source = 'solcredito_dm'
                                               then 'AFF - Solcredito DM'
                                           when medium = 'sms' and
                                                lower(source) = 'exponea'
                                               then 'Exponea sms'
                                           when medium = 'email' and
                                                lower(source) = 'exponea'
                                               then 'Exponea email'
                                           when medium = 'banner' and
                                                source = 'crt'
                                               then 'Criteo'
                                           when medium = 'affiliate' and
                                                source = 'adservice'
                                               then 'AFF - Adservice'
                                           when medium = 'email' and
                                                source = 'antvniopln'
                                               then 'AFF- Antevenio_PLN'
                                           when medium = 'email' and
                                                source = 'antvniopl'
                                               then 'AFF- Antevenio'
                                           when medium = 'sms' and
                                                source = 'atv_sms'
                                               then 'AFF- Antevenio sms'
                                           when medium = 'sms' and
                                                source = 'fcsms'
                                               then 'AFF- Fiestacrédito sms'
                                           when medium = 'email' and
                                                source = 'fsc'
                                               then 'AFF- Fiestacrédito'
                                           when medium = 'sms' and
                                                source = 'scdt_sms'
                                               then 'AFF - Solcredito sms'
                                           when medium = 'sms' and
                                                source = 'cdmarket_sms'
                                               then 'AFF- Credimarket sms'
                                           when medium = 'sms' and
                                                source = 'yntd'
                                               then 'AFF- Younited sms'
                                           when medium = 'sms' and
                                                source = 'ntx_sms'
                                               then 'AFF- Natexo sms'
                                           when medium = 'sms' and
                                                source = 'vlsms'
                                               then 'AFF- Volsor sms'
                                           when medium = 'sms' and
                                                source = 'l2w'
                                               then 'AFF- Lead2Win sms'
                                           when medium = 'sms' and
                                                source = 'ctz_sms'
                                               then 'AFF-CREDITOZEN SMS'
                                           when medium = 'referral' and
                                                source = 'goprestamo.es'
                                               then 'Goprestamo Organic'
                                           when medium = 'goprecpc' and
                                                source = 'goprestamo'
                                               then 'Goprestamo CPC'
                                           when source in ('www-moneyman-es.cdn.ampproject.org', 'prozaem.kz') and
                                                medium = 'referral'
                                               then 'organic'
                                           when medium = '(none)' and
                                                toDate(date) between today() - interval 2 day and today()
                                               then 'pending partners'
                                           when medium = '(none)'
                                               then null
                                           else medium end, id) as medium
                         from es_report.trans_source
                         where date > '2021-01-01'
                         group by id) tr_source
                        on tr_source.id = inflow.id
               left join(select id,
                                multiIf(type = 'inapp', 'In Apps banner', type = 'Weblayer', 'Weblayer banner',
                                        'Direct') banners
                         from product_analytics.exponea_banner
                         where banner_action_click = 1
                           and toDate(date_requested) between toDate(show_time) and toDate(show_time) + interval 1 day) banners
                        on banners.id = inflow.id
               left join(select id,
                                multiIf(action_type = 'mobile notification', 'Mobile push',
                                        action_type = 'browser notification', 'Web push', 'Direct') pushes
                         from product_analytics.exponea_credit_report_fin
                         where action_type = 'mobile notification'
                            or action_type = 'browser notification'
                             and cam_action_clicked = 1
                             and
                               toDate(date_requested) between toDate(sent_timestamp) and toDate(sent_timestamp) + interval 1 day) push
                        on push.id = inflow.id
          settings join_use_nulls = 1

-- marketing_agg_costs
-- drop table if exists product_analytics.marketing_agg_costs;
-- create table if not exists product_analytics.marketing_agg_costs ENGINE = MergeTree() order by(assumeNotNull(id)) AS

with [100864, 100053, 100857,
    100782, 100766, 100661,
    100652, 100644, 100518,
    100426, 100425, 100262,
    100240, 100158, 101082] as list_of_api_partners
      select inflow.id                                                                                       id,
             initial_amount,
             group_name,
             issued_ind,
             type_of_borrower,
             borrower_category,
             has(list_of_api_partners, cpa.id)                        as is_full_api,
             case
                 when inflow.partner_name in ('MOB-iOS app', 'MOB-Android app')
                     then coalesce(cpa.partner, inflow.partner_name)
                 else coalesce(inflow.partner_name, cpc.partner_name) end                                 as partner_name,
             coalesce(inflow.date_requested, cpc.dt_c)                                                       date_requested,
             if(inflow.id is null, cpc.cost_per_day, 0)                                                   as cost_wo_leads,
             if(qty_id_p_name_day = 0, 0,
                if(inflow.id is null, 0, cpc.cost_per_day) / qty_id_p_name_day)                           as cost_per_day,
             cpl,
             cps,
             spendings
      from product_analytics.marketing_partner inflow
               full outer join product_analytics.marketing_cpc_costs cpc on cpc.dt_c = inflow.date_requested and
                                                                            cpc.partner_name = inflow.partner_name
               left join product_analytics.marketing_cpa_costs cpa on cpa.apl_id = inflow.id
          settings join_use_nulls = 1


-- marketing_final_costs

-- drop table if exists product_analytics.marketing_final_costs;
-- create table if not exists product_analytics.marketing_final_costs ENGINE = MergeTree() order by(assumeNotNull(date_requested)) AS

select count(id)                                                  as cnt,
             toDate(date_requested)                                     as date_requested,
             partner_name,
             type_of_borrower,
             borrower_category,
             ifNull(group_name, 'Not found')                            as group_name,
             is_full_api,
             countIf(id, issued_ind = 1)                                as is_loan,
             sumIf(initial_amount, issued_ind = 1)                      as issuanse_new,
             sum(ifNull(cost_per_day, 0))                               as cost_per_day_sum,
             sum(ifNull(cpl, 0))                                        as cpl_w_vat,
             sum(ifNull(cps, 0))                                        as cps_w_vat,
             sum(ifNull(spendings, 0))                                  as spendings,
             sum(ifNull(cost_wo_leads, 0))                              as cost_wo_leads
from product_analytics.marketing_agg_costs
group by date_requested, partner_name, type_of_borrower, borrower_category, group_name, is_full_api

--------
web-analytics/AIRFLOW_DAGS/sql_scripts/MMES_marketing/cpa_costs.sql
-- drop table if exists product_analytics.marketing_cpa_costs;
-- create table if not exists product_analytics.marketing_cpa_costs ENGINE = MergeTree() order by(assumeNotNull(id)) AS
SELECT id,
                                 partner,
                                 date_requested,
                                 arrayFirstOrNull(elem_num, df, dt -> assumeNotNull(df <= toDate(date_requested) and
                                                                      dt >= toDate(date_requested)),
                                    arrayEnumerate(df_arr),
                                    df_arr,
                                    dt_arr)                                            as sp_num,
                   cpl_arr[sp_num] * (1 + vat_arr[sp_num])                             as cpl,
                   if(issued_ind != 1, null, cps_arr[sp_num] * (1 + vat_arr[sp_num]))  as cps,
                   spendings_arr[sp_num] * (1 + vat_arr[sp_num]) / cnt_apl_arr[sp_num] as spendings,
                                 apl_id

                          FROM (SELECT id,
                                       partner,
                                       date_requested,
                                       issued_ind,
                                       groupArray(ifNull(df, toDate('1970-01-01')))      as df_arr,
                                       groupArray(ifNull(dt, today() + interval 1 year)) as dt_arr,
                                       groupArray(ifNull(cpl, 0))                        as cpl_arr,
                                       groupArray(ifNull(cps, 0))                        as cps_arr,
                                       groupArray(ifNull(vat, 0))                        as vat_arr,
                                       groupArray(ifNull(spendings, 0))                  as spendings_arr,
                                       groupArray(ifNull(cnt_apl, 1))                    as cnt_apl_arr,
                                       arrayReduce('groupUniqArray',
                                                   arrayFlatten(groupArray(credit_ids))) as apl_ids
                                FROM (SELECT id,
                                             coalesce(f_at.date_requested, toDate(c2.date_requested)) as date_requested,
                                             partner,
                                             issued_ind,
                                             df,
                                             dt,
                                             cpl,
                                             cps,
                                             vat,
                                             spendings,
                                             cnt_apl_1,
                                             countIf(c2.id, c2.date_requested >= df and c2.date_requested <= dt) as cnt_apl_2,
                                             arrayFilter((i, dr)->assumeNotNull(dr) >= df and
                                                                  assumeNotNull(dr) <= dt,
                                                         groupArray(c2.id),
                                                         groupArray(c2.date_requested))                          as credit_ids_2,
                                             max2(cnt_apl_2, cnt_apl_1)                                          as cnt_apl,
                                             if(cnt_apl_2 > cnt_apl_1, credit_ids_2, credit_ids_1)               as credit_ids
                                      FROM (SELECT af.id                                                    as id,
                                                   toDate(c.date_requested)                                 as date_requested,
                                                   coalesce(af.partner, c.partner_name)                           as partner,
                                                   issued_ind,
                                                   af.date_from                                             as df,
                                                   coalesce(af.date_to,
                                                            today() + interval 1 year)                      as dt, --always future
                                                   af.cpl                                                   as cpl,
                                                   af.cps                                                   as cps,
                                                   af.vat                                                   as vat,
                                                   af.spendings                                             as spendings,
                                                   countIf(c.id,
                                                           toDate(c.date_requested) >= df and
                                                           toDate(c.date_requested) <= dt)                  as cnt_apl_1,
                                                   arrayFilter((i, dr)->assumeNotNull(dr >= df and
                                                                                      dr <= dt),
                                                               groupArray(assumeNotNull(c.id)),
                                                               groupArray(assumeNotNull(c.date_requested))) as credit_ids_1,
                                                   countIf(c.id,
                                                           c.status in ('ACTIVE', 'COMPLETED', 'EXPIRED', 'SOLD') and
                                                           toDate(c.date_requested) >= df and
                                                           toDate(c.date_requested) <= dt)                  as cnt_cr_1
                                            FROM (select c.id     as                               id
                                                       , issued_ind
                                                       , date_requested
                                                       , c.status as                               status
                                                       , coalesce(c.partner_id, p.partner_id) partner_id
                                                       , partner_name
                                                  from es_debezium.credit c
                                                           left join product_analytics.marketing_partner p on p.id = c.id) c
                                                     right join es_debezium.partner p
                                                                on c.partner_id = p.partner_lead_id
                                                     right join es_upload.affiliate_costs af
                                                                on af.id = p.id
                                            GROUP BY id, toDate(c.date_requested), partner, issued_ind, df, dt, cpl, cps, vat, spendings) f_at -- first attempt -> join via id
                                               left join (select id,
                                                                 date_requested,
                                                                 status,
                                                                 coalesce(android_partner_name, ios_partner_name, '') as appsflyer_partner_name,
                                                                 coalesce(aps_andr.ev_time, aps_andr.ev_time)         as ev_time
                                                          from es_debezium.credit cr
                                                                   left join (select toDate(min(event_time))        as ev_time,
                                                                                     case
                                                                                         when af_prt = 'gowithmedia'
                                                                                             then 'AFF- Go with Media'
                                                                                         when af_prt = 'alfaleadsagency'
                                                                                             then 'AFF- Alfaleads'
                                                                                         when af_prt = 'Mobisharks'
                                                                                             then 'AFF- Mobisharks'
                                                                                         when af_prt = '3dot14'
                                                                                             then 'AFF- 3dot14'
                                                                                         when af_prt = 'adsyntheticinc'
                                                                                             then 'AFF-Ad Synthetic'
                                                                                         when af_prt = 'adxpro'
                                                                                             then 'AFF-Ad Synthetic'
                                                                                         when af_prt = 'iconpeak'
                                                                                             then 'IconPeak'
                                                                                         when af_prt is null
                                                                                             then 'restricted'
                                                                                         else 'other' end           as ios_partner_name,
                                                                                     extract(event_value, '[0-9]+') as credit_id
                                                                              from es_airbyte_appsflyer.ios_in_app_events
                                                                              where event_name = 'loan_request'
                                                                              GROUP BY credit_id, ios_partner_name
                                                                              UNION ALL
                                                                              select toDate(min(event_time))        as ev_time,
                                                                                     'organic ios app'              as ios_partner_name,
                                                                                     extract(event_value, '[0-9]+') as credit_id
                                                                              from es_airbyte_appsflyer.ios_organic_in_app_events
                                                                              where event_name = 'loan_request'
                                                                              GROUP BY credit_id, ios_partner_name) aps_ios
                                                                             on aps_ios.credit_id = toString(cr.id)
                                                                   left join (select toDate(min(event_time))        as ev_time,
                                                                                     case
                                                                                         when af_prt = 'gowithmedia'
                                                                                             then 'AFF- Go with Media'
                                                                                         when af_prt = 'alfaleadsagency'
                                                                                             then 'AFF- Alfaleads'
                                                                                         when af_prt = 'Mobisharks'
                                                                                             then 'AFF- Mobisharks'
                                                                                         when af_prt = '3dot14'
                                                                                             then 'AFF- 3dot14'
                                                                                         when af_prt = 'adsyntheticinc'
                                                                                             then 'AFF-Ad Synthetic'
                                                                                         when af_prt = 'adxpro'
                                                                                             then 'AFF-Ad Synthetic'
                                                                                         when af_prt = 'iconpeak'
                                                                                             then 'IconPeak'
                                                                                         when af_prt is null
                                                                                             then 'restricted'
                                                                                         else 'other' end           as android_partner_name,
                                                                                     extract(event_value, '[0-9]+') as credit_id
                                                                              from es_airbyte_appsflyer.android_in_app_events
                                                                              where event_name = 'loan_request'
                                                                              GROUP BY credit_id, android_partner_name
                                                                              UNION ALL
                                                                              select toDate(min(event_time))        as ev_time,
                                                                                     'organic android app'          as android_partner_name,
                                                                                     extract(event_value, '[0-9]+') as credit_id
                                                                              from es_airbyte_appsflyer.android_organic_in_app_events
                                                                              where event_name = 'loan_request'
                                                                              GROUP BY credit_id, android_partner_name) aps_andr
                                                                             on aps_andr.credit_id = toString(cr.id)
                                                          where appsflyer_partner_name != '') c2 -- second attempt -> join via name
                                                         on c2.appsflyer_partner_name = f_at.partner
                                      GROUP BY id, date_requested, partner, issued_ind, df, dt, cpl, cps, vat, spendings, cnt_apl_1, cnt_cr_1,
                                               credit_ids_1 settings join_use_nulls = 1)
                                GROUP BY id, date_requested, partner, issued_ind)
                                   left array join apl_ids as apl_id



                                   ---------КОНЕЦ



