
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
                                                     and c.ga_campaign like '%EXP_NAME%'
                                                     then 'MOB-Android app'
                                                 when lower(c.ga_campaign) like '%brand%'
                                                     and c.ga_campaign like '%EXP_NAME%'
                                                     then 'AdWords Brand'
                                                 when c.ga_campaign like '%EXP_NAME%'
                                                     then 'AdWords Generic'
                                                 when lower(c.ga_campaign) like '%brand%'
                                                     and lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Brand'
                                                 when lower(c.ga_campaign) like '%awareness%'
                                                     and lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Awareness'
                                                 when lower(c.ga_campaign) like 'es_%'
                                                     then 'AdWords Generic'
                                                 when not (c.ga_campaign like '%EXP_NAME%')
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
