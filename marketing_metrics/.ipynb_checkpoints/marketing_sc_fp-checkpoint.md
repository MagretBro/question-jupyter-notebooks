
Разбор скрипта для Маркетинговой аналитики ClickHouse SQL, и используется механизм `MergeTree` с временным порядком.


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

---

### 📌 Общая цель:
Создать агрегированную таблицу, в которой по каждой дате и рекламному партнёру (`partner_name`) можно увидеть:
- общую сумму затрат на рекламу (`cost_per_day`)
- количество лидов/заявок (`inflow.id`)
- стоимость лида (`cost_per_day / qty_id`)
- затраты, когда заявок не было (`cost_wo_leads`)

---

## 🔍 Пошаговый разбор

```sql
-- drop table if exists product_analytics.marketing_agg_cpc_costs;
-- create table if not exists product_analytics.marketing_agg_cpc_costs ENGINE = MergeTree() order by(assumeNotNull(dt_c)) AS
```
*Комментарий*: Команды на удаление и создание таблицы закомментированы. Таблица предполагается с типом `MergeTree`, и индексируется по дате `dt_c`.

---

### Основной SELECT-запрос:

```sql
select inflow.id as id,
       dt_c,
       ga.partner_name as partner_name,
```
- `inflow.id` — ID лида (может быть `NULL`)
- `dt_c` — дата (из рекламных данных)
- `partner_name` — наименование рекламного источника (YouTube, Bing и т.д.)

---

```sql
       if(inflow.id is null, ga.cost_per_day, 0) as cost_wo_leads,
```
- Если нет лидов (т.е. заявок нет, `id = null`), то `cost_wo_leads` = `cost_per_day`
- Если лиды есть, то `cost_wo_leads` = 0  
=> метрика «потрачено, но заявок не пришло»

---

```sql
       count(inflow.id) over (partition by toDate(date_requested),partner_name ) as qty_id_p_name_day,
```
- Окружная агрегация: сколько лидов (`id`) пришло по каждому дню и партнёру  
=> метрика «сколько заявок в день от источника»

---

```sql
       if(qty_id_p_name_day = 0, 0, if(inflow.id is null, 0, ga.cost_per_day) / qty_id_p_name_day) as cost_per_day
```
- Если заявок не было (`qty_id = 0`), то `cost_per_day = 0`
- Иначе: `cost_per_day = общая сумма затрат / количество заявок`  
=> **стоимость одного лида**

---

## 🔁 FROM и JOIN’ы:

```sql
from es_report.inflow inflow
full join product_analytics.marketing_partner p on inflow.id = p.id
```
- `inflow` — таблица заявок
- `marketing_partner` — таблица соответствий id → партнер
- `FULL JOIN` — нужна, чтобы остались и заявки без партнёра, и партнёры без заявок

---

```sql
full outer join (
   ...
) ga on ga.dt_c = toDate(inflow.date_requested) and ga.partner_name = p.partner_name
```
- Подзапрос `ga` — агрегация рекламных расходов по дням и партнёрам
- `FULL OUTER JOIN` — сохраняем всё: и даты без лидов, и лиды без затрат

---

## 📦 Подзапрос `ga` — Сбор данных затрат на рекламу

### 🎯 Часть 1: Google Ads (GA)

```sql
SELECT coalesce(toDate(ga_date), today() + interval 1 year) as dt_c,
       case
         when c.ga_campaign like '%_YT_%' then 'AdWords YouTube'
         when c.ga_campaign like '%_UA_%' then 'MOB-Android app'
         ...
         else 'no cpc split'
       end as partner_name,
       sum(toFloat64(ga_adCost)) as cost_per_day
FROM es_airbyte_google_analytics.ga_costs c
WHERE ga_source = 'google'
  and ga_medium = 'cpc'
  and toFloat64(ga_adCost) > 0
GROUP BY dt_c, partner_name
```
- Фильтруем данные Google Ads (`source = google`, `medium = cpc`)
- Классифицируем кампании по имени (`ga_campaign`)
- Группируем по дате и партнёру
- Суммируем расходы

---

### 💻 Часть 2: Bing Ads

```sql
UNION ALL
SELECT coalesce(toDate(TimePeriod), today() + interval 1 year) as dt_c,
       'CPC - Bing BRAND' as partner_name,
       sum(Spend) as cost_per_day
FROM es_airbyte_bing_ads.account_performance_report_daily
GROUP BY TimePeriod
```
- Отдельная часть — расходы по Bing
- Группируем по дате (`TimePeriod`) и задаём фиксированный партнёр `CPC - Bing BRAND`

---

### ⚙️ Конец запроса:

```sql
settings join_use_nulls = 1
```
- Указывает ClickHouse учитывать `NULL`-значения при `JOIN`

---

## 🧠 Вся логика в целом:

1. Собираются **расходы на рекламу** из источников Google и Bing, с группировкой по дате и типу кампании.
2. Эти данные соединяются с **потоком заявок** (`inflow`) по дате и партнёру.
3. Считается:
   - Сколько было заявок (лидов)
   - Сколько потратили, если заявок не было
   - Стоимость лида (при наличии заявок)
4. Всё это сохраняется в таблицу `product_analytics.marketing_agg_cpc_costs`, которая может быть использована для анализа маркетинговой эффективности.

---