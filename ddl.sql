DROP TABLE IF EXISTS dbo.vw_optimatica_dealers
CREATE TABLE dbo.vw_optimatica_dealers
(
    date date,
    month_num numeric(18,0),
    year_n numeric(18,0),
    dealer_name varchar(6000),
    grouping varchar(31),
    model varchar(6000),
    media varchar(6000),
    plan_price float,
    "avg plan price" float,
    "avg fact price" float
);

DROP TABLE IF EXISTS dbo.vw_total_dealers
CREATE TABLE dbo.vw_total_dealers
(
    ym_s_date date,
    ym_s_clientID numeric(30,0),
    num_user int,
    visitID numeric(30,0),
    dateTime_visit datetime,
    grouping varchar(2000),
    source_detail varchar(4000),
    ym_s_startURL_domain text,
    Дилер varchar(58),
    Оптиматика_Дилер varchar(72),
    "Федеральный округ" varchar(65),
    Город varchar(34),
    source varchar(57),
    source_2 varchar(8000),
    medium varchar(4000),
    campaign varchar(4000),
    model varchar(41),
    content varchar(4000),
    cid varchar(4000),
    pageViews int,
    visitDuration int,
    bounce int,
    count_visit int,
    count_goals int
);

DROP TABLE IF EXISTS dbo.vw_total_dealers_all
CREATE TABLE dbo.vw_total_dealers_all
(
    ym_s_date date,
    ym_s_clientID numeric(30,0),
    num_user int,
    visitID numeric(30,0),
    dateTime_visit datetime,
    grouping varchar(2000),
    source_detail varchar(4000),
    source varchar(57),
    content varchar(4000),
    cid varchar(4000),
    pageViews int,
    visitDuration int,
    bounce int,
    count_visit int,
    count_goals int
);

DROP TABLE IF EXISTS dbo.vw_worklists_models_dealers
CREATE TABLE dbo.vw_worklists_models_dealers
(
    "ID Сессии в Я.Метрика (заявки)" varchar(30),
    "Домен (заявки)" varchar(500),
    "Дата события CRM" date,
    Заявки int,
    "Рабочие листы" int,
    Договора int,
    "Выданные ТС" int,
    "Домен метрики" text,
    grouping varchar(2000),
    source_detail varchar(4000),
    medium varchar(4000),
    source varchar(57),
    model varchar(41),
    Дилер varchar(58),
    "Федеральный округ" varchar(65),
    Город varchar(34),
    ym_s_visitID numeric(30,0),
    ym_s_counterID numeric(30,0),
    cid bigint,
    format_ads varchar(33)
);

DROP TABLE IF EXISTS dbo.vw_worklists_models_dealers_all
CREATE TABLE dbo.vw_worklists_models_dealers_all
(
    "ID Сессии в Я.Метрика (заявки)" varchar(30),
    "Дата события CRM" date,
    Заявки int,
    "Рабочие листы" int,
    Договора int,
    "Выданные ТС" int,
    grouping varchar(2000),
    source_detail varchar(4000),
    medium varchar(4000),
    source varchar(57),
    ym_s_visitID numeric(30,0),
    ym_s_counterID numeric(30,0),
    cid bigint,
    format_ads varchar(33)
);