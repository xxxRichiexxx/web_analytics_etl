SELECT
    "ID Сессии в Я.Метрика (заявки)",
    "Дата события CRM",
    "Заявки",
    "Рабочие листы",
    "Договора",
    "Выданные ТС",
    grouping,
    source_detail,
    medium,
    source,
    COALESCE(ym_s_visitID, 0) AS ym_s_visitID,
    COALESCE(ym_s_counterID, 0) AS ym_s_counterID,
    COALESCE(cid, 0) AS cid,
    format_ads
FROM sttgaz.vw_worklists_models_dealers_all