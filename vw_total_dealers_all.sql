SELECT
    ym_s_date,
    COALESCE(ym_s_clientID, 0) AS ym_s_clientID,
    COALESCE(visitID, 0) AS visitID,
    visitDuration,
    source_detail,
    source,
    pageViews,
    num_user,
    grouping,
    dateTime_visit,
    count_visit,
    count_goals,
    content,
    cid,
    bounce
FROM sttgaz.vw_total_dealers_all