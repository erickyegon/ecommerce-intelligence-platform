-- Session Type Summary - New vs Repeat
-- Calculates sessions, unique users, orders, and CVR for new vs repeat sessions
-- Source: sessions_enriched joined with orders_enriched

SELECT
  CASE WHEN s.is_repeat_session THEN 'Repeat' ELSE 'New' END as session_type,
  COUNT(*) as sessions,
  COUNT(DISTINCT s.user_id) as unique_users,
  SUM(CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END) as orders,
  ROUND(100.0 * SUM(CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as cvr_pct
FROM workspace.mff_gold.sessions_enriched s
LEFT JOIN workspace.mff_gold.orders_enriched o ON s.website_session_id = o.website_session_id
GROUP BY s.is_repeat_session;
