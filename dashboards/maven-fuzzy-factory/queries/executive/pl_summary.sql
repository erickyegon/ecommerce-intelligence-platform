-- P&L Summary (Unpivoted)
-- Uses stack() to unpivot P&L metrics into metric/amount format
-- Displays: Gross Revenue, Total COGS, Total Refunds, Net Margin

SELECT
  stack(
    4,
    'Gross Revenue', total_gross_revenue,
    'Total COGS', -(total_gross_revenue - total_net_margin - total_refund_value),
    'Total Refunds', -total_refund_value,
    'Net Margin', total_net_margin
  ) AS (metric, amount)
FROM workspace.mff_gold.executive_kpis;
