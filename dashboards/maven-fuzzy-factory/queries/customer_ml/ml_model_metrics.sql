-- ML Model Performance Metrics
-- Source: workspace.mff_gold.session_conversion_predictions
-- Calculates precision, recall, accuracy for conversion prediction model
-- AUC-ROC: 0.847 (hardcoded from model evaluation)

-- CUSTOM CALCULATION: f1_score
-- ROUND(2 * (precision * recall) / NULLIF((precision + recall), 0), 3)

SELECT
  ROUND(
    SUM(CASE WHEN predicted_conversion = 1 AND converted_label = 1 THEN 1 ELSE 0 END) * 1.0
    / NULLIF(SUM(CASE WHEN predicted_conversion = 1 THEN 1 ELSE 0 END), 0), 3
  ) as precision,
  ROUND(
    SUM(CASE WHEN predicted_conversion = 1 AND converted_label = 1 THEN 1 ELSE 0 END) * 1.0
    / NULLIF(SUM(CASE WHEN converted_label = 1 THEN 1 ELSE 0 END), 0), 3
  ) as recall,
  ROUND(
    SUM(CASE WHEN predicted_conversion = predicted_label THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3
  ) as accuracy,
  0.847 as auc_roc,
  COUNT(*) as total_sessions
FROM workspace.mff_gold.session_conversion_predictions;
