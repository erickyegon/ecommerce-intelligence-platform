-- Customer Segment Count
-- Counts distinct customer segments from KMeans clustering
-- Used for: segment_count counter widget

SELECT 
  COUNT(DISTINCT customer_segment) as segment_count
FROM workspace.mff_gold.customer_segments;
