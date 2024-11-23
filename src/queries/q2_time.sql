WITH cleaned_data AS (
  SELECT
    REGEXP_REPLACE(content, r'[\x{{1F3FB}}-\x{{1F3FF}}]', '') AS cleaned_content
  FROM {dataset_id}.{table_id}
),
emoji_counts AS (
  SELECT
    emoji,
    COUNT(*) AS count
  FROM (
    SELECT
      emoji
    FROM cleaned_data,
    UNNEST(REGEXP_EXTRACT_ALL(
      cleaned_content,
      r'[\x{{1F600}}-\x{{1F64F}}]|[\x{{1F300}}-\x{{1F5FF}}]|[\x{{1F680}}-\x{{1F6FF}}]|[\x{{2600}}-\x{{26FF}}]|[\x{{2700}}-\x{{27BF}}]|[\x{{1F700}}-\x{{1F77F}}]|[\x{{1F780}}-\x{{1F7FF}}]|[\x{{1F800}}-\x{{1F8FF}}]|[\x{{1F900}}-\x{{1F9FF}}]|[\x{{1FA00}}-\x{{1FA6F}}]|[\x{{1FA70}}-\x{{1FAFF}}]'
    )) AS emoji
  )
  GROUP BY emoji
)
SELECT
  emoji,
  count
FROM emoji_counts
ORDER BY count DESC
LIMIT 10;
