 WITH mentioned_users AS (
SELECT
A.id,
mentionedUsers.username AS mention
FROM {dataset_id}.{table_id} A,
UNNEST(mentionedUsers) AS mentionedUsers
)
SELECT
mention AS username,
COUNT(*) AS mention_count
FROM mentioned_users
GROUP BY mention
ORDER BY mention_count DESC
LIMIT 10;