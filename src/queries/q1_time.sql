select
tweet_date
,username
from
(
SELECT
    tweet_date,
    username,
    RANK() OVER (PARTITION BY tweet_date ORDER BY tweet_count DESC) AS rank
FROM (
    SELECT
        DATE(date) AS tweet_date,
        user.username AS username,
        COUNT(1) AS tweet_count
    FROM {dataset_id}.{table_id}
    WHERE DATE(date) IN (
    SELECT DATE(date)
    FROM {dataset_id}.{table_id}
    GROUP BY DATE(date)
    ORDER BY COUNT(1) DESC
    LIMIT 10
)
    GROUP BY tweet_date, username
) AS aggregated_data
)
where rank=1
order by tweet_date asc