CREATE TABLE IF NOT EXISTS simple_records.message_status
(
    messageId           uuid,
    lastUpdatedDateTime timestamp,
    status              text,
    deliveryResponse    text,
    primary key ((messageId), lastUpdatedDateTime)
) WITH CLUSTERING ORDER BY (lastUpdatedDateTime DESC);
