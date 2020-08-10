CREATE TABLE IF NOT EXISTS simple_records.message_status_by_request
(
    requestId           UUID,
    messageId           UUID,
    lastUpdatedDateTime timestamp,
    status              text,
    deliveryResponse    text,
    PRIMARY KEY ((requestId), messageId, lastUpdatedDateTime),
) WITH CLUSTERING ORDER BY (messageId DESC, lastUpdatedDateTime DESC);

