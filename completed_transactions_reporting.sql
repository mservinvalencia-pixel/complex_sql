-- Example analytical SQL query for portfolio purposes
-- Demonstrates:
-- 1. Multi-CTE query design
-- 2. JSON field extraction
-- 3. Joins between transactional and attribute-based tables
-- 4. Timezone standardization
-- 5. Reporting segmentation and aggregation
--
-- Note:
-- All table names, field names, and business mappings were anonymized
-- for confidentiality and portfolio presentation purposes.

WITH transactions AS (
  SELECT
    entity_id,
    version_id,
    event_timestamp
  FROM transaction_base_table
  WHERE record_type = 'target_type'
),

transaction_identifiers AS (
  SELECT
    entity_id,
    version_id,
    get_json_string(attribute_value, '$.S') AS entity_reference
  FROM transaction_attributes_table
  WHERE attribute_name = 'entity_reference'
),

amount_values AS (
  SELECT
    entity_id,
    version_id,
    get_json_string(attribute_value, '$.N') AS amount_value
  FROM transaction_attributes_table
  WHERE attribute_name = 'amount'
),

currency_values AS (
  SELECT
    entity_id,
    version_id,
    get_json_string(attribute_value, '$.S') AS currency_code
  FROM transaction_attributes_table
  WHERE attribute_name = 'currency'
),

channel_values AS (
  SELECT
    entity_id,
    version_id,
    get_json_string(attribute_value, '$.S') AS channel_type
  FROM transaction_attributes_table
  WHERE attribute_name = 'channel'
),

payment_method_lookup AS (
  SELECT
    b.entity_reference,
    get_json_string(a.attribute_value, '$.S') AS payment_method
  FROM reference_base_table b
  JOIN reference_attributes_table a
    ON b.entity_id = a.entity_id
   AND b.version_id = a.version_id
  WHERE a.attribute_name = 'payment_method'
),

final_dataset AS (
  SELECT
    ti.entity_reference,
    CONVERT_TZ(
      FROM_UNIXTIME(t.event_timestamp),
      'UTC',
      'America/Los_Angeles'
    ) AS event_date,
    pml.payment_method,
    -1 * CAST(av.amount_value AS DOUBLE) AS amount_value,
    ch.channel_type,
    cur.currency_code,
    CASE
      WHEN instr(ti.entity_reference, '-') > 1
        THEN substring(ti.entity_reference, 1, instr(ti.entity_reference, '-') - 1)
      ELSE 'unknown_group'
    END AS group_code
  FROM transactions t
  JOIN transaction_identifiers ti
    ON t.entity_id = ti.entity_id
   AND t.version_id = ti.version_id
  LEFT JOIN amount_values av
    ON t.entity_id = av.entity_id
   AND t.version_id = av.version_id
  LEFT JOIN currency_values cur
    ON t.entity_id = cur.entity_id
   AND t.version_id = cur.version_id
  LEFT JOIN channel_values ch
    ON t.entity_id = ch.entity_id
   AND t.version_id = ch.version_id
  LEFT JOIN payment_method_lookup pml
    ON ti.entity_reference = pml.entity_reference
  WHERE t.event_timestamp >= UNIX_TIMESTAMP('2025-01-01')
)

SELECT
  DATE(event_date) AS event_date,
  payment_method,
  currency_code,
  CASE
    WHEN group_code IN ('group_a1', 'group_a2') THEN 'Region_A'
    WHEN group_code IN ('group_b1', 'group_b2', 'group_b3') THEN 'Region_B'
    WHEN group_code IN ('group_c1') THEN 'Region_C'
    ELSE 'Other_Region'
  END AS business_segment,
  COUNT(DISTINCT entity_reference) AS transaction_count,
  ROUND(SUM(amount_value), 2) AS total_amount
FROM final_dataset
WHERE channel_type = 'online'
GROUP BY
  DATE(event_date),
  payment_method,
  currency_code,
  business_segment;
