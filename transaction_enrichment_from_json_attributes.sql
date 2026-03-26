WITH payment_lookup AS (
    SELECT
        SUBSTRING(src.record_key, INSTR(src.record_key, 'paymentTypes') + LENGTH('paymentTypes')) AS entity_id,
        CAST(get_json_string(item.value, '$.M.data.L[0].S') AS DOUBLE) AS payment_type_id,
        get_json_string(item.value, '$.M.data.L[1].S') AS payment_type_name,
        get_json_string(item.value, '$.M.data.L[2].S') AS account_ref_id
    FROM generic_lookup_attributes src,
         LATERAL json_each(src.json_value -> 'L') AS item
    WHERE src.record_key LIKE 'v_0:paymentTypes%'
      AND src.attribute_name = 'dataValues'
      AND get_json_string(item.value, '$.M.type.S') = 'row'
),

account_lookup AS (
    SELECT
        SUBSTRING(src.record_key, INSTR(src.record_key, 'accountCatalog') + LENGTH('accountCatalog')) AS entity_id,
        CAST(get_json_string(item.value, '$.M.data.L[0].S') AS DOUBLE) AS account_id,
        get_json_string(item.value, '$.M.data.L[1].S') AS account_name,
        get_json_string(item.value, '$.M.data.L[2].S') AS account_number
    FROM generic_lookup_attributes src,
         LATERAL json_each(src.json_value -> 'L') AS item
    WHERE src.record_key LIKE 'v_0:accountCatalog%'
      AND src.attribute_name = 'dataValues'
      AND get_json_string(item.value, '$.M.type.S') = 'row'
),

mapped_accounts AS (
    SELECT
        p.entity_id,
        p.payment_type_name,
        p.account_ref_id,
        a.account_name,
        a.account_number
    FROM payment_lookup p
    JOIN account_lookup a
      ON p.entity_id = a.entity_id
     AND p.account_ref_id = a.account_id
    WHERE a.account_name LIKE 'Revenue %'
),

config_mapping AS (
    SELECT
        cfg.record_key,
        cfg.config_id,
        cfg.config_name,
        cfg.entity_id,
        get_json_string(element, '$.M.value.S') AS payment_type_id,
        get_json_string(element, '$.M.id.S') AS payment_type_long,
        SUBSTRING(
            get_json_string(element, '$.M.id.S'),
            LOCATE('paymentType-', get_json_string(element, '$.M.id.S')) + LENGTH('paymentType-')
        ) AS payment_type_code,
        SPLIT(
            SUBSTRING(
                get_json_string(element, '$.M.id.S'),
                LOCATE('paymentType-', get_json_string(element, '$.M.id.S')) + LENGTH('paymentType-')
            ),
            '-'
        )[1] AS payment_type_base
    FROM generic_config_base cfg
    CROSS JOIN LATERAL UNNEST(CAST(cfg.config_json AS ARRAY<JSON>)) AS t(element)
    WHERE get_json_string(element, '$.M.id.S') LIKE 'paymentType-%'
      AND get_json_string(element, '$.M.value.S') <> ' '
),

transaction_attributes AS (
    SELECT
        base.entity_id,
        meta.entity_name,
        base.transaction_key,
        base.event_timestamp AS event_timestamp_utc,
        base.transaction_category,

        MAX(CASE WHEN attr.attribute_name = 'transactionType'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS transaction_type,

        MAX(CASE WHEN attr.attribute_name = 'paymentTypeId'
                 THEN get_json_string(attr.attribute_value, '$.N') END) AS payment_type_id,

        COALESCE(
            MAX(CASE WHEN attr.attribute_name = 'cardBrand'
                     THEN get_json_string(attr.attribute_value, '$.S') END),
            MAX(CASE WHEN attr.attribute_name = 'alternateCardBrand'
                     THEN get_json_string(attr.attribute_value, '$.S') END)
        ) AS card_brand,

        MAX(CASE WHEN attr.attribute_name = 'walletType'
                 THEN get_json_string(attr.attribute_value, '$.N') END) AS wallet_type,

        MAX(CASE WHEN attr.attribute_name = 'merchantIdentifier'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS merchant_id,

        MAX(CASE WHEN attr.attribute_name = 'maskedCardNumber'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS masked_card_number,

        MAX(CASE WHEN attr.attribute_name = 'authCode'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS auth_code,

        MAX(CASE WHEN attr.attribute_name = 'referenceNumber'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS reference_number,

        COALESCE(
            MAX(CASE WHEN attr.attribute_name = 'tokenizedReference'
                     THEN get_json_string(attr.attribute_value, '$.S') END),
            MAX(CASE WHEN attr.attribute_name = 'fallbackReference'
                     THEN get_json_string(attr.attribute_value, '$.S') END)
        ) AS external_reference,

        MAX(CASE WHEN attr.attribute_name = 'amount'
                 THEN get_json_string(attr.attribute_value, '$.N') END) AS amount,

        MAX(CASE WHEN attr.attribute_name = 'orderId'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS order_id,

        MAX(CASE WHEN attr.attribute_name = 'confirmationId'
                 THEN get_json_string(attr.attribute_value, '$.S') END) AS confirmation_id

    FROM generic_transaction_base base
    JOIN generic_transaction_attributes attr
      ON base.record_id = attr.record_id
     AND base.record_version = attr.record_version
    JOIN generic_entity_metadata meta
      ON meta.entity_id = base.entity_id
    WHERE convert_tz(from_unixtime(base.event_timestamp), 'UTC', 'America/Los_Angeles') >= '2025-06-01 00:00:00'
      AND convert_tz(from_unixtime(base.event_timestamp), 'UTC', 'America/Los_Angeles') <  '2025-07-01 00:00:00'
      AND attr.attribute_name IN (
          'paymentTypeId',
          'cardBrand',
          'alternateCardBrand',
          'walletType',
          'merchantIdentifier',
          'maskedCardNumber',
          'authCode',
          'referenceNumber',
          'tokenizedReference',
          'fallbackReference',
          'amount',
          'orderId',
          'confirmationId',
          'transactionType'
      )
    GROUP BY
        base.entity_id,
        meta.entity_name,
        base.transaction_key,
        base.event_timestamp,
        base.transaction_category
)

SELECT
    t.entity_name AS entity_name,
    m.account_number AS account_number,
    CONCAT(m.account_number, ' - ', m.account_name) AS account_description,

    DATE_FORMAT(
        convert_tz(from_unixtime(t.event_timestamp_utc), 'UTC', 'America/Los_Angeles'),
        '%Y-%m-%d'
    ) AS transaction_date,

    DATE_FORMAT(
        convert_tz(from_unixtime(t.event_timestamp_utc), 'UTC', 'America/Los_Angeles'),
        '%H:%i:00'
    ) AS transaction_time,

    CASE
        WHEN (
            (CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) < 0
                  THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END)
            -
            (CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) > 0
                  THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END)
        ) >= 0 THEN 'sale'
        ELSE 'refund'
    END AS normalized_transaction_type,

    cfg.payment_type_code AS payment_type,
    t.card_brand,
    CASE t.wallet_type
        WHEN '1' THEN 'digital_wallet_a'
        WHEN '2' THEN 'digital_wallet_b'
        ELSE NULL
    END AS wallet_type,
    CAST(t.merchant_id AS VARCHAR) AS merchant_id,
    t.masked_card_number,
    t.auth_code,
    t.reference_number,
    t.external_reference,

    CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) < 0
         THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END AS debits,

    CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) > 0
         THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END AS credits,

    (CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) < 0
          THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END)
    -
    (CASE WHEN t.transaction_category = 'payment' AND CAST(t.amount AS DOUBLE) > 0
          THEN ABS(CAST(t.amount AS DOUBLE)) ELSE 0 END) AS net_change,

    t.order_id,
    t.confirmation_id,
    agg.itinerary_id,
    t.transaction_key

FROM transaction_attributes t
JOIN config_mapping cfg
  ON cfg.entity_id = t.entity_id
 AND cfg.payment_type_id = t.payment_type_id
JOIN mapped_accounts m
  ON m.entity_id = t.entity_id
 AND cfg.payment_type_base = m.payment_type_name
LEFT JOIN generic_order_aggregation agg
  ON agg.confirmation_id = t.confirmation_id
 AND agg.entity_id = t.entity_id;
