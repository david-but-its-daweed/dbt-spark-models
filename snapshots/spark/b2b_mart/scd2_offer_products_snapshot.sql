{% snapshot scd2_offer_products_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='offer_product_id',

      strategy='check',
      check_cols=['created_time_msk', 'product_id', 'offer_id', 'disabled', 'type'],
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}



SELECT
    _id AS offer_product_id,
    id AS product_id,
    offerId AS offer_id,
    trademark AS trademark,
    hsCode AS hs_code,
    manufacturerId AS manufacturer_id,
    name AS name,
    nameInv AS name_inv,
    type AS type,
    disabled AS disabled,
    link AS link,
    logisticFields.isCertificationRequired AS is_certification_required,
    logisticFields.isAgencyRegistrationRequired AS is_agency_registration_required,
    logisticFields.agencyName AS agency_name,
    millis_to_ts_msk(ctms+1) AS created_time_msk
FROM mongo.b2b_core_offer_products_daily_snapshot
{% endsnapshot %}
