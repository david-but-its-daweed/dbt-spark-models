{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov',
      'team': 'merchant',
    },
) }}
SELECT
    _id AS order_id,
    fid AS friendly_id,
    ogid AS order_group_id,
    NAMED_STRUCT(
        'kind', src.k,
        'id', src.id
    ) AS source,
    IF(mpid IS NOT NULL, NAMED_STRUCT(
        'marketplace', mpid.m,
        'id', mpid.id
    ), NULL) AS marketplace_id,
    ctt AS creation_triggered_time_utc,
    ci.t AS marketplace_created_time,
    ELEMENT_AT(TRANSFORM(FILTER(st.st, element -> element.s == 0), x -> x.t), 1) AS created_time_utc,
    ELEMENT_AT(TRANSFORM(FILTER(st.st, element -> element.s == 1), x -> x.t), 1) AS fulfilled_online_time_utc,
    ELEMENT_AT(TRANSFORM(FILTER(st.st, element -> element.s == 2), x -> x.t), 1) AS shipped_time_utc,
    upd AS updated_time_utc,
    mid AS merchant_id,
    `sid` AS store_id,
    pi.p AS product_id,
    pi.v AS variant_id,
    pi.q AS quantity,
    ci.a.c AS country,
    ci.jci.pdr.`uid` AS user_id,
    ci.jci.dpid AS delivery_point_id,
    IF(pi.g IS NOT NULL, NAMED_STRUCT(
        'product_id', pi.g.pid,
        'variant_id', pi.g.vid
    ), NULL) AS gift,
    IF(pi.sh.opt IS NOT NULL, pi.sh.opt.whid, NULL) AS warehouse_id,
    CASE
        WHEN pi.sh.t = 1 THEN 'merchant'
        WHEN pi.sh.t = 2 THEN 'JoomLogistics'
        ELSE ''
    END AS selected_shipping_type,
    IF(pi.sh.p IS NOT NULL, NAMED_STRUCT(
        'amount', CAST(pi.sh.p.amount AS DOUBLE) / 1000000,
        'currency', pi.sh.p.ccy
    ), NULL) AS selected_shipping_price,
    NAMED_STRUCT(
        'merchant_currency', mi.m.c,
        'customer_gmv', CAST(mi.c.gmv AS DOUBLE) / 1000000,
        'merchant_gmv', CAST(mi.m.t AS DOUBLE) / 1000000,
        'merchant_revenue', CAST(mi.m.r AS DOUBLE) / 1000000,
        'merchant_unit_price', CAST(mi.m.up AS DOUBLE) / 1000000,
        'merchant_refund_amount', CAST(mi.m.ra AS DOUBLE) / 1000000,
        'merchant_revenue_from_invoice', CAST(mi.m.ri AS DOUBLE) / 1000000,
        'customer_vat', CAST(mi.c.vat AS DOUBLE) / 1000000,
        'merchant_vat', CAST(mi.m.v AS DOUBLE) / 1000000,
        'merchant_extra_weight_chanrge', CAST(mi.m.ewc AS DOUBLE) / 1000000,
        'merchant_gmv_usd', CAST(mi.m.td AS DOUBLE) / 1000000,
        'logistics_price', IF(mi.l IS NOT NULL, CAST(mi.l.sp AS DOUBLE) / 1000000, NULL),
        'logistics_ndi_compensation', IF(mi.l IS NOT NULL, CAST(mi.l.nc AS DOUBLE) / 1000000, NULL),
        'logistics_freebie_charge', IF(mi.l IS NOT NULL, CAST(mi.l.fsc AS DOUBLE) / 1000000, NULL),
        'logistics_price_usd', IF(mi.l IS NOT NULL, CAST(mi.l.pusd AS DOUBLE) / 1000000, NULL),
        'discounts', IF(mi.d IS NOT NULL, TRANSFORM(mi.d.ds, discount -> NAMED_STRUCT(
            'amount', CAST(discount.a AS DOUBLE) / 1000000,
            'type', CASE
                WHEN discount.t = 1 THEN 'other'
                WHEN discount.t = 2 THEN 'competitivePriceReduction'
                WHEN discount.t = 3 THEN 'limitedTimeOffer'
                WHEN discount.t = 4 THEN 'specialPromotion'
                WHEN discount.t = 5 THEN 'smart'
                WHEN discount.t = 6 THEN 'freebie'
                WHEN discount.t = 7 THEN 'padPost'
                WHEN discount.t = 8 THEN 'merchantPromotion'
                WHEN discount.t = 9 THEN 'couponForSubscription'
                WHEN discount.t = 10 THEN 'priceElasticity'
                WHEN discount.t = 11 THEN 'salePriceSchedule'
                ELSE 'other'
            END,
            'reference_id', COALESCE(discount.cid, discount.spid, discount.btid, discount.`mpid`, discount.peid, discount.frid)
        )), NULL),
        'take_rate', mi.tr
    ) AS money_info,
    CASE
        WHEN ELEMENT_AT(st.st,1).s = 0 THEN 'created'
        WHEN ELEMENT_AT(st.st,1).s = 1 THEN 'fulfilledOnline'
        WHEN ELEMENT_AT(st.st,1).s = 2 THEN 'shipped'
        WHEN ELEMENT_AT(st.st,1).s = 3 THEN 'cancelledByMerchant'
        WHEN ELEMENT_AT(st.st,1).s = 4 THEN 'paidByJoomRefund'
        WHEN ELEMENT_AT(st.st,1).s = 5 THEN 'refunded'
        WHEN ELEMENT_AT(st.st,1).s = 6 THEN 'returnInitiated'
        WHEN ELEMENT_AT(st.st,1).s = 7 THEN 'returnExpired'
        WHEN ELEMENT_AT(st.st,1).s = 8 THEN 'returnArrived'
        WHEN ELEMENT_AT(st.st,1).s = 9 THEN 'returnCompleted'
        WHEN ELEMENT_AT(st.st,1).s = 10 THEN 'returnDeclined'
        WHEN ELEMENT_AT(st.st,1).s = 11 THEN 'cancelledByJL'
        ELSE ''
    END AS `status`,
    IF(st.sh IS NOT NULL, st.sh.tn, NULL) AS tracking_number,
    IF(st.sh IS NOT NULL, st.sh.`sid`, NULL) AS shipper_id,
    IF(st.sh IS NOT NULL, st.sh.`oid`, NULL) AS online_order_id,
    IF(st.rv IS NOT NULL, st.rv.r, NULL) AS rating,
    st.if AS is_fraud,
    IF(st.ref IS NOT NULL, NAMED_STRUCT(
        'time_utc', st.ref.t,
        'fraction', st.ref.f,
        'customer_reason', CASE
            WHEN st.ref.cr = 0 THEN NULL
            WHEN st.ref.cr = 1 THEN 'cancelledByCustomer'
            WHEN st.ref.cr = 2 THEN 'notDelivered'
            WHEN st.ref.cr = 3 THEN 'emptyPackage'
            WHEN st.ref.cr = 4 THEN 'badQuality'
            WHEN st.ref.cr = 5 THEN 'damaged'
            WHEN st.ref.cr = 6 THEN 'wrongProduct'
            WHEN st.ref.cr = 7 THEN 'wrongQuantity'
            WHEN st.ref.cr = 8 THEN 'wrongSize'
            WHEN st.ref.cr = 9 THEN 'wrongColor'
            WHEN st.ref.cr = 10 THEN 'minorProblems'
            WHEN st.ref.cr = 11 THEN 'differentFromImages'
            WHEN st.ref.cr = 12 THEN 'other'
            WHEN st.ref.cr = 13 THEN 'counterfeit'
            WHEN st.ref.cr = 14 THEN 'sentBackToMerchant'
            WHEN st.ref.cr = 15 THEN 'returnedToMerchantByShipper'
            WHEN st.ref.cr = 16 THEN 'misleadingInformation'
            WHEN st.ref.cr = 17 THEN 'fulfillByJoomUnavailable'
            WHEN st.ref.cr = 18 THEN 'approveRejected'
            WHEN st.ref.cr = 19 THEN 'productBanned'
            WHEN st.ref.cr = 20 THEN 'packageIncomplete'
            WHEN st.ref.cr = 21 THEN 'notAsDescribed'
            WHEN st.ref.cr = 22 THEN 'missingProofOfDelivery'
            WHEN st.ref.cr = 23 THEN 'deliveredToWrongAddress'
            WHEN st.ref.cr = 24 THEN 'turnedBackByPostOffice'
            WHEN st.ref.cr = 25 THEN 'bannedByCustoms'
            WHEN st.ref.cr = 26 THEN 'incorrectConsolidation'
            WHEN st.ref.cr = 27 THEN 'groupPurchaseExpired'
            WHEN st.ref.cr = 28 THEN 'returnedToMerchantInOrigin'
            WHEN st.ref.cr = 29 THEN 'vatOnDelivery'
            WHEN st.ref.cr = 30 THEN 'shippingUnavailable'
            WHEN st.ref.cr = 31 THEN 'freeItemCancel'
            WHEN st.ref.cr = 32 THEN 'threeForTwoCancel'
            WHEN st.ref.cr = 33 THEN 'bmplCancel'
            WHEN st.ref.cr = 34 THEN 'couponAllOrderCancelBehaviour'
            WHEN st.ref.cr IS NOT NULL THEN 'other'
            ELSE NULL
        END,
        'merchant_reason', CASE
            WHEN st.ref.mr = 0 THEN NULL 
            WHEN st.ref.mr = 1 THEN 'unableToFulfill'
            WHEN st.ref.mr = 2 THEN 'outOfStock'
            WHEN st.ref.mr = 3 THEN 'wrongAddress'
            WHEN st.ref.mr = 4 THEN 'notShippedOnTime'
            WHEN st.ref.mr IS NOT NULL THEN 'other'
            ELSE NULL
        END
    ), NULL) AS refund,
    IF(st.ret IS NOT NULL, NAMED_STRUCT(
        'time_utc', st.ret.t,
        'decline_reason', CASE
            WHEN st.ret.dr = 0 THEN NULL
            WHEN st.ret.dr = 1 THEN 'incorrectItem'
            WHEN st.ret.dr = 2 THEN 'usedItem'
            WHEN st.ret.dr = 3 THEN 'damagedItem'
            WHEN st.ret.dr = 4 THEN 'other'
            WHEN st.ret.dr IS NOT NULL THEN 'other'
            ELSE NULL
        END
    ), NULL) AS `return`,
    IF(st.cjl IS NOT NULL, NAMED_STRUCT(
        'time_utc', st.cjl.ct,
        'reason', CASE
            WHEN st.cjl.reason = 0 THEN 'other'
            WHEN st.cjl.reason = 1 THEN 'prohibitedGoodsAutoDestroy'
            WHEN st.cjl.reason = 2 THEN 'overweight'
            WHEN st.cjl.reason = 3 THEN 'oversize'
            WHEN st.cjl.reason = 4 THEN 'prohibitedGoods'
            WHEN st.cjl.reason = 5 THEN 'damaged'
            WHEN st.cjl.reason = 6 THEN 'invalidParcelBarcode'
            WHEN st.cjl.reason = 7 THEN 'multipleParcelLabels'
            WHEN st.cjl.reason = 8 THEN 'alreadyCancelledOrder'
            WHEN st.cjl.reason = 9 THEN 'returnByShipper'
            WHEN st.cjl.reason IS NOT NULL THEN 'other'
            ELSE NULL
        END,
        'reaction', NAMED_STRUCT(       
            'deadline_time_utc', st.cjl.reaction.d,
            'time_utc', st.cjl.reaction.rt,
            'state', CASE
                WHEN st.cjl.reaction.s = 1 THEN 'needsReaction'
                WHEN st.cjl.reaction.s = 2 THEN 'destroyed'
                WHEN st.cjl.reaction.s = 3 THEN 'returned'
                WHEN st.cjl.reaction.s = 4 THEN 'selfPickupRequested'
                WHEN st.cjl.reaction.s = 5 THEN 'selfPickupCompleted'
                WHEN st.cjl.reaction.s = 6 THEN 'selfPickupDestroyed'
                ELSE ''
            END,
            'author', CASE
                WHEN st.cjl.reaction.a = 1 THEN 'merchantDefault'
                WHEN st.cjl.reaction.a = 2 THEN 'merchantCustom'
                WHEN st.cjl.reaction.a = 3 THEN 'warehouseAuto'
                ELSE NULL
            END,
            'account_id', st.cjl.reaction.acc,
            'pickup_address_id', st.cjl.reaction.pa,
            'self_pickup_date', IF(st.cjl.reaction.spi IS NOT NULL, st.cjl.reaction.spi.pd, NULL)
        ),
        'tracking_number', st.cjl.tn,
        'online_order_id', st.cjl.oon,
        'supported_return_methods', IF(st.cjl.srm IS NOT NULL, TRANSFORM(st.cjl.srm, method -> CASE
                WHEN method = 1 THEN 'courier'
                WHEN method = 20 THEN 'selfPickup'
                ELSE 'other'
            END
        ), NULL)
    ), NULL) AS cancelled_by_jl_info,
    cft,
    ci.t AS user_ordered_time_utc
FROM {{ source('mongo', 'merchant_order_orders_daily_snapshot') }}
