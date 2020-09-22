CREATE MATERIALIZED VIEW agg_coupon_customers AS  WITH hbd AS (
         SELECT row_number() OVER (PARTITION BY coupons.customer ORDER BY coupons.expires_dt) AS row_no,
            coupons.coupon_event,
            coupons.coupon_event_name,
            coupons.coupon,
            coupons.customer,
            coupons.s_dt,
            coupons.f_dt,
            coupons.expires_dt,
            coupons.used
           FROM coupons
          WHERE coupons.expires_dt >= CURRENT_DATE AND coupons.coupon_event::text ~~ 'HBD%'::text
        ), not_hbd AS (
         SELECT row_number() OVER (PARTITION BY coupons.customer ORDER BY coupons.expires_dt) AS row_no,
            coupons.coupon_event,
            coupons.coupon_event_name,
            coupons.coupon,
            coupons.customer,
            coupons.s_dt,
            coupons.f_dt,
            coupons.expires_dt,
            coupons.used
           FROM coupons
          WHERE coupons.expires_dt >= CURRENT_DATE AND coupons.coupon_event::text !~~ 'HBD%'::text
        )
 SELECT c.guid,
    c.customer,
    c.line_id,
    c.birth,
    c.skin_type,
    c.skin_trouble,
    c.point,
    c.expires_dt,
    hbd_t.coupon_event_name AS hb_coupon_event_name,
    hbd_t.s_dt AS hb_coupon_s_dt,
    hbd_t.expires_dt AS hb_coupon_expires_dt,
    hbd_t.used AS hb_coupon_used,
    hbd_1.coupon_event_name AS coupon_event_name_1,
    hbd_1.s_dt AS coupon_s_dt_1,
    hbd_1.expires_dt AS coupon_expires_dt_1,
    hbd_1.used AS coupon_used_1,
    hbd_2.coupon_event_name AS coupon_event_name_2,
    hbd_2.s_dt AS coupon_s_dt_2,
    hbd_2.expires_dt AS coupon_expires_dt_2,
    hbd_2.used AS coupon_used_2
   FROM customers c
     LEFT JOIN ( SELECT hbd.row_no,
            hbd.coupon_event,
            hbd.coupon_event_name,
            hbd.coupon,
            hbd.customer,
            hbd.s_dt,
            hbd.f_dt,
            hbd.expires_dt,
            hbd.used
           FROM hbd
          WHERE hbd.row_no = 1) hbd_t ON c.customer = hbd_t.customer::text
     LEFT JOIN ( SELECT not_hbd.row_no,
            not_hbd.coupon_event,
            not_hbd.coupon_event_name,
            not_hbd.coupon,
            not_hbd.customer,
            not_hbd.s_dt,
            not_hbd.f_dt,
            not_hbd.expires_dt,
            not_hbd.used
           FROM not_hbd
          WHERE not_hbd.row_no = 1) hbd_1 ON c.customer = hbd_1.customer::text
     LEFT JOIN ( SELECT not_hbd.row_no,
            not_hbd.coupon_event,
            not_hbd.coupon_event_name,
            not_hbd.coupon,
            not_hbd.customer,
            not_hbd.s_dt,
            not_hbd.f_dt,
            not_hbd.expires_dt,
            not_hbd.used
           FROM not_hbd
          WHERE not_hbd.row_no = 2) hbd_2 ON c.customer = hbd_2.customer::text;
