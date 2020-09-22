CREATE MATERIALIZED VIEW agg_order_customers AS  WITH base AS (
         SELECT o.id,
            o.sales,
            o.detail,
            o.order_id,
            o.customer,
            o.method_name,
            o.subtotal,
            o.point_out,
            o.total,
            o.point,
            o.point_in,
            o.rules,
            o.order_id2,
            o.order_id3,
            o.regular_sales,
            o.order_dt,
            o.ship_dt,
            o.cancel_dt,
            o.return_dt,
            o.site,
            o.source,
            o.status,
            o.affiliate,
            o.advt,
            o.promotion_code,
            o.coupon_event,
            o.devicetype,
            o.block_id,
            o.cancel_reason,
            o.store,
            o.goods,
            o.item_code,
            o.item_code2,
            o.item_code3,
            o.name,
            o.variation_group,
            o.variation_name1,
            o.variation_name2,
            o.qty::double precision AS qty,
                CASE
                    WHEN o.order_dt::date >= '2019-10-01'::date THEN (o.price::numeric / 1.1)::integer::numeric
                    WHEN o.order_dt::date >= '2019-01-21'::date THEN (o.price::numeric / 1.08)::integer::numeric
                    ELSE o.price::numeric
                END AS price,
            o.amt::double precision AS amt,
            o.brand,
            o.regular,
            o.noshi_purpose_name,
            o.sales_name,
            o.sales_name2,
            o.sales_kana,
            o.sales_kana2,
            o.sales_zip,
            o.created_at,
            o.updated_at,
            g.category_large,
            g.category_medium,
            g.category_detail,
            o.order_dt::date AS order_date,
            o.ship_dt::date AS ship_date,
            t_1.max_order_rank,
            g.category_large,
            g.category_medium,
            g.category_line,
            g.product_kbn,
            g.regular_flg,
            g.product_name,
            r.regular_contract_date,
                CASE
                    WHEN g.category_medium::text ~~ '%トライアル%'::text THEN g.category_medium
                    WHEN g.regular_flg::text = '1'::text THEN (g.product_kbn::text || '定期'::text)::character varying
                    WHEN g.product_kbn::text = 'サンプル'::text THEN 'サンプル'::text::character varying
                    WHEN g.category_medium::text = ANY (ARRAY['ファンデーション'::text, '下地・コンシーラー'::text, '仕上げ'::text]) THEN 'ベースメイク本品'::text::character varying
                    WHEN g.category_medium::text = ANY (ARRAY['アイメイク'::text, 'チーク'::text, 'ツール'::text, 'リップ'::text]) THEN 'ポイントメイク本品'::text::character varying
                    ELSE g.product_kbn
                END AS goods_type,
            dense_rank() OVER (PARTITION BY c.customer ORDER BY (o.order_dt::date)) AS order_rank,
            min(o.ship_dt::date) OVER (PARTITION BY c.customer ORDER BY (o.order_dt::date)) AS first_ship_date,
                CASE
                    WHEN date_part('day'::text, o.ship_dt::timestamp without time zone - r.regular_contract_date::timestamp without time zone) <= 20::double precision THEN 1
                    ELSE NULL::integer
                END AS first_regular_flg
           FROM customers c
             JOIN orders o ON c.customer = o.customer
             JOIN goods_work g ON o.goods = g.goods::text
             JOIN ( SELECT orders.customer,
                    count(DISTINCT orders.order_dt::date) AS max_order_rank
                   FROM orders
                  WHERE orders.cancel_dt IS NULL AND orders.return_dt IS NULL
                  GROUP BY orders.customer) t_1 ON t_1.customer = c.customer
             LEFT JOIN ( SELECT regulars.regular_sales,
                    min(regulars.create_dt) AS regular_contract_date
                   FROM regulars
                  GROUP BY regulars.regular_sales) r ON o.regular_sales = r.regular_sales
          WHERE o.cancel_dt IS NULL AND o.return_dt IS NULL AND o.order_dt::date <= CURRENT_DATE
        )
 SELECT CURRENT_DATE AS date,
    date_part('year'::text, CURRENT_DATE) AS year,
    date_part('month'::text, CURRENT_DATE) AS month,
    base.customer,
        CASE
            WHEN max(base.order_rank) >= 4 THEN 'F4'::text
            WHEN max(base.order_rank) = 3 THEN 'F3'::text
            WHEN max(base.order_rank) = 2 THEN 'F2'::text
            WHEN max(base.order_rank) = 1 THEN 'F1'::text
            ELSE NULL::text
        END AS f_rank,
    max(base.order_rank) AS order_times,
    min(base.order_date) AS first_order_date,
    max(base.order_date) AS last_order_date,
    min(base.ship_date) AS first_ship_date,
    min(
        CASE
            WHEN base.order_rank = 2 THEN base.ship_date
            ELSE NULL::date
        END) AS second_ship_date,
    min(
        CASE
            WHEN base.order_rank = 3 THEN base.ship_date
            ELSE NULL::date
        END) AS third_ship_date,
    min(
        CASE
            WHEN base.order_rank = 4 THEN base.ship_date
            ELSE NULL::date
        END) AS forth_ship_date,
    max(base.ship_date) AS last_ship_date,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND (base.goods_type::text ~~ '%スキンケア%'::text OR base.goods_type::text ~~ '%定期%'::text OR base.goods_type::text ~~ '%モイスチャー%'::text OR base.goods_type::text ~~ '%バイタライジング%'::text OR base.goods_type::text ~~ '%バランシング%'::text) THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_skincare,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND base.goods_type::text ~~ '%メイク%'::text THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_make,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND base.goods_type::text ~~ '%ポイントメイク%'::text THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_point_make,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND base.goods_type::text ~~ '%ベースメイク%'::text THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_base_make,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND base.goods_type::text ~~ '%定期%'::text THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_regular,
    sum(
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision AND base.goods_type::text ~~ '%定期%'::text AND base.first_regular_flg = 1 THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_month_first_regular,
    count(DISTINCT
        CASE
            WHEN ((date_part('year'::text, base.ship_date) - date_part('year'::text, CURRENT_DATE)) * 12::double precision + (date_part('month'::text, base.ship_date) - date_part('month'::text, CURRENT_DATE))) = 0::double precision THEN base.order_id
            ELSE NULL::text
        END) AS order_count_in_a_month,
    sum(
        CASE
            WHEN date_part('day'::text, base.ship_date::timestamp without time zone - CURRENT_DATE::timestamp without time zone) >= 0::double precision AND date_part('day'::text, base.ship_date::timestamp without time zone - CURRENT_DATE::timestamp without time zone) <= 365::double precision THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS total_in_a_year,
    CURRENT_DATE - max(
        CASE
            WHEN base.product_kbn::text ~~ '%スキンケア%'::text THEN base.ship_date
            ELSE NULL::date
        END) AS days_from_last_skincare,
    CURRENT_DATE - max(
        CASE
            WHEN base.product_kbn::text ~~ '%メイク%'::text THEN base.ship_date
            ELSE NULL::date
        END) AS days_from_last_make,
        CASE
            WHEN (CURRENT_DATE - max(
            CASE
                WHEN base.product_kbn::text ~~ '%スキンケア%'::text THEN base.ship_date
                ELSE NULL::date
            END)) < 121 THEN true
            ELSE false
        END AS skincare_active,
        CASE
            WHEN (CURRENT_DATE - max(
            CASE
                WHEN base.product_kbn::text ~~ '%メイク%'::text THEN base.ship_date
                ELSE NULL::date
            END)) < 181 THEN true
            ELSE false
        END AS make_active,
    CURRENT_DATE - max(base.ship_date) AS days_from_last_purchase,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 1 THEN base.goods_type::text
            ELSE NULL::text
        END, ','::text) AS first_goods_type,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 2 THEN base.goods_type::text
            ELSE NULL::text
        END, ','::text) AS second_goods_type,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 3 THEN base.goods_type::text
            ELSE NULL::text
        END, ','::text) AS third_goods_type,
    string_agg(DISTINCT
        CASE
            WHEN (CURRENT_DATE - base.order_date) < 366 THEN base.goods_type::text
            ELSE NULL::text
        END, ','::text) AS one_year_goods_type,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = base.max_order_rank THEN base.goods_type::text
            ELSE NULL::text
        END, ','::text) AS latest_goods_type,
    string_agg(DISTINCT base.goods_type::text, ','::text) AS all_goods_type,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 1 THEN base.product_name::text
            ELSE NULL::text
        END, ','::text) AS first_product_name,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 2 THEN base.product_name::text
            ELSE NULL::text
        END, ','::text) AS second_product_name,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 3 THEN base.product_name::text
            ELSE NULL::text
        END, ','::text) AS third_product_name,
    string_agg(DISTINCT
        CASE
            WHEN (CURRENT_DATE - base.order_date) < 366 THEN base.product_name::text
            ELSE NULL::text
        END, ','::text) AS one_year_product_name,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = base.max_order_rank THEN base.product_name::text
            ELSE NULL::text
        END, ','::text) AS latest_product_name,
    string_agg(DISTINCT base.product_name::text, ','::text) AS all_product_name,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 1 THEN base.goods
            ELSE NULL::text
        END, ','::text) AS first_goods,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 2 THEN base.goods
            ELSE NULL::text
        END, ','::text) AS second_goods,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 3 THEN base.goods
            ELSE NULL::text
        END, ','::text) AS third_goods,
    string_agg(DISTINCT
        CASE
            WHEN (CURRENT_DATE - base.order_date) < 366 THEN base.goods
            ELSE NULL::text
        END, ','::text) AS one_year_goods,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = base.max_order_rank THEN base.goods
            ELSE NULL::text
        END, ','::text) AS latest_goods,
    string_agg(DISTINCT base.goods, ','::text) AS all_goods,
    sum(
        CASE
            WHEN (base.ship_date - base.first_ship_date) < 366 THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS one_year_ltv,
    sum(
        CASE
            WHEN (base.ship_date - base.first_ship_date) < 731 THEN base.price::double precision * base.qty
            ELSE NULL::double precision
        END) AS two_year_ltv,
    min(
        CASE
            WHEN base.goods_type::text = 'メイクトライアル'::text THEN base.ship_dt::date
            ELSE NULL::date
        END) AS first_mtr_date,
    min(
        CASE
            WHEN base.goods_type::text = 'モイスチャートライアル'::text THEN base.ship_dt::date
            ELSE NULL::date
        END) AS first_smtr_date,
    min(
        CASE
            WHEN base.goods_type::text = 'バランシングトライアル'::text THEN base.ship_dt::date
            ELSE NULL::date
        END) AS first_sbtr_date,
    min(
        CASE
            WHEN base.goods_type::text = 'バイタライジングトライアル'::text THEN base.ship_dt::date
            ELSE NULL::date
        END) AS first_svtr_date,
    count(DISTINCT
        CASE
            WHEN base.product_kbn::text ~~ '%メイク%'::text THEN base.order_id
            ELSE NULL::text
        END) AS make_order_times,
    count(DISTINCT
        CASE
            WHEN base.product_kbn::text ~~ 'メイク本品'::text THEN base.order_id
            ELSE NULL::text
        END) AS make_product_order_time,
    count(DISTINCT
        CASE
            WHEN base.product_kbn::text ~~ '%スキンケア%'::text THEN base.order_id
            ELSE NULL::text
        END) AS skincare_order_times,
    count(DISTINCT
        CASE
            WHEN base.product_kbn::text ~~ 'スキンケア本品'::text THEN base.order_id
            ELSE NULL::text
        END) AS skincare_product_order_time,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 1 THEN base.category_line::text
            ELSE NULL::text
        END, ','::text) AS first_goods_line,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 2 THEN base.category_line::text
            ELSE NULL::text
        END, ','::text) AS second_goods_line,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = 3 THEN base.category_line::text
            ELSE NULL::text
        END, ','::text) AS third_goods_line,
    string_agg(DISTINCT
        CASE
            WHEN (CURRENT_DATE - base.order_date) < 366 THEN base.category_line::text
            ELSE NULL::text
        END, ','::text) AS one_year_goods_line,
    string_agg(DISTINCT
        CASE
            WHEN base.order_rank = base.max_order_rank THEN base.category_line::text
            ELSE NULL::text
        END, ','::text) AS latest_goods_line,
    string_agg(DISTINCT base.category_line::text, ','::text) AS all_goods_line,
    count(DISTINCT
        CASE
            WHEN base.regular::double precision > 0::double precision THEN base.order_id
            ELSE NULL::text
        END) AS regular_times,
    count(DISTINCT
        CASE
            WHEN base.category_line::text ~~ 'モイスチャーライン'::text AND base.product_name::text ~~ '%ローション%'::text THEN base.order_id
            ELSE NULL::text
        END) AS skin_mois_times,
    count(DISTINCT
        CASE
            WHEN base.category_line::text ~~ 'モイスチャーライン'::text AND base.product_name::text ~~ '%セラム%'::text THEN base.order_id
            ELSE NULL::text
        END) AS skin_seramu_times,
    count(DISTINCT
        CASE
            WHEN base.product_name::text ~~ 'マットスムースミネラルファンデーション'::text THEN base.order_id
            ELSE NULL::text
        END) AS mat_found_times,
    count(DISTINCT
        CASE
            WHEN base.product_name::text ~~ 'ナイトミネラルファンデーション'::text THEN base.order_id
            ELSE NULL::text
        END) AS ground_night_times
   FROM base base(id, sales, detail, order_id, customer, method_name, subtotal, point_out, total, point, point_in, rules, order_id2, order_id3, regular_sales, order_dt, ship_dt, cancel_dt, return_dt, site, source, status, affiliate, advt, promotion_code, coupon_event, devicetype, block_id, cancel_reason, store, goods, item_code, item_code2, item_code3, name, variation_group, variation_name1, variation_name2, qty, price, amt, brand, regular, noshi_purpose_name, sales_name, sales_name2, sales_kana, sales_kana2, sales_zip, created_at, updated_at, category_large, category_medium, category_detail, order_date, ship_date, max_order_rank, category_large_1, category_medium_1, category_line, product_kbn, regular_flg, product_name, regular_contract_date, goods_type, order_rank, first_ship_date, first_regular_flg)
  GROUP BY base.customer;