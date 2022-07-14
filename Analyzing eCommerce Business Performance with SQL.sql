--- Add ERD query
BEGIN;


CREATE TABLE IF NOT EXISTS public.customers_dataset
(
    customer_id character varying COLLATE pg_catalog."default",
    customer_unique_id character varying COLLATE pg_catalog."default",
    customer_zip_code_prefix numeric,
    customer_city character varying COLLATE pg_catalog."default",
    customer_state character varying COLLATE pg_catalog."default",
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.geolocation_dataset
(
    geolocation_zip_code_prefix numeric,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city character varying COLLATE pg_catalog."default",
    geolocation_state character varying COLLATE pg_catalog."default",
    PRIMARY KEY (geolocation_zip_code_prefix)
);

CREATE TABLE IF NOT EXISTS public.order_items_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    order_item_id numeric,
    product_id character varying COLLATE pg_catalog."default",
    seller_id character varying COLLATE pg_catalog."default",
    shipping_limit_date date,
    price numeric,
    freight_value numeric
);

CREATE TABLE IF NOT EXISTS public.order_payments_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    payment_sequential numeric,
    payment_type character varying COLLATE pg_catalog."default",
    payment_installments numeric,
    payment_value numeric
);

CREATE TABLE IF NOT EXISTS public.order_reviews_dataset
(
    review_id character varying COLLATE pg_catalog."default",
    order_id character varying COLLATE pg_catalog."default",
    review_score numeric,
    review_comment_title character varying COLLATE pg_catalog."default",
    review_comment_message character varying COLLATE pg_catalog."default",
    review_creation_date date,
    review_answer_timestamp date,
    PRIMARY KEY (review_id)
);

CREATE TABLE IF NOT EXISTS public.orders_dataset
(
    order_id character varying COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying COLLATE pg_catalog."default",
    order_status character varying COLLATE pg_catalog."default",
    order_purchase_timestamp date,
    order_approved_at date,
    order_delivered_carrier_date date,
    order_delivered_customer_date date,
    order_estimated_delivery_date date,
    CONSTRAINT orders_dataset_pkey PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.product_dataset
(
    product_id character varying COLLATE pg_catalog."default" NOT NULL,
    product_category_name character varying COLLATE pg_catalog."default",
    product_name_lenght character varying COLLATE pg_catalog."default",
    product_description_lenght character varying COLLATE pg_catalog."default",
    product_photos_qty character varying COLLATE pg_catalog."default",
    product_weight_g numeric,
    product_length_cm numeric,
    product_height_cm numeric,
    product_width_cm numeric,
    CONSTRAINT product_dataset_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.sellers_dataset
(
    seller_id character varying COLLATE pg_catalog."default" NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city character varying COLLATE pg_catalog."default",
    seller_state character varying COLLATE pg_catalog."default",
    CONSTRAINT sellers_dataset_pkey PRIMARY KEY (seller_id)
);

ALTER TABLE IF EXISTS public.customers_dataset
    ADD CONSTRAINT customer_zip_code_prefix FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT product_id FOREIGN KEY (product_id)
    REFERENCES public.product_dataset (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT seller_id FOREIGN KEY (seller_id)
    REFERENCES public.sellers_dataset (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_items_dataset
    ADD CONSTRAINT order_id FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_payments_dataset
    ADD CONSTRAINT order_id FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_reviews_dataset
    ADD CONSTRAINT order_id FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.orders_dataset
    ADD CONSTRAINT customer_id FOREIGN KEY (customer_id)
    REFERENCES public.customers_dataset (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.sellers_dataset
    ADD CONSTRAINT seller_zip_code_prefix FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;

--- Showing Average MAU, Total New Customer, Total Repeat Order Customer, and Average Order per year query
with calc_mau as (
        select MAU.tahun, round(AVG(MAU.cust),0) as average_mau
        from (    select extract(month from order_purchase_timestamp) as bulan,
                    extract(year from order_purchase_timestamp) as tahun,
                    count (distinct cd.customer_unique_id) as cust
                from customers_dataset as cd
                join orders_dataset as od
                on cd.customer_id = od.customer_id
                group by 1,2
                order by 2,1) as MAU
        group by 1
    ),
new_user as (select 
             date_part('year', first_purchase_time) as tahun,
             count(1) as new_customers
        from (select 
                c.customer_unique_id,
                min(o.order_purchase_timestamp) as first_purchase_time
                from orders_dataset o 
                join customers_dataset c on c.customer_id = o.customer_id
                group by 1) subq
        group by 1),
user_order as (with cto as(select 
                    date_part('year', od.order_purchase_timestamp) as "tahun", 
                    cd.customer_unique_id , 
                    count(od.order_id) as customer_total_order
        from orders_dataset od 
        join customers_dataset cd on cd.customer_id = od.customer_id
        group by 1,2
        order by 1)

        select  tahun,
                sum(case when cto.customer_total_order >1 then 1 else 0 end) as total_cust_repeat_order,
                round(avg(cto.customer_total_order),2) as average_order
        from cto group by 1)
        
select uo.*, nu.new_customers, cm.average_mau 
from calc_mau as cm 
join new_user as nu on nu.tahun = cm.tahun
join user_order as uo on cm.tahun = uo.tahun
;

--- Showing Total Order per year query
select date_part('year', od.order_purchase_timestamp) as "tahun", 
             count(od.order_id) as total_order
from orders_dataset od 
join customers_dataset cd on cd.customer_id = od.customer_id
group by 1
order by 1;

---Total Revenue Per Year Table
create table total_revenue_per_year as (
    select 
        date_part ('year',od.order_purchase_timestamp) as tahun,
        sum(oi.price + oi.freight_value) as total_revenue
    from orders_dataset as od
    join order_items_dataset as oi on od.order_id = oi.order_id
    where od.order_status = 'delivered'
    group by 1
    order by 1)

--- Total Canceled Order Per Year Table
create table total_cancelled as(
    select     date_part ('year',od.order_purchase_timestamp) as tahun,
            sum(case when od.order_status = 'canceled' then 1 else 0 end) as total_cancel_order
    from orders_dataset as od
    group by 1
    order by 1);

--- Highest Revenue Product Per Year Table
create table top_product_category as(
    with calc_revenue as(
            select 
                date_part ('year',od.order_purchase_timestamp) as tahun,
                pd.product_category_name,
                sum(oi.price + oi.freight_value) as total_product_revenue
            from orders_dataset as od
            join order_items_dataset as oi on od.order_id = oi.order_id
            join product_dataset as pd on oi.product_id = pd.product_id
            where od.order_status = 'delivered'
            group by 1,2
            order by 1,3 desc)

    select cmr.tahun, cmr.product_category_name as max_revenue_product, max(cr.total_product_revenue) as total_revenue
    from(
        select tahun, product_category_name,
        rank () over(partition by tahun order by total_product_revenue desc) as "rank"
        from calc_revenue) as cmr
    join calc_revenue as cr on cmr.tahun = cr.tahun
    where cmr.rank= 1
    group by 1,2
    order by 1);

--- Most Canceled Product Per Year  Table
create table most_cancelled_product_category as (
    with calc_cancel as(
        select 
            date_part ('year',od.order_purchase_timestamp) as tahun,
            pd.product_category_name,
            sum(case when od.order_status = 'canceled' then 1 else 0end) as count_cancel_order
        from orders_dataset as od
        join order_items_dataset as oi on od.order_id = oi.order_id
        join product_dataset as pd on oi.product_id = pd.product_id
        group by 1,2
        order by 1,2)
	
    select cx.tahun, cx.product_category_name as max_canceled_product, max(cc.count_cancel_order) as canceled_count
    from(
        select tahun, product_category_name,
        rank () over(partition by tahun order by count_cancel_order desc) as "rank"
        from calc_cancel) as cx
    join calc_cancel as cc on cx.tahun = cc.tahun
    where cx.rank = 1
    group by 1,2
    order by 1)
    ;

--- Combine Table
select 
    a.tahun as Tahun,
    a.total_revenue as Total_Revenue,
    b.total_cancel_order as Total_Cancelled_Order,
    c.max_revenue_product as Highest_Revenue_Product,
    d.max_canceled_product as Most_Cancelled_Product
from total_revenue_per_year as a
join total_cancelled as b on a.tahun = b.tahun
join top_product_category as c on b.tahun = c.tahun
join most_cancelled_product_category as d on c.tahun = d.tahun;

--- All-time Payment Type Usage and Payment Type Usage per Year Table
select     op.payment_type,
        count(1) as all_time_use,
        sum(case when date_part('year', od.order_purchase_timestamp) = 2016 then 1 else 0 end) as "2016",
        sum(case when date_part('year', od.order_purchase_timestamp) = 2017 then 1 else 0 end) as "2017",
        sum(case when date_part('year', od.order_purchase_timestamp) = 2018 then 1 else 0 end) as "2018"
from order_payments_dataset as op
join orders_dataset as od
on op.order_id = od.order_id
group by 1
order by all_time_use desc;
