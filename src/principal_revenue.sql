with partners as (
select
	  customer_id
	, string_agg(distinct partner_code, ',') as partners_list
from mfs_replica_db."lead"
group by 1
)
, financings as ( 
select customer_id 
	, max(case when state in ('DISBURSED', 'COLLECTION', 'OVERDUE', 'PAID') then 1 else 0 end) as previous_financing
	, max (case when state in ('DISBURSED', 'COLLECTION', 'OVERDUE') then 1 else 0 end) as active_financing
from mfs_replica_db.loan 
group by 1
)
, collections as ( 
select distinct l.customer_id 
from mfs_replica_db.loan l 
inner join mfs_replica_db.collection c2 on l.id = c2.loan_id
)
, unpaid_principal_raw as ( -- Across all loans of the given client
select 
	  l.customer_id 
	, l.id as loan_id
	, l.principal as principal
	, sum(rti.amount) as paid_principal
from mfs_replica_db.loan l 
inner join mfs_replica_db.installment i on l.id = i.loan_id
inner join mfs_replica_db.rel_txn_installment rti on i.id = rti.installment_id and rti."type" = 'PRINCIPAL'
where l.state in ('DISBURSED', 'COLLECTION', 'OVERDUE') -- We consider only "active" financings
group by 1, 2, 3
) 
, unpaid_principal as (
select 
	  customer_id
	, sum (principal) as principal
	, sum (paid_principal) as paid_principal
from unpaid_principal_raw
group by 1
)
, reps_raw as (
select 
	  p."name" as company_name
	, p.id as party_id
	, p.reg_num as ico
	, coalesce (concat(p2.first_name, ' ', p2.last_name), p2.name) as representative_name
	, p2.email 
	, p2.phone 
	, r.role_text 
	, r.party_rank 
	, case when user_id is null then 2 else 1 end as user_id_based_rank
from mfs_replica_db.representative r 
left join mfs_replica_db.party p on r.organization_id = p.id
left join mfs_replica_db.party p2 on r.party_id = p2.id
where r.is_statutory = true
)
, reps as (
select
	*
	, RANK () OVER (
		PARTITION BY party_id 
		ORDER BY user_id_based_rank asc, party_rank ASC
	) within_company_rank
from reps_raw
)
, last_published_offer as (
select merchant_id
	, partner_code 
	, max(created_at) as created_at 
from mfs_replica_db.offer o 
where published = true
group by 1, 2)
, clients_with_active_offer as (
select distinct coalesce (pc.customer_id::text, o.merchant_id) as customer_id 
from last_published_offer o
left join mfs_replica_db.partner_connection pc on pc.merchant_id = o.merchant_id and pc.partner_code = o.partner_code 
where current_date - date(o.created_at) <= 30
)
, forecasts as ( 
select
	  coalesce(customer_id::text, merchant_id ) as customer_id 
	, currency
	, sum(case when product = 'M1' then forecasted_revenue else 0 end) as forecasted_1m_revenue
	, sum(case when product = 'M3' then forecasted_revenue else 0 end) as forecasted_3m_revenue
	, sum(case when product = 'M6' then forecasted_revenue else 0 end) as forecasted_6m_revenue
	, sum(case when product = 'M12' then forecasted_revenue else 0 end) as forecasted_12m_revenue
	, count(*)
from mfs_replica_db.risk_evaluation a 
inner join mfs_replica_db.risk_evaluation_grade b on a.id = b.risk_evaluation_id
where to_char(a.created_at, 'YYYY-MM') = '2024-12'
and partner_code <> 'Google'
and a."type" = 'BUSINESS_DATA'
group by 1, 2
)
select 
	  c.party_id as customer_id
	, p.reg_num as ico
	, p.country 
	, p.name 
	, pts.partners_list
	, p.email 
	, p.phone 
	, r1.representative_name
	, r1.email
	, r1.phone
	, r2.representative_name
	, r2.email
	, r2.phone
	, date(c.created_at) as customer_created_at
	, case when vo.customer_id is null then 0 else 1 end as valid_offer
	, coalesce (f.previous_financing, 0) as previous_financing
	, coalesce (f.active_financing, 0) as active_financing
	, case when coll.customer_id is null then 0 else 1 end as collection
	, coalesce (up.principal - up.paid_principal, 0) as unpaid_principal
	, coalesce (fcst.forecasted_1m_revenue, 0) as forecasted_1m_revenue
	, coalesce (fcst.forecasted_3m_revenue, 0) as forecasted_3m_revenue
	, coalesce (fcst.forecasted_6m_revenue, 0) as forecasted_6m_revenue
	, coalesce (fcst.forecasted_12m_revenue, 0) as forecasted_12m_revenue
	, fcst.currency as forecast_currency
from mfs_replica_db.customer c 
inner join mfs_replica_db.party p on c.party_id = p.id
left join partners pts on c.party_id = pts.customer_id
left join financings f on c.party_id = f.customer_id
left join collections coll on c.party_id = coll.customer_id
left join unpaid_principal up on c.party_id = up.customer_id
left join reps r1 on c.party_id = r1.party_id and r1.within_company_rank = 1
left join reps r2 on c.party_id = r2.party_id and r2.within_company_rank = 2
left join clients_with_active_offer vo on c.party_id::text = vo.customer_id
left join forecasts fcst on c.party_id::text = fcst.customer_id;
