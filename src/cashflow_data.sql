truncate table dwh_fasanara.cashflow_data;
insert into dwh_fasanara.cashflow_data
with cd as (
    select
        id as loan_id
        , now() - interval '1 day' as as_of_date
    from mfs_replica_db.loan
    where id is not null
    group by loan_id
), i as (
	select
	ins.id
	, ins.loan_id
	, cd.as_of_date
	from mfs_replica_db.installment as ins
	inner join cd on ins.loan_id = cd.loan_id
), ri as (
	select
	rti.installment_id
	, i.loan_id
	, i.as_of_date
	, rti.bank_transaction_id as transaction_id
	, rti.effective_date as transaction_date
	, rti.amount as transaction_amount
	, case rti.type
		when 'PROLONGATION' then 'SERVICE_FEE'
		when 'PRINCIPAL' then 'PRINCIPAL PAYMENT'
		when 'FEE' then 'INTEREST_PAYMENT'
		when 'FEE_REMINDER2' then 'LATE_FEE'
		when 'FEE_REMINDER1' then 'LATE_FEE'
		when 'PENALTY' then 'LATE_FEE'
	end as transaction_type
	from mfs_replica_db.rel_txn_installment as rti
	inner join i on rti.installment_id = i.id
), tr as (
	select
	ri.*
	, b.id as account_id
	, b.iban
	, b.currency
	from mfs_replica_db.bank_transaction b
	inner join ri on ri.transaction_id = b.id
)
select
	tr.as_of_date
    , tr.account_id
    , tr.loan_id
    , tr.transaction_id
    , tr.transaction_date
    , tr.transaction_type
	, tr.currency
	, tr.transaction_amount
	, null as reversal_reference
from tr;