
----Importing data
drop table np_temp;
CREATE TABLE np_temp (
payment_creation_day date,
txn_reference integer,
bin integer,
funding_source text,
issuer_country_code text,
shopper_interaction text,
mcc integer,
cvc_supplied text,
avs_supplied text,
transaction_amount real,
transaction_currency text,
interchange_amount real,
interchange_currency text
);

create table np_temp_format as 
select  
   date(SUBSTRING(payment_creation_day, 7,4) || '-' || SUBSTRING(payment_creation_day, 4,2) || '-' || SUBSTRING(payment_creation_day, 1,2))  payment_date,
   nt.*
from np_temp nt 
where txn_reference is not null
;

----export to python as cleandata.csv
select 
   payment_date
   bin, 
   funding_source, 
   issuer_country_code, 
   shopper_interaction, 
   cvc_supplied, 
   avs_supplied, 
   transaction_currency, 
   interchange_currency,
   sum(interchange_amount) total_interchange, 
   count(distinct txn_reference) total_txns
from np_temp_format
group by 1,2,3,4,5,6,7,8,9
order by 1,2,3,4,5,6,7,8,9;

-----Different values for estimated value
select 
   interchange_amount, 
   max(xyz)*interchange_amount all_trans_updated,
   max(xyz) all_transactions,
   count(txn_reference) total_transactions
from np_temp_format ntf
left join (select count(txn_reference) xyz from np_temp_format) as b on 'apple' = 'apple'
group by 1;

select 
avg(interchange_amount) avg_interchange_amount, 
min(case when bucket = 1 then interchange_amount end ) min_value, 
max(case when bucket = 1 then interchange_amount end) quart_one_value, 
max(case when bucket = 2 then interchange_amount end) quart_two_value, 
max(case when bucket = 3 then interchange_amount end) quart_three_value, 
max(case when bucket = 4 then interchange_amount end) quart_four_value,
count(distinct txn_reference) total_txns
from 
(select 
   a.*, 
   ntile(4) over(order by interchange_amount) bucket
from np_temp_format as a) as fin 
;



------Cohort analysis
drop table np_base_one;
create table np_base_one as 
select 
   payment_date,
   funding_source || '-' || shopper_interaction || '-' || mcc concat_string_2,
   interchange_amount,
   funding_source || '-' || shopper_interaction || '-' || mcc ||'-'|| interchange_amount  as concat_string, 
   sum(interchange_amount) total_interchange, 
   count(distinct txn_reference) total_txns
from np_temp_format
group by 1,2,3,4

---export to sheet and python as cohort_data
select 
  payment_date,
  concat_string_2,
  interchange_amount,
  total_interchange, 
  total_txns
from np_base_one;

---trend
select 
   payment_date, 
   sum(total_interchange) cost, 
   sum(total_txns) total_txns,
   cast(sum(total_interchange) as double)/sum(total_txns) count_txns
from
   np_base_one
   group by 1;

----cohort sheet: 
  
select 
    payment_date, 
    sum( case when concat_string_2 = 'CREDIT-ContAuth-5310' then total_interchange end) "CREDIT-ContAuth-5310",
    sum( case when concat_string_2 = 'CREDIT-ContAuth-5311' then total_interchange end) "CREDIT-ContAuth-5311",
    sum( case when concat_string_2 = 'CREDIT-Ecommerce-5310' then total_interchange end) "CREDIT-Ecommerce-5311",
    sum( case when concat_string_2 = 'CREDIT-Ecommerce-5311' then total_interchange end) "CREDIT-ContAuth-5310",
    sum( case when concat_string_2 = 'DEBIT-ContAuth-5310' then total_interchange end) "DEBIT-ContAuth-5310",
    sum( case when concat_string_2 = 'DEBIT-ContAuth-5311' then total_interchange end) "DEBIT-ContAuth-5311",
    sum( case when concat_string_2 = 'DEBIT-Ecommerce-5310' then total_interchange end) "DEBIT-Ecommerce-5310",
    sum( case when concat_string_2 = 'DEBIT-Ecommerce-5311' then total_interchange end) "DEBIT-Ecommerce-5311",
    sum( case when concat_string_2 = 'PREPAID-ContAuth-5310' then total_interchange end) "PREPAID-ContAuth-5310",
    sum( case when concat_string_2 = 'PREPAID-ContAuth-5311' then total_interchange end) "PREPAID-ContAuth-5311",
    sum( case when concat_string_2 = 'PREPAID-Ecommerce-5310' then total_interchange end) "PREPAID-Ecommerce-5310",
    sum( case when concat_string_2 = 'PREPAID-Ecommerce-5311' then total_interchange end) "PREPAID-Ecommerce-5311"
from 
np_base_one
group by 1

----Pre and post analysis

select 
   coalesce(a.concat_string_2, b.concat_string_2) concat_string_2, 
   coalesce(a.interchange_amount, b.interchange_amount) interchange_amount,
   a.total_interchange_a, 
   a.txns_a,
   b.total_interchange_b,
   b.txns_b
from 
(select 
concat_string_2,
interchange_amount, 
sum(total_interchange) total_interchange_a, 
sum(total_txns) txns_a 
from np_base_one 
where payment_date < (select date(min(payment_date),'+5 days') from np_base_one)
group by 1,2
) as a 
full outer join 
(select 
concat_string_2,
interchange_amount, 
sum(total_interchange) total_interchange_b,
sum(total_txns) txns_b
from np_base_one 
where payment_date > (select date(max(payment_date),'-5 days') from np_base_one)
group by 1,2
) as b on a.concat_string_2 = b.concat_string_2  and a.interchange_amount = b.interchange_amount
order by 1,2
;

----
select 
sum(total_interchange) total_interchange_a, 
sum(total_txns) txns_a 
from np_base_one 
where payment_date < (select date(min(payment_date),'+5 days') from np_base_one)
;
select 
sum(total_interchange) total_interchange_b,
sum(total_txns) txns_b
from np_base_one 
where payment_date > (select date(max(payment_date),'-5 days') from np_base_one);
