
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
    avs_supplied,
   interchange_amount,
   funding_source || '-' || shopper_interaction || '-' || mcc ||'-'|| interchange_amount  as concat_string, 
   sum(interchange_amount) total_interchange, 
   count(distinct txn_reference) total_txns
from np_temp_format
group by 1,2,3,4,5


select 
   funding_source,
   avs_supplied,
   interchange_amount,
   sum(interchange_amount) total_interchange, 
   count(distinct txn_reference) total_txns
from 
   np_temp_format
group by 1,2,3
;   
---export to sheet and python as cohort_data
select 
  payment_date,
  concat_string_2,
  avs_supplied,
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

----PYTHON 


import pandas as pd
import numpy as np
import plotly as pl
import plotly.express as px
import streamlit as st
import math as mt
import sqlite3
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from operator import attrgetter
import matplotlib.colors as mcolors
import plotly.graph_objects as go
from plotly.subplots import make_subplots

data = pd.read_csv ('/Users/niharpatel/Desktop/Cleaned data.csv')   
data2 = pd.read_csv ('/Users/niharpatel/Desktop/cohort_data.csv')   
df = pd.DataFrame(data)
df2 = pd.DataFrame(data2)

df['bin'] = df["bin"].map(str)
df[["txn_reference","bin","shopper_interaction","cvc_supplied","avs_supplied","interchange_amount"]].describe(include="all")

##getting different mean and quartile values
df.describe(include="all")

n_trans2=df.groupby('payment_date').agg(sum_inter = ('interchange_amount','sum'),
                                        count_txn = ('txn_reference', 'count')).reset_index()

n_trans2['average'] = n_trans2['sum_inter']/n_trans2['count_txn']


n_trans_nump = n_trans2.to_numpy()
n_trans_series = n_trans2.squeeze()
resultList = list(n_trans_dict.items())
n_trans_dict = n_trans2.to_dict()

------line chart

fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Scatter( x=n_trans_series["payment_date"], y=n_trans_series["sum_inter"], name="yaxis data"),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(x = n_trans_series["payment_date"], y=n_trans_series["average"], name="yaxis2 data"),
    secondary_y=True,
)
--------COHORT ANALYSIS

df['combined'] = df['funding_source'] + '_' + df['shopper_interaction'] + '_' + df['mcc'].astype(str)

n_trans3=df.groupby(['payment_date','combined']).agg(sum_inter = ('interchange_amount','sum'),
                                        count_txn = ('txn_reference', 'count')).reset_index()

fig = px.line(n_trans3, x='payment_date', y='sum_inter', color='combined', markers=True)
fig.show()


##Trying it by all different variables
##df.groupby(['payment_date','funding_source']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
##df.groupby(['payment_date','shopper_interaction']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
##df.groupby(['payment_date','mcc']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
##df.groupby(['payment_date','cvc_supplied']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
##df.groupby(['payment_date','avs_supplied']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
##df.groupby(['payment_date','interchange_amount']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()




n_trans2 = df.groupby(['payment_date','avs_supplied']).aggregate(sum_inter = ('interchange_amount','sum')).reset_index()
fig = px.bar(n_trans2, x="payment_date", y="sum_inter", color="avs_supplied", title="cohort")
fig.show()




----------------------------
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

----

select min(payment_date), date(max(payment_date),  from np_temp_format;


select * from np_temp_format;

------------PURCHASE FREQUENCY
select 
   fin.concat_string, 
   count(distinct bin) distinct_credits, 
   count(distinct case when txns > 1 then bin end) distinct_credits_rec,
   sum(txns) txns, 
   sum(case when txns > 1 then txns end) recurring_txns
from 
(select 
    funding_source || '-' || shopper_interaction || '-' || mcc concat_string, 
    bin, 
    count(distinct txn_reference) txns 
from np_temp_format
group by 1,2)
as fin 
group by 1;
