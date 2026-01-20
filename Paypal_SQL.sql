Create database paypal;
use paypal;
select * from transactions;
select * from users;
select * from chargebacks;
select * from payment_methods;

#1.Calculate monthly revenue from successful transactions
CREATE VIEW Monthly_Revenue_S AS
    SELECT 
        txn_month_name, SUM(net_revenue) AS Revenue
    FROM
        transactions
    WHERE
        transaction_status = 'Success'
    GROUP BY txn_month_name
    ORDER BY SUM(amount) DESC;

#select * from Monthly_Revenue_S;

#2. Find top 10 users by total transaction value
create view Top_Users_All as
 with user_detail as (
 select user_id, sum(amount) as Total_Transaction from transactions group by user_id order by Total_Transaction desc
 ), 
 ranked_user as (
 select user_id,Total_transaction, dense_rank() over(order by Total_Transaction desc) as drk from user_detail
 ) 
 select * from ranked_user where drk <= 10;
 
 #select * from Top_Users_All;
 
 #3.Compute transaction success and failure rate by payment method
 CREATE VIEW S_F_Rate_by_payment_Method AS
    SELECT 
        payment_method,
        COUNT(*) AS Total_Transaction,
        SUM(CASE
            WHEN IS_Success = 1 THEN 1
            ELSE 0
        END) AS Sucess_Count,
        SUM(CASE
            WHEN IS_Success = 0 THEN 1
            ELSE 0
        END) AS Failed_Count,
        ROUND(SUM(CASE
                    WHEN IS_Success = 1 THEN 1
                    ELSE 0
                END) * 100 / COUNT(*),
                2) AS Sucess_PCT,
        ROUND(SUM(CASE
                    WHEN IS_Success = 0 THEN 1
                    ELSE 0
                END) * 100 / COUNT(*),
                2) AS Failure_PCT
    FROM
        transactions
    GROUP BY payment_method;
  
 #select * from S_F_Rate_by_payment_Method;
 
 #4. Calculate month-over-month revenue growth
create view MOM_Revenue as
WITH monthly_revenue AS (
    SELECT 
        YEAR(transaction_date) AS txn_year,
        MONTH(transaction_date) AS txn_month,txn_month_name,
        SUM(net_revenue) AS revenue
    FROM transactions
    WHERE transaction_status = 'Success'
    GROUP BY 
        YEAR(transaction_date),
        MONTH(transaction_date),
        txn_month_name
)
SELECT 
    txn_year,
    txn_month,
    txn_month_name,
    ROUND(revenue, 2) AS current_month_revenue,
    ROUND(
        revenue - LAG(revenue) 
        OVER (ORDER BY txn_year, txn_month), 2
    ) AS revenue_change,
    ROUND(
        (revenue - LAG(revenue) 
        OVER (ORDER BY txn_year, txn_month))
        / LAG(revenue) 
        OVER (ORDER BY txn_year, txn_month) * 100, 2
    ) AS mom_growth_pct
FROM monthly_revenue
ORDER BY txn_year, txn_month;
#select * from MOM_Revenue;

#5. Identify users with declining activity
create view user_declining_data as
 with monthly_transaction_count as (
 select user_id,count(*) as txn_count, year(transaction_date) as txn_year,month(transaction_date) as txn_month from transactions group by 
 user_id, year(transaction_date), month(transaction_date)
 ),
 Logged_detail as (select user_id, txn_year,txn_month, txn_count, 
 lag(txn_count) over( partition by user_id order by txn_year, txn_month) as previous_month_txn from monthly_transaction_count)
 select user_id, txn_year,txn_month, txn_count as current_month_txn,previous_month_txn, 
 txn_count -  previous_month_txn as activity_change
 from Logged_detail
 where txn_count < previous_month_txn
 order by activity_change;
 #select * from user_declining_data;
 
 #6.Find revenue lost due to failed transactions
 CREATE VIEW revenue_lost_failed AS
    SELECT 
        ROUND(SUM(amount), 2) AS revenue_lost
    FROM
        transactions
    WHERE
        transaction_status = 'Failed';
#select * from revenue_lost_failed;

#7.Compute chargeback rate pct
select round( sum(case when is_chargeback = 1 then 1 else 0 end) / count(*) * 100,2) as chargeback_pct from transactions;

#8. Compare business vs personal account performance
create view business_vs_personal as
SELECT
    u.account_type,
    COUNT(*) AS total_transactions,
    ROUND(SUM(t.amount), 2) AS total_transaction_value,
    ROUND(
        SUM(CASE 
            WHEN t.transaction_status = 'Success' 
            THEN t.net_revenue 
            ELSE 0 
        END), 2
    ) AS total_net_revenue,
    ROUND(
        SUM(CASE 
            WHEN t.transaction_status = 'Success' 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*), 2
    ) AS success_rate_pct,
    ROUND(
        SUM(CASE 
            WHEN t.is_chargeback = 1 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*), 2
    ) AS chargeback_rate_pct
FROM transactions t
JOIN users u 
    ON t.user_id = u.user_id
GROUP BY u.account_type;

#select * from business_vs_personal;

#9. Calculate average time gap between transactions
create view average_time_gap as
with prev_transaction as(
select user_id, transaction_date,
lag(transaction_date) over(partition by user_id order by transaction_date) as previous_txn_date 
from transactions
)
select user_id, round(avg(datediff(transaction_date,previous_txn_date)),2) as avg_diff from 
prev_transaction where previous_txn_date is not null
group by user_id;

#select * from average_time_gap;
