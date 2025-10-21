-- models/marts/agg_loan_metrics_by_state.sql

{{ config(
    materialized='table', 
    description='Ключові метрики прийнятих кредитів, згруповані за штатом та грейдом'
) }}

WITH accepted_loans AS (
   
        addr_state,
        grade,
        loan_amnt,
        int_rate,
        annual_inc,
        dti,
        total_pymnt,
        total_rec_prncp
    FROM
        {{ ref('stg_accepted_loans') }}
    WHERE
        
        loan_amnt IS NOT NULL AND annual_inc IS NOT NULL
)

SELECT
    addr_state,
    grade,
    COUNT(*) AS total_loans,
    
    AVG(loan_amnt) AS avg_loan_amount,
    AVG(int_rate) AS avg_interest_rate,
    AVG(annual_inc) AS avg_annual_income,
    
    SUM(loan_amnt) AS total_funded_amount,
    SUM(total_pymnt) AS total_payments_received,
    SUM(total_rec_prncp) AS total_principal_received,
    
   
    (SUM(total_pymnt) - SUM(total_rec_prncp)) / SUM(total_rec_prncp) AS avg_return_on_principal

FROM
    accepted_loans
GROUP BY
    1, 2
ORDER BY
    addr_state, grade