{{ config(
    materialized='table', 
    description='Середні фінансові показники, згруповані за статусом верифікації доходу'
) }}

SELECT
    verification_status,
    loan_status,
    COUNT(*) AS total_loans,

    AVG(annual_inc) AS avg_annual_income,
    AVG(loan_amnt) AS avg_loan_amount,
    AVG(int_rate) AS avg_interest_rate

FROM
    {{ ref('stg_accepted_loans') }}
WHERE
    verification_status IS NOT NULL
GROUP BY
    1, 2
ORDER BY
    verification_status, total_loans DESC