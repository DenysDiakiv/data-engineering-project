{{ config(
    materialized='table', 
    description='Статистика платежів за терміном кредиту (36/60 місяців)'
) }}

SELECT
    term,
    COUNT(*) AS total_loans,
    
    SUM(total_pymnt) AS total_payments_received,
    SUM(total_rec_int) AS total_interest_received,
    SUM(recoveries) AS total_recoveries,
    
    AVG(total_pymnt) AS avg_total_payment,
    AVG(total_rec_prncp) AS avg_principal_received

FROM
    {{ ref('stg_accepted_loans') }}
WHERE
    term IS NOT NULL
GROUP BY
    1
ORDER BY
    term