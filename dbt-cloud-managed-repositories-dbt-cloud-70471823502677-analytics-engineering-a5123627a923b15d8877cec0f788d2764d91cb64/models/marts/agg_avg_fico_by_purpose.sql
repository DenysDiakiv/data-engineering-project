{{ config(
    materialized='table', 
    description='Середній кредитний бал FICO за метою кредиту'
) }}

SELECT
    purpose,
    COUNT(*) AS total_loans,
 
    AVG(fico_range_low) AS avg_fico_low, 
    AVG(loan_amnt) AS avg_loan_amount,

    AVG(dti) AS avg_dti

FROM
    {{ ref('stg_accepted_loans') }}
WHERE
    purpose IS NOT NULL AND fico_range_low IS NOT NULL
GROUP BY
    1
ORDER BY
    total_loans DESC