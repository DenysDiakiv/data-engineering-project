{{ config(
    materialized='table', 
    file_format='delta',
    partition_by=['addr_state']
) }}

SELECT
    loan_amnt,
    issue_d,
    title,
    dti,
    zip_code,
    addr_state,
    emp_length,
    loan_status
FROM
    {{ ref('stg_accepted_loans') }}

UNION ALL

SELECT
    loan_amnt,
    issue_d,
    title,
    dti,
    zip_code,
    addr_state,
    emp_length,
    loan_status
FROM
    {{ ref('stg_rejected_loans') }}