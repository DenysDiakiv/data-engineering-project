{{ config(materialized='view') }}

SELECT
    loan_amnt,
    
    TRY_CAST(
        CONCAT_WS('-', 
            SUBSTRING(issue_d, 5, 4), 
            CASE SUBSTRING(issue_d, 1, 3) 
                WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                ELSE NULL
            END,
            '01'
        ) AS DATE
    ) AS issue_d,
    
    title,
    CAST(REGEXP_REPLACE(dti, '[% ]', '') AS DOUBLE) AS dti,
    zip_code,
    addr_state,
    CAST(REGEXP_REPLACE(emp_length, '[<|>|+| years| year|\s]', '') AS DOUBLE) AS emp_length,
    'Accepted' AS loan_status,
    
    sec_app_collections_12_mths_ex_med,
    sec_app_mths_since_last_major_derog,
    CAST(REGEXP_REPLACE(hardship_amount, '[$, % ]', '') AS DOUBLE) AS hardship_amount,
    CAST(REGEXP_REPLACE(hardship_payoff_balance_amount, '[$, % ]', '') AS DOUBLE) AS hardship_payoff_balance_amount,
    CAST(REGEXP_REPLACE(hardship_last_payment_amount, '[$, % ]', '') AS DOUBLE) AS hardship_last_payment_amount,

    TRY_CAST(
        CONCAT_WS('-', 
            SUBSTRING(hardship_start_date, 5, 4), 
            CASE SUBSTRING(hardship_start_date, 1, 3) 
                WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                ELSE NULL
            END, '01'
        ) AS DATE
    ) AS hardship_start_date,
    
    hardship_flag,
    hardship_type,
    hardship_reason,
    debt_settlement_flag,
    settlement_status
    
FROM
    {{ source('raw_data', 'accepted_clean_delta') }} 
WHERE
    TRUE