{{ config(materialized='view') }}

SELECT
    "Amount Requested" AS loan_amnt,
    
    TRY_CAST(
        CONCAT_WS('-', 
            SUBSTRING("Application Date", 5, 4), 
            CASE SUBSTRING("Application Date", 1, 3) 
                WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                ELSE NULL
            END,
            '01'
        ) AS DATE
    ) AS issue_d,
    
    "Loan Title" AS title,
    CAST(risk_score AS DOUBLE) AS risk_score,
    
    CAST(REGEXP_REPLACE("Debt-To-Income Ratio", '[%| ]', '') AS DOUBLE) AS dti,
    
    "Zip Code" AS zip_code,
    state AS addr_state,
    
    CAST(REGEXP_REPLACE("Employment Length", '[<|>|+| years| year|\s]', '') AS DOUBLE) AS emp_length,
    
    'Rejected' AS loan_status,
    
    "Policy Code" AS policy_code
    
FROM
    {{ source('raw_data', 'rejected_clean_delta') }}
WHERE 
    TRUE