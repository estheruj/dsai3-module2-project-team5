SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position,
    CONCAT(data_type, ' (', CASE WHEN is_nullable = 'YES' THEN 'NULLABLE' ELSE 'REQUIRED' END, ')') AS full_type
FROM
    `dsai-module2-project.brazilian_ecommerce_staging.INFORMATION_SCHEMA.COLUMNS`
ORDER BY
    table_name,
    ordinal_position


SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position,
    -- Combine data_type and is_nullable into one column for easier processing
    CONCAT(data_type, ' (', CASE WHEN is_nullable = 'YES' THEN 'NULLABLE' ELSE 'REQUIRED' END, ')') AS full_type
FROM
    `dsai-module2-project.brazilian_ecommerce_marts.INFORMATION_SCHEMA.COLUMNS`
ORDER BY
    table_name,
    ordinal_position


SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position,
    -- Combine data_type and is_nullable into one column for easier processing
    CONCAT(data_type, ' (', CASE WHEN is_nullable = 'YES' THEN 'NULLABLE' ELSE 'REQUIRED' END, ')') AS full_type
FROM
    `dsai-module2-project.brazilian_ecommerce.INFORMATION_SCHEMA.COLUMNS`
ORDER BY
    table_name,
    ordinal_position