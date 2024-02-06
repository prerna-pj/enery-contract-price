-- 1. How did the average revenue (base price + consumption * energy price)  
-- per contract develop between 01.10.2020 and 01.01.2021?
-- Assumption: 
-- 1. consuption = usage
-- 2. The price of the contract depends when the contract was created
-- 3. Not considering the contracts whice product-price mapping for the start timeframe of the contract is not prosent
-- Note: Please feel free to select more columns as required for your use case
SELECT
    cpp_map.contractid,
    p.productcode,
    p.productname,
    p.energy,
    (cpp_map.price_baseprice + (cpp_map.usage * cpp_map.price_workingprice))
        AS revenue_per_year
FROM contracts_products_prices_map AS cpp_map
LEFT JOIN products AS p
    ON cpp_map.productid = p.id;

-- 2. How many contracts were on delivery on 01.01.2021?
-- Assumption: 
-- 1. On delivery, is the time between the start date and end date of a contracts
SELECT COUNT(DISTINCT id) AS tot_contracts
FROM contracts
WHERE '2021-01-01' BETWEEN startdate AND COALESCE(enddate, '9999-12-31');

-- 3. How many new contract were loaded into the DWH on 01.12.2020?
-- Assumption:
-- 1. The load date is the date the contract is loaded to the DWH. 
-- E.g. a customer could sign a contract today with start date in May 2024. 
-- It would be loaded to the DWH then tonight, but it is not on delivery until May
-- 2. For new contract loaded on May, it should be created within last month ie: April
-- So the no. of contracts loaded into DWH on Dec 2020 will be any contracts created in the month of Nov 2020
SELECT COUNT(*) AS tot_new_contracts
FROM contracts
WHERE
    createdat BETWEEN DATE('2020-12-01', 'start of month', '-1 month', '0 day') AND '2020-12-01';
