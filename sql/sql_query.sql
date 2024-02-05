-- 1. How did the average revenue (base price + consumption * energy price)  
-- per contract develop between 01.10.2020 and 01.01.2021?
-- Assumption: 
-- consuption = usage and the price of the contract depends when the contract was created
SELECT c.contractid, c.productid, (cp_map.price_baseprice + (c.usage * cp_map.price_workingprice)) AS revenue
FROM contracts AS c
INNER JOIN contracts_prices_map AS cp_map
ON c.id = cp_map.id;

-- 2. How many contracts were on delivery on 01.01.2021?
-- Assumption: 
-- On delivery, is the time between the start date and end date of a contracts
SELECT COUNT(DISTINCT id) AS tot_contracts
FROM contracts
WHERE '2021-01-01' BETWEEN startdate AND COALESCE(endate, '9999-12-31');

-- 3. How many new contract were loaded into the DWH on 01.12.2020?
-- Assumption:
-- The load date is the date the contract is loaded to the DWH. 
-- E.g. a customer could sign a contract today with start date in May 2024. 
-- It would be loaded to the DWH then tonight, but it is not on delivery until May
SELECT COUNT(*) AS tot_new_contracts
FROM contracts
WHERE status = 'indelivery' AND createdat = '2020-12-01' AND startdate >= '2020-12-01';
