-- 1. How did the average revenue (base price + consumption * energy price)  
-- Grundpreis + Verbrauch * Arbeitspreis) per contract develop between 01.10.2020 and 01.01.2021?

-- 2. How many contracts were on delivery on 01.01.2021?
SELECT COUNT(*)
FROM contracts
WHERE status = 'indelivery' AND modificationdate = '2021-01-01';

-- 3. How many new contract were loaded into the DWH on 01.12.2020?
SELECT COUNT(*)
FROM contracts
WHERE status = 'indelivery' AND createdat = '2020-12-01';
