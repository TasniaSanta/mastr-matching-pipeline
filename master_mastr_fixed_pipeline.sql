-- Choose correct role, database, schema
USE ROLE MSB_SANDBOX_ROLE;
USE DATABASE MSB_SANDBOX;
USE SCHEMA SANDBOX_TASNIA_SANTA;


CREATE OR REPLACE TABLE new_EinheitenSolar (
    EINHEITMASTRNUMMER VARCHAR,
    LOKATIONMASTRNUMMER VARCHAR,
    POSTLEITZAHL VARCHAR,
    ORT VARCHAR,
    REGISTRIERUNGSDATUM DATE,
    INBETRIEBNAHMEDATUM DATE,
    BRUTTOLEISTUNG FLOAT,
    NETTONENNLEISTUNG FLOAT,
    ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER FLOAT,
    SPEICHERAMGLEICHENORT BOOLEAN,
    NAMESTROMERZEUGUNGSEINHEIT VARCHAR
);

CREATE OR REPLACE TABLE new_EinheitenStromSpeicher (
    EINHEITMASTRNUMMER VARCHAR,
    LOKATIONMASTRNUMMER VARCHAR,
    POSTLEITZAHL VARCHAR,
    ORT VARCHAR,
    REGISTRIERUNGSDATUM DATE,
    INBETRIEBNAHMEDATUM DATE,
    BRUTTOLEISTUNG FLOAT,
    NETTONENNLEISTUNG FLOAT,
    ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER FLOAT,
    EEGMASTRNUMMER VARCHAR,
    NAMESTROMERZEUGUNGSEINHEIT VARCHAR
);

CREATE OR REPLACE TABLE lokationen (
    MastrNummer STRING,
    VerknuepfteEinheitenMaStRNummern STRING,
    NetzanschlusspunkteMaStRNummern STRING
);

CREATE OR REPLACE TABLE netzanschlusspunkte (
    NetzanschlusspunktMastrNummer STRING,
    LokationMaStRNummer STRING,
    Messlokation STRING,
    Marktgebiet FLOAT
);

SELECT COUNT(*) FROM new_EinheitenSolar;
SELECT COUNT(*) FROM new_EinheitenStromSpeicher;
SELECT COUNT(*) FROM LOKATIONEN;
SELECT COUNT(*) FROM NETZANSCHLUSSPUNKTE;
SELECT * FROM new_EinheitenSolar LIMIT 10;
SELECT * FROM new_EinheitenStromSpeicher LIMIT 10;
SELECT * FROM LOKATIONEN LIMIT 10;
select * from  NETZANSCHLUSSPUNKTE limit 10;

SHOW VIEWS IN MSB_SANDBOX.EXPOSED;
SELECT * FROM MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER LIMIT 100;

--------------------------------------------------------------------------------
-- 1. CREATE CLEAN SEL → SAN → MELO MAPPING.
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SEL_MELO_MAP AS
SELECT
    LokationMaStRNummer AS SEL,
    Messlokation AS MELO,
    NetzanschlusspunktMastrNummer AS SAN
FROM netzanschlusspunkte
WHERE Messlokation IS NOT NULL;


--------------------------------------------------------------------------------
-- 2. BUILD MASTER_MASTR_FIXED
-- This table combines SEL + SAN + MeLo + all SEE (PV / Storage units)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE MASTER_MASTR_FIXED AS
SELECT
    l.MastrNummer AS SEL,
    m.MELO,
    m.SAN,

    -- PV UNIT
    s.EINHEITMASTRNUMMER AS SOLAR_EINHEIT,
    s.BRUTTOLEISTUNG AS SOLAR_KWP,
    s.NAMESTROMERZEUGUNGSEINHEIT AS SOLAR_NAME,

    -- STORAGE UNIT
    sp.EINHEITMASTRNUMMER AS SPEICHER_EINHEIT,
    sp.BRUTTOLEISTUNG AS SPEICHER_KW,
    sp.NAMESTROMERZEUGUNGSEINHEIT AS SPEICHER_NAME

FROM lokationen l
LEFT JOIN SEL_MELO_MAP m
    ON m.SEL = l.MastrNummer

LEFT JOIN new_EinheitenSolar s
    ON s.LOKATIONMASTRNUMMER = l.MastrNummer

LEFT JOIN new_EinheitenStromSpeicher sp
    ON sp.LOKATIONMASTRNUMMER = l.MastrNummer;


--------------------------------------------------------------------------------
-- 3. BUILD LOCATION_SUMMARY_FIXED
-- This version groups by SEL + MELO + SAN
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE LOCATION_SUMMARY_FIXED AS
SELECT
    SEL,
    MELO,
    SAN,

    COUNT(DISTINCT SOLAR_EINHEIT) AS SOLAR_UNITS,
    SUM(SOLAR_KWP) AS TOTAL_SOLAR_KWP,

    COUNT(DISTINCT SPEICHER_EINHEIT) AS STORAGE_UNITS,
    SUM(SPEICHER_KW) AS TOTAL_STORAGE_KW,

    ARRAY_AGG(DISTINCT SOLAR_NAME) AS SOLAR_NAMES,
    ARRAY_AGG(DISTINCT SPEICHER_NAME) AS STORAGE_NAMES,

    ARRAY_AGG(DISTINCT SOLAR_EINHEIT) AS RAW_SOLAR_LIST,
    ARRAY_AGG(DISTINCT SPEICHER_EINHEIT) AS RAW_STORAGE_LIST

FROM MASTER_MASTR_FIXED
GROUP BY SEL, MELO, SAN;

--------------------------------------------------------------------------------
-- 4. CUSTOMER MATCH (FINAL, CORRECT)
-- Matches CRM customers to MaStR..
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_MATCH_FIXED AS
SELECT
    cust.METERINGCODE,
    cust."Salesforce Customer ID",
    cust."Salesforce Account ID",
    cust.GCID AS CUSTOMER_GCID,
    cust.ABN_NR,
    cust.ABN_PLZ,
    cust.ABN_ORT,
    cust.ABN_STRASSE,
    cust.ABN_HAUSNR,

    -- Attached MaStR data
    ls.SEL,
    ls.SAN,
    ls.SOLAR_UNITS,
    ls.TOTAL_SOLAR_KWP,
    ls.STORAGE_UNITS,
    ls.TOTAL_STORAGE_KW,
    ls.SOLAR_NAMES,
    ls.STORAGE_NAMES,
    ls.RAW_SOLAR_LIST,
    ls.RAW_STORAGE_LIST,

    CASE
        WHEN cust.METERINGCODE IS NULL THEN 'CUSTOMER_NO_MELO'
        WHEN ls.MELO IS NULL THEN 'NO_MATCH'
        ELSE 'MATCH'
    END AS MATCH_STATUS

FROM MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER cust
LEFT JOIN LOCATION_SUMMARY_FIXED ls
    ON cust.METERINGCODE = ls.MELO;

select * from  CUSTOMER_MATCH_FIXED limit 10;

--------------------------------------------------------------------------------
-- 5. KEY METRICS SECTION
-- This produces all important KPIs for the analysis.
--------------------------------------------------------------------------------

-- A. Total CRM customers 98659
SELECT COUNT(*) AS TOTAL_CRM_CUSTOMERS
FROM MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER;

-- B. CRM MeLo that exist in MaStR (THEORETICAL MAX MATCH)
-- This number is: ~41,442
SELECT COUNT(DISTINCT c.METERINGCODE) AS CRM_MELO_IN_MASTR
FROM MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER c
JOIN netzanschlusspunkte n
    ON c.METERINGCODE = n.Messlokation;


-- C. Matched customers (final correct value: ~41,431)
SELECT COUNT(DISTINCT METERINGCODE) AS MATCHED_CUSTOMERS
FROM CUSTOMER_MATCH_FIXED
WHERE MATCH_STATUS = 'MATCH';


-- D. Unmatched customers--------57227
SELECT COUNT(DISTINCT METERINGCODE) AS UNMATCHED_CUSTOMERS
FROM CUSTOMER_MATCH_FIXED
WHERE MATCH_STATUS <> 'MATCH';


-- E. Customers with SAN/SEL but NO PV/BATTERY (the 11 special cases)
SELECT COUNT(*) AS NO_UNIT_CUSTOMERS
FROM LOCATION_SUMMARY_FIXED
WHERE SOLAR_UNITS = 0
  AND STORAGE_UNITS = 0
  AND MELO IS NOT NULL;


--------------------------------------------------------------------------------
-- 6. TABLE OF THE 11 CUSTOMERS (SAN + SEL exist, but NO UNITS)

SELECT *
FROM CUSTOMER_MATCH_FIXED
WHERE MATCH_STATUS = 'NO_MATCH'
  AND (SOLAR_UNITS IS NULL OR SOLAR_UNITS = 0)
  AND (STORAGE_UNITS IS NULL OR STORAGE_UNITS = 0)
  AND METERINGCODE IN (
        SELECT DISTINCT c.METERINGCODE
        FROM MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER c
        JOIN netzanschlusspunkte n
            ON c.METERINGCODE = n.Messlokation
  );
  
SELECT *
FROM CUSTOMER_MATCH_FIXED
WHERE MATCH_STATUS = 'MATCH';

