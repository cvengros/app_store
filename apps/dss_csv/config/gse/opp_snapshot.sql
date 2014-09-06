-- generates snapshot - for each day there's a line
-- timeseries only genereates snapshots between the first and the last known change, that's why we union an empty line with future valid from

SELECT
    oli.TotalPrice AS amount,
    o.Probability AS probability,
    o.AccountId AS account_id_id,
    o.OwnerId AS opp_owner_id_id,
    o.Id AS opportunity_id_id,

    pe.Product2Id AS product_id_id,

    o.StageName AS stage_id_id,
    TO_CHAR(o._SNAPSHOT_DATE, 'DD/MM/YYYY') AS snapshot_date,
    TO_CHAR(o.CreatedDate, 'DD/MM/YYYY') as created_date,
    TO_CHAR(o.CloseDate, 'DD/MM/YYYY') as close_date,
    o.ForecastCategoryName as forecast_id_id

FROM (
    SELECT *
    FROM (
        SELECT
            CAST(_SNAPSHOT_DATE AS DATE),
            Id,
            TS_LAST_VALUE(_LOAD_ID, 'CONST') AS _LOAD_ID,
            TS_LAST_VALUE(Probability, 'CONST') AS Probability,
            TS_LAST_VALUE(AccountId, 'CONST') AS AccountId,
            TS_LAST_VALUE(OwnerId, 'CONST') AS OwnerId,
            TS_LAST_VALUE(StageName, 'CONST') AS StageName,
            TS_LAST_VALUE(CreatedDate, 'CONST') AS CreatedDate,
            TS_LAST_VALUE(CloseDate, 'CONST') AS CloseDate,
            TS_LAST_VALUE(ForecastCategoryName, 'CONST') AS ForecastCategoryName,
            TS_LAST_VALUE(_VALID_FROM, 'CONST') AS _VALID_FROM
        FROM (
            SELECT
                _LOAD_ID,
                Probability,
                AccountId,
                OwnerId,
                Id,
                StageName,
                CreatedDate,
                CloseDate,
                ForecastCategoryName,
                _VALID_FROM
            FROM dev_salesforce_Opportunity
            UNION ALL -- need a far future fake record for each id to make the time series generator work
            SELECT DISTINCT
                '-1' AS _LOAD_ID,
                NULL,
                NULL,
                NULL,
                Id,
                NULL,
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS TIMESTAMP),
                NULL,
                CURRENT_DATE + 1
            FROM dev_salesforce_Opportunity
        ) q
        TIMESERIES _SNAPSHOT_DATE AS '1 day'
            OVER (PARTITION BY Id ORDER BY _VALID_FROM)
    ) q WHERE _LOAD_ID <> -1 -- eliminate the fake far future records
) o
    LEFT OUTER JOIN dev_salesforce_OpportunityLineItem oli
-- interpolating the latest older version of line item for each snapshot I have for opportunity
ON o.Id = oli.OpportunityId AND o._VALID_FROM INTERPOLATE PREVIOUS VALUE oli._VALID_FROM
    LEFT OUTER JOIN dev_salesforce_PricebookEntry pe
ON oli.PricebookEntryId = pe.Id AND oli._VALID_FROM INTERPOLATE PREVIOUS VALUE pe._VALID_FROM


