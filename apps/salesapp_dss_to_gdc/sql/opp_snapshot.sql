SELECT
    o.Amount AS amount,
    o.Probability AS probability,
    o.AccountId AS account_id_id,
    o.OwnerId AS opp_owner_id_id,
    o.Id AS opportunity_id_id,
    pe.Product2Id AS product_id_id,
    o.StageName AS stage_id_id,
    TO_CHAR(o._INSERTED_AT, 'DD/MM/YYYY') AS snapshot_date,
    TO_CHAR(o.CreatedDate, 'DD/MM/YYYY') as created_date,
    TO_CHAR(o.CloseDate, 'DD/MM/YYYY') as close_date,
    o.ForecastCategoryName as forecast_id_id

FROM dss_Opportunity_snapshot o
    LEFT OUTER JOIN dss_OpportunityLineItem_last_snapshot oli
ON o.Id = oli.OpportunityId
    LEFT OUTER JOIN dss_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id;

-- fun analytic sql to fill in snapshots that have null values in some columns with first known value
SELECT
    o.amount,
    o.probability,
    ISNULL(o.AccountId, AccountId_default) AS account_id_id,
      --o.OwnerId AS opp_owner_id_id,
    o.opportunity_id_id,

    --pe.Product2Id AS product_id_id,
    o.stage_id_id,
    o.snapshot_date,

    o.created_date,
    o.close_date
      --o.ForecastCategoryName as forecast_id_id


FROM (
  SELECT
    *,
    FIRST_VALUE(AccountId_first_value) OVER (PARTITION BY opportunity_id_id ORDER BY AccountId_first_value) AS AccountId_default
  FROM
    (
      SELECT
        *,
        CASE
          WHEN (NOT (AccountId IS NULL)) AND (AccountId_prev_line IS NULL)
          THEN AccountId
          ELSE NULL
        END AS AccountId_first_value
      FROM
      (
        SELECT
          Amount AS amount,
          Probability AS probability,
          Id AS opportunity_id_id,
            AccountId,
          LAG(AccountId) OVER (PARTITION BY Id ORDER BY _SNAPSHOT_AT) AS AccountId_prev_line,
          StageName AS stage_id_id,
          TO_CHAR(_SNAPSHOT_AT, 'DD/MM/YYYY') AS snapshot_date,
          TO_CHAR(CreatedDate, 'DD/MM/YYYY') as created_date,
          TO_CHAR(CloseDate, 'DD/MM/YYYY') as close_date,

          FIRST_VALUE(AccountId) OVER (PARTITION BY Id ORDER BY _SNAPSHOT_AT) AS AccountId_first_version
        FROM
          dss_Opportunity_snapshot os1
        WHERE Id = '006U0000004I67hIAC'
      ) os2
    ) os3
) o
    LEFT OUTER JOIN dss_OpportunityLineItem_last_snapshot oli
ON o.opportunity_id_id = oli.OpportunityId
    LEFT OUTER JOIN dss_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id