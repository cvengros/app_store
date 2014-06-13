SELECT
  stage_duration,
  stage_velocity,
  account_id_id,
  opp_owner_id_id,
  sc.Id AS opportunity_id_id,
  pe.Product2Id AS product_id_id,
  stage_id_id,
  created_date,
  close_date
FROM (
  SELECT
    DATEDIFF(day,
        date_entered_stage,
        -- date the next stage was entered
        ISNULL(LEAD(os.date_entered_stage) OVER (PARTITION BY Id ORDER BY _VALID_FROM), last_snapshot)
    ) AS stage_duration,
    DATEDIFF(day,
      CreatedDate_last_version,
      date_entered_stage
    ) AS stage_velocity,
    AccountId_last_version AS account_id_id,
    OwnerId_last_version AS opp_owner_id_id,
    Id,
    StageName AS stage_id_id,
    TO_CHAR(CreatedDate_last_version, 'DD/MM/YYYY') as created_date,
    TO_CHAR(CloseDate_last_version, 'DD/MM/YYYY') as close_date
  FROM (
    SELECT
      Id,
      StageName,
      -- the first snapshot when it was in that stage
      FIRST_VALUE(_VALID_FROM) OVER (PARTITION BY Id, StageName  ORDER BY _VALID_FROM) AS date_entered_stage,
      -- stage on the previous line
      LAG(StageName) OVER (PARTITION BY Id ORDER BY _VALID_FROM)  AS previous_line_stage,
      Name,
      -- current column values
      FIRST_VALUE(Name) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS Name_last_version,
      FIRST_VALUE(AccountId) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS AccountId_last_version,
      FIRST_VALUE(OwnerId) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS OwnerId_last_version,
      FIRST_VALUE(CreatedDate) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS CreatedDate_last_version,
      FIRST_VALUE(CloseDate) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS CloseDate_last_version,
      FIRST_VALUE(_VALID_FROM) OVER (PARTITION BY Id ORDER BY _VALID_FROM DESC) AS last_snapshot,
      _VALID_FROM

    FROM  sfdc_Opportunity
  ) os
  WHERE os.previous_line_stage is NULL OR os.previous_line_stage <> os.StageName
) sc
LEFT OUTER JOIN sfdc_OpportunityLineItem_last_snapshot oli
ON sc.Id = oli.OpportunityId

LEFT OUTER JOIN sfdc_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id