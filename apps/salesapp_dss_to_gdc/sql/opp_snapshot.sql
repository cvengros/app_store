SELECT
    --o.Name,
    --pe.Product2Id,
    --o._VALID_FROM,
    --oli._VALID_FROM,
    --pe._VALID_FROM,
    o.Amount AS amount,
    o.Probability AS probability,
    o.AccountId AS account_id_id,
    o.OwnerId AS opp_owner_id_id,
    o.Id AS opportunity_id_id,
    pe.Product2Id AS product_id_id,
    o.StageName AS stage_id_id,
    TO_CHAR(o._VALID_FROM, 'DD/MM/YYYY') AS snapshot_date,
    TO_CHAR(o.CreatedDate, 'DD/MM/YYYY') as created_date,
    TO_CHAR(o.CloseDate, 'DD/MM/YYYY') as close_date,
    o.ForecastCategoryName as forecast_id_id

FROM sfdc_Opportunity o
    LEFT OUTER JOIN sfdc_OpportunityLineItem oli
-- interpolating the latest older version of line item for each snapshot I have for opportunity
ON o.Id = oli.OpportunityId AND o._VALID_FROM INTERPOLATE PREVIOUS VALUE oli._VALID_FROM
    LEFT OUTER JOIN sfdc_PricebookEntry pe
ON oli.PricebookEntryId = pe.Id AND oli._VALID_FROM INTERPOLATE PREVIOUS VALUE pe._VALID_FROM
