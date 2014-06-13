SELECT
    o.Amount AS amount,
    o.Probability AS probability,
    o.AccountId AS account_id_id,
    o.OwnerId AS opp_owner_id_id,
    o.Id AS opportunity_id_id,
    pe.Product2Id AS product_id_id,
    o.StageName AS stage_id_id,
    TO_CHAR(o.CreatedDate, 'DD/MM/YYYY') as created_date,
    TO_CHAR(o.CloseDate, 'DD/MM/YYYY') as close_date,
    o.ForecastCategoryName as forecast_id_id

FROM sfdc_Opportunity_last_snapshot o
    LEFT OUTER JOIN sfdc_OpportunityLineItem_last_snapshot oli
ON o.Id = oli.OpportunityId
    LEFT OUTER JOIN sfdc_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id