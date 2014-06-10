SELECT
    a.Id AS activity_id,
    TO_CHAR(a.ActivityDate, 'DD/MM/YYYY') AS activity_date,
    TO_CHAR(o.CreatedDate, 'DD/MM/YYYY') AS opp_created_date,
    TO_CHAR(o.CloseDate, 'DD/MM/YYYY') as opp_close_date,
    a.Id AS activity_id_id,
    a.AccountId AS account_id_id,
    a.WhatId AS opportunity_id_id,
    a.OwnerId AS activity_owner_id_id,
    o.OwnerId AS opp_owner_id_id,
    pe.Product2Id AS product_id_id,
    o.StageName AS stage_id_id

FROM (SELECT ActivityDate, Id, AccountId, WhatId, OwnerId FROM dss_Task_last_snapshot
    UNION
    SELECT ActivityDate, Id, AccountId, WhatId, OwnerId FROM dss_Event_last_snapshot
    ) a
    LEFT OUTER JOIN dss_Opportunity_last_snapshot o
ON a.WhatId = o.Id
    LEFT OUTER JOIN dss_OpportunityLineItem_last_snapshot oli
ON o.Id = oli.OpportunityId
    LEFT OUTER JOIN dss_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id