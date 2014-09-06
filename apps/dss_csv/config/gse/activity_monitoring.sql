SELECT
    '0' AS dummy,
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

FROM (SELECT ActivityDate, Id, AccountId, WhatId, OwnerId FROM dev_salesforce_Task_last_snapshot
    UNION
    SELECT ActivityDate, Id, AccountId, WhatId, OwnerId FROM dev_salesforce_Event_last_snapshot
    ) a
    LEFT OUTER JOIN dev_salesforce_Opportunity_last_snapshot o
ON a.WhatId = o.Id
    LEFT OUTER JOIN dev_salesforce_OpportunityLineItem_last_snapshot oli
ON o.Id = oli.OpportunityId
    LEFT OUTER JOIN dev_salesforce_PricebookEntry_last_snapshot pe
ON oli.PricebookEntryId = pe.Id