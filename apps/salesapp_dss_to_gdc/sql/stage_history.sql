-- jde jenom o zmeny stage .. ale chci vracet i ostatni hodnoty v momente kdy se menila stage

-- all snapshots when the stage changed

SELECT DISTINCT

    Id, Name, AccountId, OwnerId, CreatedDate, CloseDate,
    StageName,
    -- the first snapshot when it was in that stage - tohle je dobre
    FIRST_VALUE(_SNAPSHOT_AT) OVER (PARTITION BY Id, StageName  ORDER BY _SNAPSHOT_AT ASC) AS date_entered_stage,
    _SNAPSHOT_AT,
    -- the first snapshot after, when it was recorded in another stage .. tam je potreba nadefinovat asi nejak jinak to okno - nebo to sgroupit
    LEAD(_SNAPSHOT_AT, 1) OVER (PARTITION BY Id, StageName  ORDER BY _SNAPSHOT_AT ASC) AS last_date_in_stage
    FROM  dss_Opportunity_snapshot


    WHERE Id = '006U000000AlSx1IAF'

    -- jeste je potreba doresit jak tam doplnit hodnoty, ktery uz znam - asi nejaky first known row a s tim to nejak sjoinovat a isnull



