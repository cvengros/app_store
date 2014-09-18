-- output: user - opportunity can see
-- each role is a boss for himself

SELECT
    uboss.Id AS gooddata_user_id,
    o.Id AS opportunity_id,
    '0' AS dummy
-- user
FROM dev_salesforce_User_last_snapshot uboss
-- to users with no role, join the opps he owns
-- this might be custom for each implementation
    LEFT OUTER JOIN dev_salesforce_Opportunity_last_snapshot onorole
ON (uboss.UserRoleId IS NULL OR uboss.UserRoleId ='') AND uboss.Id = onorole.OwnerId
-- join user as a boss
    INNER JOIN dev_salesforce_UserRoleHierarchy_last_snapshot h
ON uboss.UserRoleId = h.Id
-- join user as subordinate
    INNER JOIN dev_salesforce_User_last_snapshot usub
ON h.SubordinateId = usub.UserRoleId
-- join opps that subordinate owns
    INNER JOIN dev_salesforce_Opportunity_last_snapshot o
ON usub.Id = o.OwnerId
UNION
SELECT
    uboss.Id AS gooddata_user_id,
    o.Id AS opportunity_id,
    '0' AS dummy
-- user
FROM (SELECT * FROM dev_salesforce_User_last_snapshot WHERE UserRoleId IS NULL OR UserRoleId ='') uboss
-- to users with no role, join the opps he owns
-- this might be custom for each implementation
    INNER JOIN dev_salesforce_Opportunity_last_snapshot o
ON uboss.Id = o.OwnerId
