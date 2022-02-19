CREATE VIEW `SystemBodyNames` AS
SELECT
    `sb`.`Id` AS `Id`,
    `sb`.`SystemId` AS `SystemId`,
    CASE
        WHEN `sb`.`HasBodyId` = 1 THEN `sb`.`BodyId`
        ELSE NULL
    END AS `BodyID`,
    `sn`.`Name` AS `SystemName`,
    `sbd`.`Id` AS `BodyDesignationId`,
    `sbd`.`BodyDesignation` AS `BodyDesignation`,
    COALESCE(
        `sbn`.`Name`,
        CONCAT(`sn`.`Name`,`sbd`.`BodyDesignation`)
    ) AS `BodyName`,
    `sbd`.`BodyCategory` AS `BodyCategory`,
    `sbd`.`BodyCategoryDescription` AS `BodyCategoryDescription`,
    `sbd`.`Stars` AS `Stars`,
    `sbd`.`Planet` AS `Planet`,
    `sbd`.`Moon1` AS `Moon1`,
    `sbd`.`Moon2` AS `Moon2`,
    `sbd`.`Moon3` AS `Moon3`,
    `sbn`.`Name` AS `CustomName`,
    `aop`.`ArgOfPeriapsis` AS `ArgOfPeriapsis`,
    `sb`.`IsNamedBody` AS `IsNamedBody`,
    COALESCE(
        `bv`.`ValidFrom`,
        CAST('2014-01-01' as DATETIME)
    ) AS `ValidFrom`,
    COALESCE(
        `bv`.`ValidUntil`,
        CAST('9999-12-31' as DATETIME)
    ) AS `ValidUntil`,
    COALESCE(
        `bv`.`IsRejected`,
        0
    ) AS `IsRejected`
FROM `SystemBodies` `sb`
JOIN `SystemNames` `sn` ON `sn`.`Id` = `sb`.`SystemId`
LEFT JOIN `SystemBodies_Named` `sbn` ON `sbn`.`Id` = `sb`.`Id`
LEFT JOIN `SystemBodyDesignations` `sbd` ON `sbd`.`Id` = `sb`.`BodyDesignationId`
LEFT JOIN `SystemBodies_ArgOfPeriapsis` `aop` ON `aop`.`Id` = `sb`.`Id`
LEFT JOIN `SystemBodies_Validity` `bv` ON `bv`.`Id` = `sb`.`Id`
