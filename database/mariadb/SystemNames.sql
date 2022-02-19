CREATE VIEW `SystemNames` AS
SELECT
    `s`.`Id` AS `Id`,
    `s`.`ModSystemAddress` AS `ModSystemAddress`,
    `s`.`SystemAddress` AS `SystemAddress`,
    `s`.`X` AS `X`,
    `s`.`Y` AS `Y`,
    `s`.`Z` AS `Z`,
    CASE
        WHEN `n`.`Name` IS NOT NULL THEN `n`.`Name`
        WHEN `h`.`Id` IS NOT NULL THEN CONCAT(
            `hr`.`Name`,
            `h`.`PGSuffix`
        )
        ELSE CONCAT(
            `sr`.`Name`,
            `s`.`PGSuffix`
        ) 
    END AS `Name`,
    CONCAT(`sr`.`Name`,`s`.`PGSuffix`) AS `PGName`,
    `s`.`IsHASystem` AS `IsHASystem`,
    `s`.`IsNamedSystem` AS `IsNamedSystem`,
    COALESCE(
        `sv`.`ValidFrom`,
        CAST('2014-01-01' AS DATETIME)
    ) AS `ValidFrom`,
    COALESCE(
        `sv`.`ValidUntil`,
        CAST('9999-12-31' AS DATETIME)
    ) AS `ValidUntil`,
    COALESCE(`sv`.`IsRejected`,0) AS `IsRejected`,
    `ss`.`SimbadName` AS `SimbadName`,
    `ss`.`SimbadIdent` AS `SimbadIdent`,
    `ss`.`RA_J2000` AS `Simbad_RAJ2000`,
    `ss`.`Dec_J2000` AS `Simbad_DEJ2000`,
    `ss`.`Parallax` AS `Simbad_Parallax`,
    `ss`.`EpochError_J2000B1950` AS `EpochError_J2000B1950`,
    `sg`.`GaiaDR2SourceId` AS `GaiaDR2SourceId`,
    COALESCE(
        `sp`.`PermitName`,
        CASE
            WHEN `sr`.`IsPermitLocked` = 1 THEN 'Unknown'
            ELSE NULL
        END
    ) AS `PermitName`
FROM `Systems` `s`
LEFT JOIN `Regions` `sr` ON `sr`.`RegionAddress` = `s`.`RegionAddress`
LEFT JOIN `Systems_HASector` `h` ON `h`.`Id` = `s`.`Id`
LEFT JOIN `Regions` `hr` ON `hr`.`Id` = `h`.`RegionId`
LEFT JOIN `Systems_Named` `n` ON `n`.`Id` = `s`.`Id`
LEFT JOIN `Systems_Validity` `sv` ON `sv`.`Id` = `s`.`Id`
LEFT JOIN `Systems_Simbad` `ss` ON `ss`.`Id` = `s`.`Id`
LEFT JOIN `Systems_Gaia` `sg` ON `sg`.`Id` = `s`.`Id`
LEFT JOIN `Systems_Permit` `sp` ON `sp`.`Id` = `s`.`Id`