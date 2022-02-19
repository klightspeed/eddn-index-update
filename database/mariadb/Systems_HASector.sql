CREATE TABLE `Systems_HASector` (
	`Id` INT(11) NOT NULL,
	`ModSystemAddress` BIGINT(20) NOT NULL,
	`RegionId` SMALLINT(6) NOT NULL,
	`Mid1a` TINYINT(3) UNSIGNED NOT NULL,
	`Mid1b` TINYINT(3) UNSIGNED NOT NULL,
	`Mid2` TINYINT(3) UNSIGNED NOT NULL,
	`SizeClass` TINYINT(3) UNSIGNED NOT NULL,
	`Mid3` TINYINT(3) UNSIGNED NOT NULL,
	`Sequence` SMALLINT(5) UNSIGNED NOT NULL,
	`PGSuffix` VARCHAR(128) DEFAULT NULL AS (concat(' ',char(`Mid1a` + 65),char(`Mid1b` + 65),'-',char(`Mid2` + 65),' ',char(`SizeClass` + 97),case when `Mid3` = 0 then '' else concat(`Mid3`,'-') end,`Sequence`)) virtual COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `RegionId` (`RegionId`, `Mid1a`, `Mid1b`, `Mid2`, `SizeClass`, `Mid3`, `Sequence`) USING BTREE,
	INDEX `RegionSysAddress` (`RegionId`, `ModSystemAddress`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
