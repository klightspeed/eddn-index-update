CREATE TABLE `Systems` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`ModSystemAddress` BIGINT(20) NOT NULL,
	`X` MEDIUMINT(9) NOT NULL,
	`Y` MEDIUMINT(9) NOT NULL,
	`Z` MEDIUMINT(9) NOT NULL,
	`RegionAddress` INT(11) DEFAULT NULL AS (`ModSystemAddress` >> 40) virtual,
	`Mid1a` TINYINT(3) UNSIGNED DEFAULT NULL AS ((`ModSystemAddress` >> 16 & 0x1fffff) MOD 26) virtual,
	`Mid1b` TINYINT(3) UNSIGNED DEFAULT NULL AS (floor((`ModSystemAddress` >> 16 & 0x1fffff) / 26 MOD 26)) virtual,
	`Mid2` TINYINT(3) UNSIGNED DEFAULT NULL AS (floor((`ModSystemAddress` >> 16 & 0x1fffff) / (26 * 26) MOD 26)) virtual,
	`SizeClass` TINYINT(3) UNSIGNED DEFAULT NULL AS (`ModSystemAddress` >> 37 & 7) virtual,
	`Mid3` TINYINT(3) UNSIGNED DEFAULT NULL AS (floor((`ModSystemAddress` >> 16 & 0x1fffff) / (26 * 26 * 26))) virtual,
	`Sequence` SMALLINT(5) UNSIGNED DEFAULT NULL AS (`ModSystemAddress` & 65535) virtual,
	`SystemAddress` BIGINT(20) DEFAULT NULL AS ((`ModSystemAddress` & 0xffff) << 44 - (`ModSystemAddress` >> 37 & 7) * 3 | (`ModSystemAddress` >> 40 & 0x7f) << 37 - (`ModSystemAddress` >> 37 & 7) * 3 | (`ModSystemAddress` >> 16 & 0x7f) << 30 - (`ModSystemAddress` >> 37 & 7) * 2 | (`ModSystemAddress` >> 47 & 0x3f) << 24 - (`ModSystemAddress` >> 37 & 7) * 2 | (`ModSystemAddress` >> 23 & 0x7f) << 17 - (`ModSystemAddress` >> 37 & 7) * 1 | (`ModSystemAddress` >> 53 & 0x7f) << 10 - (`ModSystemAddress` >> 37 & 7) * 1 | (`ModSystemAddress` >> 30 & 0x7f) << 3 | `ModSystemAddress` >> 37 & 7) virtual,
	`PGSuffix` VARCHAR(128) DEFAULT NULL AS (concat(' ',char(`Mid1a` + 65),char(`Mid1b` + 65),'-',char(`Mid2` + 65),' ',char(`SizeClass` + 97),case when `Mid3` = 0 then '' else concat(`Mid3`,'-') end,`Sequence`)) virtual COLLATE 'utf8_general_ci',
	`IsHASystem` BIT(1) NOT NULL,
	`IsNamedSystem` BIT(1) NOT NULL,
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `IX_ModSystemAddress` (`ModSystemAddress`) USING BTREE,
	INDEX `StarPos` (`Z`, `Y`, `X`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
