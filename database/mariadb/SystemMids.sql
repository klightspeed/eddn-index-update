CREATE TABLE `SystemMids` (
	`MidVal` MEDIUMINT(9) NOT NULL,
	`MassCode` TINYINT(3) UNSIGNED NOT NULL,
	`Mid1a` TINYINT(3) UNSIGNED NOT NULL,
	`Mid1b` TINYINT(3) UNSIGNED NOT NULL,
	`Mid2` TINYINT(3) UNSIGNED NOT NULL,
	`Mid3` TINYINT(3) UNSIGNED NOT NULL,
	`RelX` INT(11) DEFAULT NULL AS (((`MidVal` & 127) << `MassCode`) * 320) virtual,
	`RelY` INT(11) DEFAULT NULL AS (((`MidVal` >> 7 & 127) << `MassCode`) * 320) virtual,
	`RelZ` INT(11) DEFAULT NULL AS (((`MidVal` >> 14 & 127) << `MassCode`) * 320) virtual,
	`PGSuffix` VARCHAR(16) DEFAULT NULL AS (concat(' ',char(`Mid1a` + 65),char(`Mid1b` + 65),'-',char(`Mid2` + 65),' ',char(`MassCode` + 97),case when `Mid3` = 0 then '' else concat(`Mid3`,'-') end)) stored COLLATE 'utf8_general_ci',
	PRIMARY KEY (`MassCode`, `MidVal`) USING BTREE,
	UNIQUE INDEX `MassCode` (`MassCode`, `Mid3`, `Mid2`, `Mid1b`, `Mid1a`) USING BTREE,
	INDEX `PGSuffix` (`PGSuffix`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
