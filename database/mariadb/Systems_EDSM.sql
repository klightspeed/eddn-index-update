CREATE TABLE `Systems_EDSM` (
	`Id` INT(11) NOT NULL,
	`EdsmId` INT(11) NOT NULL,
	`TimestampSeconds` INT(11) NOT NULL,
	`Timestamp` DATETIME DEFAULT NULL AS ('2014-01-01' + interval `TimestampSeconds` second) virtual,
	`HasCoords` BIT(1) NOT NULL,
	`IsHidden` BIT(1) NOT NULL,
	`IsDeleted` BIT(1) NOT NULL,
	PRIMARY KEY (`EdsmId`) USING BTREE,
	INDEX `Id` (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
