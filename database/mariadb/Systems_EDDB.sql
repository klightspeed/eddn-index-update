CREATE TABLE `Systems_EDDB` (
	`Id` INT(11) NOT NULL,
	`EddbId` INT(11) NOT NULL,
	`TimestampSeconds` INT(11) NOT NULL,
	`Timestamp` DATETIME DEFAULT NULL AS ('1970-01-01' + interval `TimestampSeconds` second) virtual,
	PRIMARY KEY (`EddbId`) USING BTREE,
	INDEX `Id` (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
