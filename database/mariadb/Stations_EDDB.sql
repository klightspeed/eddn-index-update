CREATE TABLE `Stations_EDDB` (
	`Id` INT(11) NOT NULL,
	`EddbStationId` INT(11) NOT NULL,
	`Timestamp` DATETIME NOT NULL,
	PRIMARY KEY (`EddbStationId`) USING BTREE,
	INDEX `Id` (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
