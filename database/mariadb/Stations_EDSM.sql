CREATE TABLE `Stations_EDSM` (
	`Id` INT(11) NOT NULL,
	`EdsmStationId` INT(11) NOT NULL,
	`Timestamp` DATETIME NOT NULL,
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `EdsmStationId` (`EdsmStationId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
