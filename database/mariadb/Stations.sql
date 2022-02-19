CREATE TABLE `Stations` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`MarketId` BIGINT(20) NULL DEFAULT NULL,
	`SystemName` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	`StationName` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	`SystemId` INT(11) NULL DEFAULT NULL,
	`StationType` VARCHAR(128) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`StationType_Location` VARCHAR(128) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`Body` VARCHAR(128) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`BodyID` INT(11) NULL DEFAULT NULL,
	`IsRejected` BIT(1) NOT NULL DEFAULT b'0',
	`ValidFrom` DATETIME NOT NULL DEFAULT '2014-01-01 00:00:00',
	`ValidUntil` DATETIME NOT NULL DEFAULT '9999-12-31 00:00:00',
	`Test` BIT(1) NOT NULL DEFAULT b'0',
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `SystemMarket` (`SystemId`, `MarketId`) USING BTREE,
	INDEX `SystemStation` (`SystemName`, `StationName`, `MarketId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
