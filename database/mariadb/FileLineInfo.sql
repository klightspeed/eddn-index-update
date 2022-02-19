CREATE TABLE `FileLineInfo` (
	`FileId` INT(11) NOT NULL,
	`LineNo` INT(11) NOT NULL,
	`SoftwareId` INT(11) NOT NULL,
	`LineLength` INT(11) NULL DEFAULT NULL,
	`SystemId` INT(11) NULL DEFAULT NULL,
	`BodyId` INT(11) NULL DEFAULT NULL,
	`Timestamp` DATETIME NOT NULL,
	`GatewayTimestamp` DATETIME(6) NOT NULL,
	`DistFromArrivalLS` FLOAT NULL DEFAULT NULL,
	`HasBodyId` BIT(1) NOT NULL,
	`HasSystemAddress` BIT(1) NOT NULL,
	`HasMarketId` BIT(1) NOT NULL,
	PRIMARY KEY (`FileId`, `LineNo`) USING BTREE,
	INDEX `SystemId` (`SystemId`) USING BTREE,
	INDEX `BodyId` (`BodyId`) USING BTREE,
	INDEX `Timestamp` (`Timestamp`) USING BTREE,
	INDEX `SoftwareId` (`SoftwareId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
