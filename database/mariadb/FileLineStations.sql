CREATE TABLE `FileLineStations` (
	`FileId` INT(11) NOT NULL,
	`LineNo` INT(11) NOT NULL,
	`StationId` INT(11) NOT NULL,
	PRIMARY KEY (`FileId`, `LineNo`) USING BTREE,
	INDEX `StationId` (`StationId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
