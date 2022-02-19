CREATE TABLE `FileLineNavRoutes` (
	`FileId` INT(11) NOT NULL,
	`LineNo` INT(11) NOT NULL,
	`SystemId` INT(11) NOT NULL,
	`EntryNum` SMALLINT(6) NOT NULL,
	PRIMARY KEY (`FileId`, `LineNo`, `EntryNum`) USING BTREE,
	INDEX `SystemId` (`SystemId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
