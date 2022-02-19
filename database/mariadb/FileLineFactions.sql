CREATE TABLE `FileLineFactions` (
	`FileId` INT(11) NOT NULL,
	`LineNo` INT(11) NOT NULL,
	`FactionId` INT(11) NOT NULL,
	`EntryNum` TINYINT(4) NOT NULL,
	PRIMARY KEY (`FileId`, `LineNo`, `EntryNum`) USING BTREE,
	INDEX `FactionId` (`FactionId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
