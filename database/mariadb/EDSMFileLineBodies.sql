CREATE TABLE `EDSMFileLineBodies` (
	`FileId` INT(11) NOT NULL,
	`LineNo` INT(11) NOT NULL,
	`EdsmBodyId` INT(11) NOT NULL,
	PRIMARY KEY (`FileId`, `LineNo`) USING BTREE,
	INDEX `EdsmBodyId` (`EdsmBodyId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
