CREATE TABLE `SystemBodies_ParentSet` (
	`Id` INT(11) NOT NULL,
	`ParentSetId` INT(11) NOT NULL,
	UNIQUE INDEX `Id` (`Id`, `ParentSetId`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
