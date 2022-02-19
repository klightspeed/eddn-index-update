CREATE TABLE `ParentSets` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`BodyID` SMALLINT(6) NOT NULL,
	`ParentJson` VARCHAR(255) NOT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `IX_ParentSet_ParentJson` (`BodyID`, `ParentJson`, `Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
