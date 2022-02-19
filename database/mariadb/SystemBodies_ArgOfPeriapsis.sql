CREATE TABLE `SystemBodies_ArgOfPeriapsis` (
	`Id` INT(11) NOT NULL,
	`ArgOfPeriapsis` FLOAT(30,10) NOT NULL,
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `Id` (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
