CREATE TABLE `Systems_Permit` (
	`Id` INT(11) NOT NULL,
	`PermitName` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
