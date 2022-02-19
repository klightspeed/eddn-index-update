CREATE TABLE `Systems_Named` (
	`Id` INT(11) NOT NULL,
	`Name` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `Name` (`Name`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
