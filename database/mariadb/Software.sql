CREATE TABLE `Software` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`Name` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;