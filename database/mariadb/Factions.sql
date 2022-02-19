CREATE TABLE `Factions` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`Name` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	`Allegiance` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	`Government` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `Name` (`Name`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
