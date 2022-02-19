CREATE TABLE `SystemBodies` (
	`Id` INT(11) NOT NULL AUTO_INCREMENT,
	`SystemId` INT(11) NOT NULL,
	`BodyId` SMALLINT(6) NOT NULL,
	`BodyDesignationId` MEDIUMINT(9) NOT NULL,
	`IsNamedBody` BIT(1) NOT NULL,
	`HasBodyId` BIT(1) NOT NULL,
	PRIMARY KEY (`Id`) USING BTREE,
	INDEX `PgDesig` (`IsNamedBody`, `SystemId`, `BodyDesignationId`, `HasBodyId`, `BodyId`, `Id`) USING BTREE
)
COLLATE='utf8_general_ci'
ENGINE=Aria
ROW_FORMAT=FIXED
;;
