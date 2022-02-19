CREATE TABLE `FilePrefixes` (
	`Prefix` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	`PrimarySchema` VARCHAR(128) NOT NULL COLLATE 'utf8_general_ci',
	`EventType` VARCHAR(32) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
    `MinDate` DATE NOT NULL,
    `MaxDate` DATE NOT NULL
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;;
