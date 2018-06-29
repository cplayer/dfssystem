-- IF EXISTS ( SELECT name FROM sys.databases WHERE name = 'nameServerFileList' ) DROP DATABASE `nameServerFileList`;
-- IF EXISTS ( SELECT name FROM sys.databases WHERE name = 'dataServerFileList' ) DROP DATABASE `dataServerFileList`;
DROP TABLE IF EXISTS `nameServerFileList`;
DROP TABLE IF EXISTS `dataServerFileList`;

CREATE TABLE IF NOT EXISTS `nameServerFileList` (
    `fileID` INT UNSIGNED,
    `fileName` VARCHAR(100) NOT NULL,
    `filePath` VARCHAR(255) NOT NULL,
    `fileLen` INT UNSIGNED,
    PRIMARY KEY (`fileID`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- CREATE TABLE IF NOT EXISTS `dataServerFileChunkList` (
--     `fileID` INT UNSIGNED,
--     `chunkID` INT UNSIGNED,
--     `chunkFilePath` VARCHAR(255) NOT NULL,
--     `chunkFileName` VARCHAR(255) NOT NULL,
--     PRIMARY KEY (`fileID`)
-- )ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `dataServerFileList` (
    `index` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `fileID` INT UNSIGNED,
    `fileChunk` INT UNSIGNED,
    `fileChunkTotal` INT UNSIGNED,
    `filePath` VARCHAR(255) NOT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8;