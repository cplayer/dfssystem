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
    `fileID` INT UNSIGNED,
    `fileChunk` INT UNSIGNED,
    `fileChunkTotal` INT UNSIGNED,
    `filePath` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`fileID`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;