DROP TABLE if exists default.test1;

CREATE TABLE IF NOT EXISTS default.test1 LIKE ${HiveTableName};

DROP TABLE IF EXISTS ${HiveTableName};

CREATE TABLE ${HiveTableName} LIKE default.test1;
