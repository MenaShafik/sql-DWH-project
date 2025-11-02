/*
================================================
create database and schemas
================================================
script putpose :
it creates a new db named 'datawarehouse' after checking if it already exists.
it is dropped and recreated,
the script sets up three schemas within db: 'bronze','silver' and 'gold'.

WARNIING :
RUNNING THIS SCRIPT WILL DROP THE ENTIRE 'datawarehouse' db if it exists.
all data in db will be permanently deleted
*/

use master;
go
--drop and recreate the 'datawarehouse' db
IF EXISTS(SELECT 1 FROM sys.databases WHERE name  = 'DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET  SIGLE USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END
GO

--CREATE the 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

