/*
=======================================================
Create Database and Schemas
=======================================================
Script Purpose
  This script creats a new database named'Datawarehouse' after checking if it exists before.
  If the database exists, it is dropper and recreated.
  And also creating 3 schemas 'bronze', 'silver', 'gold'.
*/

USE master;
GO

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse
END;
GO

--Create the Datawarehouse database
CREATE DATABASE Datawarehouse;
GO
  
USE Datawarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
