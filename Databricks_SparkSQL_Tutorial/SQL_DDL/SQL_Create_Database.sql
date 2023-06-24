-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Create Database Details
-- MAGIC | Originally Created By | Raveendra  
-- MAGIC | Reference And Credits  | apache.spark.org  & databricks.com
-- MAGIC
-- MAGIC ###History
-- MAGIC |Date | Developed By | comments
-- MAGIC |----|-----|----
-- MAGIC |23/05/2021|Ravendra| Initial Version
-- MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Database 
-- MAGIC * Creates a database with the specified name. If database with the same name already exists, an exception is thrown.
-- MAGIC * __`Syntax`__
-- MAGIC * __`CREATE { DATABASE | SCHEMA } [ IF NOT EXISTS ] database_name
-- MAGIC     [ COMMENT database_comment ]
-- MAGIC     [ LOCATION database_directory ]
-- MAGIC     [ WITH DBPROPERTIES ( property_name = property_value [ , ... ] ) ]`__
-- MAGIC
-- MAGIC #### Database Creation Parameters :
-- MAGIC
-- MAGIC * __`database_name`__ : The name of the database to be created.
-- MAGIC
-- MAGIC * __`IF NOT EXISTS`__ : Creates a database with the given name if it does not exist. If a database with the same name already exists, nothing will happen.
-- MAGIC
-- MAGIC * __`database_directory`__ :  Path of the file system in which the specified database is to be created. If the specified path does not exist in the underlying file system, creates a directory with the path. If the location is not specified, the database is created in the default warehouse directory, whose path is configured by the static configuration spark.sql.warehouse.dir.
-- MAGIC
-- MAGIC * __`database_comment`__ : The description for the database.
-- MAGIC
-- MAGIC * __`WITH DBPROPERTIES ( property_name=property_value [ , … ] )`__ : The properties for the database in key-value pairs.
-- MAGIC
-- MAGIC ### Describe Database
-- MAGIC #### Database Describe Parameters
-- MAGIC * __Syntax`__ : __`{ DESC | DESCRIBE } DATABASE [ EXTENDED ] db_name`__
-- MAGIC
-- MAGIC * __`db_name`__ : The name of an existing database or an existing schema in the system. If the name does not exist, an exception is thrown.

-- COMMAND ----------

# user/hive/warehouse  -- default databases and tables location. it will create one folder for database as database_name.db

-- COMMAND ----------

create database sparksql_db

-- COMMAND ----------

create table sparksql_db.customers(id int,name string)

-- COMMAND ----------

describe database sparksql_db

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/sparksql_db.db/customers/
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/%fs ls dbfs:/databases/sparksql_db/

-- COMMAND ----------

drop database sales cascade

-- COMMAND ----------

show databases

-- COMMAND ----------

create database if not exists batch30_db;
--create database customers_db comment 'this is customer database we are using for to store customer information'
--create database sales comment 'this database we are using for to store sales information' location '/databases/sales/'
--describe database batch30_db

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/customers

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/batch30_db.db/

-- COMMAND ----------

create table sparksql_db.customers(id int,name string)

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/

-- COMMAND ----------

--create table customers(id int,name string)
insert into sparksql_db.customers values(1,'Ravi')

-- COMMAND ----------

describe database sparksql_db

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.warehouse.dir")

-- COMMAND ----------

create database IF NOT EXISTS ravi_db location '/sparksql/databases/ravi_db';

-- COMMAND ----------

-- MAGIC %fs ls /sparksql/databases/ravi_db/emp

-- COMMAND ----------

create table ravi_db.emp(id int,name string);

-- COMMAND ----------

describe database ravi_db

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/batch28.db/locations

-- COMMAND ----------

CREATE DATABASE sparksql;

-- COMMAND ----------

DROP DATABASE IF EXISTS batch28;
CREATE DATABASE IF NOT EXISTS batch28;

-- COMMAND ----------

create table if not exists batch28.customers(id int,name string,loc string);
create table if not exists batch28.locations(id int,name string);


-- COMMAND ----------

select * from batch28.locations

-- database,table name,colum name and data type -- metadata 
--- data will inside table as rows/records 

-- COMMAND ----------

insert into batch28.locations
select 1,'Bangalore'
union all
select 2,'Chennai'
union all
select 3,'Hyderabad'

-- COMMAND ----------

-- drop database batch28 CASCADE;
-- how to remove / drop non-empty databases
-- using CASCADE we can achieve this

-- COMMAND ----------

CREATE TABLE spark_catalog.batch28.locations (
  id INT,
  name STRING)
USING delta
TBLPROPERTIES (
  'Type' = 'MANAGED',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

select 1 as id,'ravi' as name 

-- COMMAND ----------

CREATE TABLE spark_catalog.batch28.customers (
  id INT,
  name STRING,
  loc STRING)
USING delta
TBLPROPERTIES (
  'Type' = 'MANAGED',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

show create table batch28.locations

-- COMMAND ----------

describe database batch28

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.warehouse.dir")
-- MAGIC #spark.conf.set("spark.sql.warehouse.dir","/project_name/databases/")

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/batch28.db/locations

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC /* show tables
-- MAGIC  show databases
-- MAGIC  */
-- MAGIC show databases

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/")

-- COMMAND ----------

CREATE DATABASE CUSTOMERS_DB location '/tmp/databases/customerdb' 
comment 'THIS Database we can use for storing customer information.';

-- COMMAND ----------

describe database customers_db;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/databases/customerdb/

-- COMMAND ----------

show databases;

-- COMMAND ----------

create table CUSTOMERS_DB.cust_dim(id int, name string) USING delta

-- COMMAND ----------

-- show databases
-- use database_name
-- show tables
--show create table cust_dim

-- COMMAND ----------

DROP database CUSTOMERS_DB CASCADE

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/databases/customerdb/cust_dim

-- COMMAND ----------

create database IF NOT EXISTS finance_external_db comment 'this database we will be using for storing finance data' LOCATION "/tmp/finance_db/"

-- COMMAND ----------

-- MAGIC %fs ls /tmp/finance_db/

-- COMMAND ----------

create table finance_external_db.amount_fact (fact_key string,id int,amount long)

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/finance_db.db

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database customer_db cascade;

-- COMMAND ----------

-- Create database `customer_db`. This throws exception if database with name customer_db
-- already exists.
CREATE DATABASE IF NOT EXISTS customer_db;

-- COMMAND ----------

-- Create database `customer_db` only if database with same name doesn't exist.
CREATE DATABASE IF NOT EXISTS customer_db;

-- COMMAND ----------

-- Create database `customer_db` only if database with same name doesn't exist with
-- `Comments`,`Specific Location` and `Database properties`.
CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
    WITH DBPROPERTIES (ID=001, Name='ravi');

-- COMMAND ----------

-- Verify that properties are set.
DESCRIBE DATABASE EXTENDED customer_db; 

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/

-- COMMAND ----------

-- Create employees DATABASE
CREATE DATABASE  IF NOT EXISTS employees COMMENT 'For software companies' LOCATION "/FileStore/tables/db";

-- COMMAND ----------

-- Describe employees DATABASE.
-- Returns Database Name, Description and Root location of the filesystem
-- for the employees DATABASE.
DESCRIBE DATABASE employees; 

-- COMMAND ----------

-- Create employees DATABASE
CREATE DATABASE IF NOT EXISTS employees COMMENT 'For software companies';

-- COMMAND ----------


-- Alter employees database to set DBPROPERTIES
ALTER DATABASE employees SET DBPROPERTIES ('Create-by' = 'raj', 'Create-date' = '01/01/2021');

-- COMMAND ----------


-- Describe employees DATABASE with EXTENDED option to return additional database properties
DESCRIBE DATABASE EXTENDED employees; 


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS EXTERNAL_DB LOCATION '/tmp/database'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS EXTERNAL_DB.test(id int);
insert into EXTERNAL_DB.test values (1),(2),(3);

-- COMMAND ----------

select * from EXTERNAL_DB.test

-- COMMAND ----------

-- MAGIC %fs ls /tmp/database/test/

-- COMMAND ----------

DESCRIBE HISTORY EXTERNAL_DB.test

-- COMMAND ----------

SHOW CREATE TABLE EXTERNAL_DB.test

-- COMMAND ----------

DROP DATABASE EXTERNAL_DB CASCADE;

-- COMMAND ----------

-- MAGIC %fs ls /tmp/database/

-- COMMAND ----------


-- Create deployment SCHEMA
CREATE SCHEMA deployment COMMENT 'Deployment environment';

-- COMMAND ----------

-- Describe deployment, the DATABASE and SCHEMA are interchangeable, your meaning are the same.
DESC DATABASE deployment; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DROP DATABASE
-- MAGIC * Drops a database and deletes the directory associated with the database from the file system. An exception is thrown if the database does not exist in the system.
-- MAGIC * `Syntax`
-- MAGIC * __`DROP { DATABASE | SCHEMA } [ IF EXISTS ] dbname [ RESTRICT | CASCADE ]`__
-- MAGIC ##### Parameters
-- MAGIC * __`DATABASE | SCHEMA`__
-- MAGIC
-- MAGIC * `DATABASE` and `SCHEMA` mean the same thing, either of them can be used.
-- MAGIC
-- MAGIC * __`IF EXISTS`__  If specified, no exception is thrown when the database does not exist.
-- MAGIC
-- MAGIC * __`RESTRICT`__  If specified, will restrict dropping a non-empty database and is enabled by default.
-- MAGIC
-- MAGIC * __`CASCADE`__  If specified, will drop all the associated tables and functions.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS inventory_db;

-- COMMAND ----------

-- Drop the database and it's tables
DROP DATABASE inventory_db CASCADE;

-- COMMAND ----------

-- MAGIC %fs rm -r /learingwithravi

-- COMMAND ----------

-- MAGIC %fs cp "/learingwithravi/backup/dept.csv" "/learingwithravi/dept.csv" 

-- COMMAND ----------

-- Drop the database using IF EXISTS
DROP DATABASE IF EXISTS inventory_db CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.put("/learingwithravi/dept.csv","""
-- MAGIC Deptno,Dname,Loc
-- MAGIC 10,ACCOUNTING,NEW YORK
-- MAGIC 20,RESEARCH,DALLAS
-- MAGIC 30,SALES,CHICAGO
-- MAGIC 40,OPERATIONS,BOSTON
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/

-- COMMAND ----------

-- MAGIC %fs ls 	dbfs:/user/hive/warehouse/sparksql_db.db/customers/

-- COMMAND ----------


select * from delta.`/user/hive/warehouse/sparksql_db.db/customers`

-- COMMAND ----------

