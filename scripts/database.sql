 
---data warehouse projects 

use master;

if exists (select 1 from sys.databases where name='datawarehouse')
begin 
alter database datawarehouse set single_USER with rollback immediate
drop database datawarehouse;
end;

---create database
create Database datawarehouse;

use datawarehouse;

---we will create schema starting with the first one 
create schema bronze; 
go
create schema silver; 
go
create schema gold; 
go---seprator 
