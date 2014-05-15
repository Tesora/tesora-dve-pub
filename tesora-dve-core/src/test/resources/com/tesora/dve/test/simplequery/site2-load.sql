drop database if exists site2_TestDB2;
drop database if exists site2_TestDB;
drop database if exists site2_MyTestDB;
create database site2_TestDB;
use site2_TestDB;

create table foo (
	id int,
	value varchar(20)
);

insert into foo values (2, "value2");
insert into foo values (4, "value4");

create table bar (
	id int,
	value varchar(20)
);

insert into bar 
values 
(3, "barvalue3"),
(2000, "barvalue");
