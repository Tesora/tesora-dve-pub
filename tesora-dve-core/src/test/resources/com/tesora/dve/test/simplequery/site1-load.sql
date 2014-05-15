drop database if exists site1_TestDB2;
drop database if exists site1_TestDB;
drop database if exists site1_MyTestDB;
create database site1_TestDB;
use site1_TestDB;

create table foo (
	id int,
	value varchar(20)
);

insert into foo values (1, "value1");
insert into foo values (3, "value3");
insert into foo values (5, "value5");

create table bar (
	id int,
	value varchar(20)
);
insert into bar 
values
(2, "value2"),
(2001, "value2001");


