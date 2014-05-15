drop database if exists site3_1_TestDB;
create database site3_1_TestDB;
use site3_1_TestDB;

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

drop database if exists site3_2_TestDB;
create database site3_2_TestDB;
use site3_2_TestDB;

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


