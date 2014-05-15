drop database if exists site3_TestDB;
create database site3_TestDB;
use site3_TestDB;

create table RandomGen2 (
	id int,
	value varchar(20)
);

insert into RandomGen2 
values
(6, "value6"),
(7, "value7");

create table BroadcastGen2 (
	id int,
	value varchar(20)
);
insert into BroadcastGen2
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5"),
(6, "value6"),
(7, "value7");

create table StaticGen2 (
	id int,
	value varchar(20)
);

create table `RangeGen2` (
	id int,
	value varchar(20)
);
insert into RangeGen2
values
(6, "value6"),
(7, "value7");

create table RandomGen3 (
	id int,
	value varchar(20)
);

insert into RandomGen3 
values
(6, "value6"),
(7, "value7");

create table BroadcastGen3 (
	id int,
	value varchar(20)
);
insert into BroadcastGen3
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5"),
(6, "value6"),
(7, "value7");

create table StaticGen3 (
	id int,
	value varchar(20)
);
create table `RangeGen3` (
	id int,
	value varchar(20)
);
