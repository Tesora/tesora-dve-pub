drop database if exists site2_TestDB;
create database site2_TestDB;
use site2_TestDB;

create table t1 (
	a int,
	b int,
	c int,
	d tinyint unsigned
);

insert into t1
values
(1, 3, 2, 3),
(3, 1, 2, 3),
(2, 1, 2, 3),
(2, 2, 2, 255),
(2, 3, 2, 3);

create table t2 (
	a int,
	b int,
	p tinyint unsigned,
	q int
);

insert into t2
values
(1, 1, 2, 3),
(1, 2, 2, 3),
(1, 3, 2, 3),
(3, 1, 2, 3),
(3, 2, 2, 3),
(3, 3, 2, 3),
(4, 1, 2, 3),
(4, 2, 2, 3),
(4, 3, 2, 3);
