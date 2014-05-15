drop database if exists site1_TestDB;
create database site1_TestDB;
use site1_TestDB;

create table t1 (
	a int,
	b int,
	c int,
	d tinyint unsigned
);

insert into t1
values
(1, 1, 2, 3),
(1, 2, 2, 3),
(3, 2, 2, 3),
(3, 3, 2, 3);

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
