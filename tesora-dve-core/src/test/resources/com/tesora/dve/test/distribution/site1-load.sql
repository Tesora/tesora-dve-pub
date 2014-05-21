---
-- #%L
-- Tesora Inc.
-- Database Virtualization Engine
-- %%
-- Copyright (C) 2011 - 2014 Tesora Inc.
-- %%
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License, version 3,
-- as published by the Free Software Foundation.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.
-- #L%
---
drop database if exists site1_TestDB;
create database site1_TestDB;
use site1_TestDB;

create table Random (
	id int,
	value varchar(20)
);

insert into Random
values
(1, "value1"),
(2, "value2"),
(5, "value5");

create table Broadcast (
	id int,
	value varchar(20)
);
insert into Broadcast
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5");

create table Static (
	id int,
	value varchar(20)
);
insert into Static
values
(2, "value2"),
(4, "value4");

create table `Range` (
	id int,
	value varchar(20)
);
insert into `Range`
values
(2, "value2"),
(4, "value4");

create table RandomGen2 (
	id int,
	value varchar(20)
);

create table BroadcastGen2 (
	id int,
	value varchar(20)
);
insert into BroadcastGen2
values
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
select * from `Range`;
insert into RandomGen2
select * from Random;
insert into BroadcastGen2
select * from Broadcast;

create table RandomGen3 (
	id int,
	value varchar(20)
);

create table BroadcastGen3 (
	id int,
	value varchar(20)
);
insert into BroadcastGen3
values
(6, "value6"),
(7, "value7");

create table StaticGen3 (
	id int,
	value varchar(20)
);
insert into RandomGen3
select * from Random;
insert into BroadcastGen3
select * from Broadcast;

insert into StaticGen3
values
(6, "value6");

create table `RangeGen3` (
	id int,
	value varchar(20)
);
insert into RangeGen3
select * from `Range`;

insert into RangeGen3
values
(6, "value6");

create table RandomOneSite (
	id int,
	value varchar(20)
);

insert into RandomOneSite
values
(1, "value1"),
(2, "value2"),
(5, "value5");

create table BroadcastOneSite (
	id int,
	value varchar(20)
);
insert into BroadcastOneSite
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5");

create table StaticOneSite (
	id int,
	value varchar(20)
);
insert into StaticOneSite
values
(2, "value2"),
(4, "value4");

create table `RangeOneSite` (
	id int,
	value varchar(20)
);
insert into `RangeOneSite`
values
(2, "value2"),
(4, "value4");
