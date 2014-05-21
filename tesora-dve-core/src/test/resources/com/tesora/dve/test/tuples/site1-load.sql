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
