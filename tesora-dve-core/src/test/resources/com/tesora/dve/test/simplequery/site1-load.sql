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


