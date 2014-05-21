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
drop database if exists site2_TestDB;
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
