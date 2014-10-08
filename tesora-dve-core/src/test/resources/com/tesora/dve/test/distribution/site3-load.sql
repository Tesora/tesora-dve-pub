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
use site3_TestDB;

-- truncate table  Random;
truncate table  RandomGen2;
truncate table  RandomGen3;
-- truncate table  RandomOneSite;
-- truncate table  Broadcast;
truncate table  BroadcastGen2;
truncate table  BroadcastGen3;
-- truncate table  BroadcastOneSite;
-- truncate table  Static;
truncate table  StaticGen2;
truncate table  StaticGen3;
-- truncate table  StaticOneSite;
-- truncate table  `Range`;
truncate table  RangeGen2;
truncate table  RangeGen3;
-- truncate table  RangeOneSite;

insert into RandomGen2 
values
(6, "value6"),
(7, "value7");

insert into BroadcastGen2
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5"),
(6, "value6"),
(7, "value7");

insert into RangeGen2
values
(6, "value6"),
(7, "value7");

insert into RandomGen3 
values
(6, "value6"),
(7, "value7");

insert into BroadcastGen3
values
(1, "value1"),
(2, "value2"),
(3, "value3"),
(4, "value4"),
(5, "value5"),
(6, "value6"),
(7, "value7");
