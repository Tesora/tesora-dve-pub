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
drop database if exists site1_d7test;
drop database if exists site1_TestDB;
create database site1_TestDB;
use site1_TestDB;

create table alltypes ( bit_column bit,
						tinyint_column tinyint,
						bigint_column bigint,
						longvarb_column long varbinary,
						varb_column varbinary(200),
						binary_column binary(10),
						text_column text,
						char_column char(10),
						num_column numeric(10,2),
						dec_column decimal(5,4),
						int_column integer,
						smallint_column smallint,
						float_column float,
						double_column double,
						varchar_column varchar(10),
						date_column date,
						time_column time,
						datetime_column datetime,
						intu_column integer unsigned,
						bigintu_column bigint unsigned );

insert into alltypes values (1,1,11111111111111,"bin1","bin1","bin1","text1","char1",1.12,1.1234,1,1,1.111111,1.111111,"varchar1","2011-01-11","11:11:11","2011-01-11 11:11:11",11111,1111111111);

