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
insert into user_table
(table_id, name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
values 
(100, "foo", 1, 3, 3, "InnoDB", "BASE TABLE"),
(200, "bar", 1, 3, 3, "InnoDB", "BASE TABLE");

insert into user_column 
(name, data_type, native_type_name, user_table_id, size, hash_position, prec, scale, nullable, has_default_value, default_value_is_constant, auto_generated, default_value, order_in_table, on_update, cdv)
values 
("id",     4, "integer", 100,  4, 1, 10, 0, 1, 0, 1, 1, null, 0, 0, 0),
("value", 12, "varchar", 100, 20, 0, 20, 0, 1, 0, 1, 0, null, 1, 0, 0),

("id",     4, "integer", 200,  4, 1, 10, 0, 1, 0, 1, 1, null, 0, 0, 0),
("value", 12, "varchar", 200, 20, 0, 20, 0, 1, 0, 1, 0, null, 1, 0, 0);
