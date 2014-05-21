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
insert into persistent_group (name) values ("sg2Ref");
insert into storage_generation (persistent_group_id, locked, version)
select sg.persistent_group_id, 1, 0
from persistent_group sg where sg.name = 'sg2Ref';
insert into storage_generation (persistent_group_id, locked, version)
select sg.persistent_group_id, 0, 1
from persistent_group sg where sg.name = 'sg2Ref';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 0 and sg.name = 'sg2Ref' and site.name = 'site1';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 0 and sg.name = 'sg2Ref' and site.name = 'site2';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 1 and sg.name = 'sg2Ref' and site.name = 'site3';

insert into persistent_group (name) values ("sg3Ref");
insert into storage_generation (persistent_group_id, locked, version)
select sg.persistent_group_id, 1, 0
from persistent_group sg where sg.name = 'sg3Ref';
insert into storage_generation (persistent_group_id, locked, version)
select sg.persistent_group_id, 0, 1
from persistent_group sg where sg.name = 'sg3Ref';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 0 and sg.name = 'sg3Ref' and site.name = 'site1';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 0 and sg.name = 'sg3Ref' and site.name = 'site2';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 1 and sg.name = 'sg3Ref' and site.name = 'site1';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 1 and sg.name = 'sg3Ref' and site.name = 'site2';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 1 and sg.name = 'sg3Ref' and site.name = 'site3';

insert into user_table
(name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select dist.name, dist.id, sg.persistent_group_id, 3, "InnoDB", "BASE TABLE"
from distribution_model dist, persistent_group sg
where sg.name = "DefaultGroup";

insert into user_table
(name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select concat(dist.name,"Gen2"), dist.id, sg.persistent_group_id, 3, "InnoDB", "BASE TABLE"
from distribution_model dist, persistent_group sg
where sg.name = "sg2Ref";

insert into user_table
(name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select concat(dist.name,"Gen3"), dist.id, sg.persistent_group_id, 3, "InnoDB", "BASE TABLE"
from distribution_model dist, persistent_group sg
where sg.name = "sg3Ref";


insert into persistent_group (name) values ("sgOneSite");
insert into storage_generation (persistent_group_id, locked, version)
select sg.persistent_group_id, 0, 0
from persistent_group sg where sg.name = 'sgOneSite';
insert into generation_sites (generation_id, site_id)
select gen.generation_id, site.id
from persistent_group sg, storage_generation gen, storage_site site
where gen.persistent_group_id = sg.persistent_group_id and gen.version = 0 and sg.name = 'sgOneSite' and site.name = 'site1';

insert into user_table
(name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select concat(dist.name,"OneSite"), dist.id, sg.persistent_group_id, 3, "InnoDB", "BASE TABLE"
from distribution_model dist, persistent_group sg
where sg.name = "sgOneSite";

insert into user_table
(table_id, name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
values 
(1000, "templ", 1, 1, 3, "InnoDB", "BASE TABLE");

insert into user_column 
(name, data_type, native_type_name, user_table_id, size, hash_position, prec, scale, nullable, default_value_is_constant, has_default_value, auto_generated, default_value, on_update, cdv, order_in_table)
values 
("id",     4, "integer", 1000,  4, 1, 10, 0, 0, 1, 0, 1, null, 0, 0,1),
("value", 12, "varchar", 1000, 20, 0,  0, 0, 1, 1, 0, 0, null, 0, 0,2);

insert into user_column
(name, data_type, native_type_name, user_table_id, size, hash_position, prec, scale, nullable, has_default_value, default_value_is_constant, auto_generated, default_value, on_update, cdv, order_in_table)
select
uc.name, data_type, native_type_name, ut.table_id, size, hash_position, prec, scale, nullable, has_default_value, default_value_is_constant, auto_generated, default_value, on_update, cdv, order_in_table
from user_column uc, user_table ut
where uc.user_table_id = 1000;

insert into distribution_range
(name, signature, persistent_group_id)
select t.name, "int", t.persistent_group_id
from user_table t, distribution_model d
where t.distribution_model_id = d.id and d.name = 'Range';

insert into range_table_relation
(range_id, table_id)
select dr.range_id, t.table_id
from distribution_range dr, user_table t
where dr.name = t.name; 





