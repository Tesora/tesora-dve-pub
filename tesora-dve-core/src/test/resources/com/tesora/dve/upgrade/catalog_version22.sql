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
create table auto_incr (incr_id integer not null auto_increment, nextId bigint not null, table_id integer, scope_id integer, primary key (incr_id)) ENGINE=InnoDB;
create table character_sets (id integer(11) not null auto_increment, character_set_name varchar(32) default '' not null, default_collate_name varchar(32) default '' not null, description varchar(60) default '' not null, maxlen bigint(3) default '0' not null, pe_character_set_name varchar(32) default '' not null, primary key (id)) ENGINE=InnoDB;
create table config (config_id integer not null auto_increment, name varchar(255), value varchar(255), primary key (config_id)) ENGINE=InnoDB;
create table container (container_id integer not null auto_increment, name varchar(255), base_table_id integer, distribution_model_id integer not null, range_id integer, storage_group_id integer not null, primary key (container_id)) ENGINE=InnoDB;
create table container_tenant (ctid integer not null auto_increment, discriminant longtext not null, container_id integer not null, primary key (ctid)) ENGINE=InnoDB;
create table distribution_model (name varchar(31) not null, id integer not null auto_increment, primary key (id)) ENGINE=InnoDB;
create table distribution_range (range_id integer not null auto_increment, name varchar(255) not null, signature varchar(255), persistent_group_id integer not null, primary key (range_id)) ENGINE=InnoDB;
create table dynamic_policy (policy_id integer not null auto_increment, aggregate_class varchar(255), aggregate_count integer, aggregate_provider varchar(255), large_class varchar(255), large_count integer, large_provider varchar(255), medium_class varchar(255), medium_count integer, medium_provider varchar(255), name varchar(255) not null, small_class varchar(255), small_count integer, small_provider varchar(255), strict bit not null, primary key (policy_id)) ENGINE=InnoDB;
create table engines (id integer(11) not null auto_increment, comment varchar(80) default '' not null, engine varchar(64) default '' not null, savepoints varchar(3), support varchar(8) default '' not null, transactions varchar(3), xa varchar(3), primary key (id)) ENGINE=InnoDB;
create table external_service (id integer not null auto_increment, auto_start bit not null, config longtext, connect_user varchar(255), name varchar(255) not null unique, plugin varchar(255) not null, uses_datastore bit not null, primary key (id)) ENGINE=InnoDB;
create table foreign_key (fk_id integer not null auto_increment, name varchar(255) not null, user_table_id integer not null, primary key (fk_id)) ENGINE=InnoDB;
create table generation_key_range (key_gen_id integer not null auto_increment, range_end longtext not null, range_start longtext not null, version integer not null, range_id integer not null, generation_id integer not null, primary key (key_gen_id)) ENGINE=InnoDB;
create table generation_sites (generation_id integer not null, site_id integer not null) ENGINE=InnoDB;
create table persistent_group (persistent_group_id integer not null auto_increment, name varchar(255) not null unique, primary key (persistent_group_id)) ENGINE=InnoDB;
create table priviledge (id integer not null auto_increment, user_database_id integer, tenant_id integer, user_id integer, primary key (id)) ENGINE=InnoDB;
create table project (project_id integer not null auto_increment, group_tag varchar(255), name varchar(255), default_policy_id integer, default_persistent_group_id integer, primary key (project_id)) ENGINE=InnoDB;
create table provider (id integer not null auto_increment, config longtext, enabled bit not null, name varchar(255) not null unique, plugin varchar(255) not null, primary key (id)) ENGINE=InnoDB;
create table range_table_relation (relationship_id integer not null auto_increment, range_id integer not null, table_id integer not null, primary key (relationship_id), unique (table_id)) ENGINE=InnoDB;
create table scope (scope_id integer not null auto_increment, local_name varchar(255) not null, scope_future_table_id integer, scope_tenant_id integer, scope_table_id integer, primary key (scope_id), unique (scope_tenant_id, local_name)) ENGINE=InnoDB;
create table server (server_id integer not null auto_increment, ipAddress varchar(255), name varchar(255), primary key (server_id)) ENGINE=InnoDB;
create table shape (shape_id integer not null auto_increment, name varchar(255) not null, definition longtext not null, typehash varchar(40) not null, database_id integer not null, primary key (shape_id)) ENGINE=InnoDB;
create table site_instance (id integer not null auto_increment, instance_url varchar(255) not null, is_master integer not null, name varchar(255) not null unique, status varchar(255) not null, storage_site_id integer, primary key (id)) ENGINE=InnoDB;
create table statistics_log (id integer not null auto_increment, name varchar(255), opClass varchar(255), opCount integer not null, responseTime float not null, timestamp datetime, type varchar(255), primary key (id)) ENGINE=InnoDB;
create table storage_generation (generation_id integer not null auto_increment, locked bit not null, version integer not null, persistent_group_id integer not null, primary key (generation_id)) ENGINE=InnoDB;
create table storage_site (id integer not null auto_increment, haType varchar(255), name varchar(255), primary key (id), unique (name)) ENGINE=InnoDB;
create table table_fk (id integer not null auto_increment, fk_id integer not null, source_table_id integer not null, target_table_id integer not null, primary key (id)) ENGINE=InnoDB;
create table template (template_id integer not null auto_increment, definition longtext not null, name varchar(255) not null, primary key (template_id)) ENGINE=InnoDB;
create table tenant (tenant_id integer not null auto_increment, description varchar(255), ext_tenant_id varchar(255) not null unique, suspended bit, user_database_id integer not null, primary key (tenant_id)) ENGINE=InnoDB;
create table txn_record (xid varchar(255) not null, created_date datetime not null, host varchar(255) not null, is_committed integer not null, primary key (xid)) ENGINE=InnoDB;
create table user (id integer not null auto_increment, accessSpec varchar(255), admin_user bit, name varchar(255), plaintext varchar(255), primary key (id)) ENGINE=InnoDB;
create table user_column (user_column_id integer not null auto_increment, auto_generated bit not null, cdv integer, charset varchar(255), collation varchar(255), comment varchar(255), data_type integer not null, default_value longtext, default_value_is_constant integer not null, has_default_value bit not null, hash_position integer, name varchar(255) not null, native_type_modifiers varchar(255), native_type_name longtext not null, nullable bit not null, on_update integer not null, order_in_table integer not null, prec integer not null, scale integer not null, size integer not null, user_table_id integer, primary key (user_column_id)) ENGINE=InnoDB;
create table user_database (user_database_id integer not null auto_increment, default_character_set_name varchar(255), default_collation_name varchar(255), fk_mode varchar(255) not null, multitenant_mode varchar(255) not null, name varchar(255), templatereqd bit, template varchar(255), default_group_id integer, primary key (user_database_id)) ENGINE=InnoDB;
create table user_key (key_id integer not null auto_increment, card bigint, key_comment varchar(255), constraint_type varchar(255), constraint_name varchar(255), fk_delete_action varchar(255), fk_update_action varchar(255), name varchar(255) not null, persisted integer not null, position integer not null, forward_schema_name varchar(255), forward_table_name varchar(255), synth integer not null, index_type varchar(255) not null, referenced_table integer, user_table_id integer not null, primary key (key_id)) ENGINE=InnoDB;
create table user_key_column (key_column_id integer not null auto_increment, length integer, position integer not null, forward_column_name varchar(255), src_column_id integer not null, key_id integer not null, targ_column_id integer, primary key (key_column_id)) ENGINE=InnoDB;
create table user_table (table_id integer not null auto_increment, collation varchar(255), comment varchar(2048), create_table_stmt longtext, engine varchar(255) not null, name varchar(255) not null, refs integer, row_format varchar(255), state varchar(255), container_id integer, distribution_model_id integer not null, privtab_ten_id integer, persistent_group_id integer not null, shape_id integer, user_database_id integer not null, primary key (table_id)) ENGINE=InnoDB;
alter table auto_incr add index FK630074E440DE575B (scope_id), add constraint FK630074E440DE575B foreign key (scope_id) references scope (scope_id);
alter table auto_incr add index FK630074E44B90C0A4 (table_id), add constraint FK630074E44B90C0A4 foreign key (table_id) references user_table (table_id);
alter table container add index FKE7814C81A7119A06 (distribution_model_id), add constraint FKE7814C81A7119A06 foreign key (distribution_model_id) references distribution_model (id);
alter table container add index FKE7814C81CD240652 (base_table_id), add constraint FKE7814C81CD240652 foreign key (base_table_id) references user_table (table_id);
alter table container add index FKE7814C813548D159 (range_id), add constraint FKE7814C813548D159 foreign key (range_id) references distribution_range (range_id);
alter table container add index FKE7814C81FA36213C (storage_group_id), add constraint FKE7814C81FA36213C foreign key (storage_group_id) references persistent_group (persistent_group_id);
alter table container_tenant add index FK744DB628D47F54CF (container_id), add constraint FK744DB628D47F54CF foreign key (container_id) references container (container_id);
alter table distribution_range add index FK411CF7C27758B4A0 (persistent_group_id), add constraint FK411CF7C27758B4A0 foreign key (persistent_group_id) references persistent_group (persistent_group_id);
alter table foreign_key add index FKA9B3C4744E941A98 (user_table_id), add constraint FKA9B3C4744E941A98 foreign key (user_table_id) references user_table (table_id);
alter table generation_key_range add index FKE0BEDCF63548D159 (range_id), add constraint FKE0BEDCF63548D159 foreign key (range_id) references distribution_range (range_id);
alter table generation_key_range add index FKE0BEDCF65ED51A09 (generation_id), add constraint FKE0BEDCF65ED51A09 foreign key (generation_id) references storage_generation (generation_id);
alter table generation_sites add index FK238DE4255ED51A09 (generation_id), add constraint FK238DE4255ED51A09 foreign key (generation_id) references storage_generation (generation_id);
alter table generation_sites add index FK238DE4251DC461FC (site_id), add constraint FK238DE4251DC461FC foreign key (site_id) references storage_site (id);
alter table priviledge add index FK9D63D4CFF229CB7C (user_database_id), add constraint FK9D63D4CFF229CB7C foreign key (user_database_id) references user_database (user_database_id);
alter table priviledge add index FK9D63D4CFD1F1FE05 (user_id), add constraint FK9D63D4CFD1F1FE05 foreign key (user_id) references user (id);
alter table priviledge add index FK9D63D4CF984D7EE5 (tenant_id), add constraint FK9D63D4CF984D7EE5 foreign key (tenant_id) references tenant (tenant_id);
alter table project add index FKED904B19110A6DA2 (default_persistent_group_id), add constraint FKED904B19110A6DA2 foreign key (default_persistent_group_id) references persistent_group (persistent_group_id);
alter table project add index FKED904B199FF5FD50 (default_policy_id), add constraint FKED904B199FF5FD50 foreign key (default_policy_id) references dynamic_policy (policy_id);
alter table range_table_relation add index FK15954A8F3548D159 (range_id), add constraint FK15954A8F3548D159 foreign key (range_id) references distribution_range (range_id);
alter table range_table_relation add index FK15954A8F4B90C0A4 (table_id), add constraint FK15954A8F4B90C0A4 foreign key (table_id) references user_table (table_id);
create index local_name_idx on scope (local_name);
alter table scope add index FK6833E54DF51922F (scope_table_id), add constraint FK6833E54DF51922F foreign key (scope_table_id) references user_table (table_id);
alter table scope add index FK6833E547CA6DEBA (scope_tenant_id), add constraint FK6833E547CA6DEBA foreign key (scope_tenant_id) references tenant (tenant_id);
alter table scope add index FK6833E5490459BB5 (scope_future_table_id), add constraint FK6833E5490459BB5 foreign key (scope_future_table_id) references user_table (table_id);
alter table shape add index FK6854FA14F28D9F0 (database_id), add constraint FK6854FA14F28D9F0 foreign key (database_id) references user_database (user_database_id);
alter table site_instance add index FK4BCF268D6B83A038 (storage_site_id), add constraint FK4BCF268D6B83A038 foreign key (storage_site_id) references storage_site (id);
create index EntryDate on statistics_log (timestamp);
alter table storage_generation add index FK9463F15C7758B4A0 (persistent_group_id), add constraint FK9463F15C7758B4A0 foreign key (persistent_group_id) references persistent_group (persistent_group_id);
alter table table_fk add index FKCAA0FAD65BA06D06 (target_table_id), add constraint FKCAA0FAD65BA06D06 foreign key (target_table_id) references user_column (user_column_id);
alter table table_fk add index FKCAA0FAD6BC668FAB (fk_id), add constraint FKCAA0FAD6BC668FAB foreign key (fk_id) references foreign_key (fk_id);
alter table table_fk add index FKCAA0FAD61FA31BC (source_table_id), add constraint FKCAA0FAD61FA31BC foreign key (source_table_id) references user_column (user_column_id);
alter table tenant add index FKCBB4E8AAF229CB7C (user_database_id), add constraint FKCBB4E8AAF229CB7C foreign key (user_database_id) references user_database (user_database_id);
alter table user_column add index FKDB781A4A4E941A98 (user_table_id), add constraint FKDB781A4A4E941A98 foreign key (user_table_id) references user_table (table_id);
alter table user_database add index FK6DAC6B6F89F56C36 (default_group_id), add constraint FK6DAC6B6F89F56C36 foreign key (default_group_id) references persistent_group (persistent_group_id);
alter table user_key add index FKF022DBEB4E941A98 (user_table_id), add constraint FKF022DBEB4E941A98 foreign key (user_table_id) references user_table (table_id);
alter table user_key add index FKF022DBEB47539000 (referenced_table), add constraint FKF022DBEB47539000 foreign key (referenced_table) references user_table (table_id);
alter table user_key_column add index FK5363C62ACE8F00F5 (src_column_id), add constraint FK5363C62ACE8F00F5 foreign key (src_column_id) references user_column (user_column_id);
alter table user_key_column add index FK5363C62A26850BCF (key_id), add constraint FK5363C62A26850BCF foreign key (key_id) references user_key (key_id);
alter table user_key_column add index FK5363C62AB56AB2F3 (targ_column_id), add constraint FK5363C62AB56AB2F3 foreign key (targ_column_id) references user_column (user_column_id);
alter table user_table add index FK7358465A7758B4A0 (persistent_group_id), add constraint FK7358465A7758B4A0 foreign key (persistent_group_id) references persistent_group (persistent_group_id);
alter table user_table add index FK7358465AA7119A06 (distribution_model_id), add constraint FK7358465AA7119A06 foreign key (distribution_model_id) references distribution_model (id);
alter table user_table add index FK7358465A4F98A34F (shape_id), add constraint FK7358465A4F98A34F foreign key (shape_id) references shape (shape_id);
alter table user_table add index FK7358465AF229CB7C (user_database_id), add constraint FK7358465AF229CB7C foreign key (user_database_id) references user_database (user_database_id);
alter table user_table add index FK7358465AE7F25E6B (privtab_ten_id), add constraint FK7358465AE7F25E6B foreign key (privtab_ten_id) references tenant (tenant_id);
alter table user_table add index FK7358465AD47F54CF (container_id), add constraint FK7358465AD47F54CF foreign key (container_id) references container (container_id);
alter table container_tenant add key `cont_ten_idx` (container_id, discriminant(80));
alter table shape add unique key `unq_shape_idx` (database_id, name, typehash);
create table pe_version (schema_version int not null, code_version varchar(128) not null, state varchar(64) not null);
