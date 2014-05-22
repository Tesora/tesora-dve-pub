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
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_variable (
`name` VARCHAR(128) NOT NULL DEFAULT '', 
`value` LONGBLOB NOT NULL
);
CREATE TABLE d7_actions (
`aid` VARCHAR(255) NOT NULL DEFAULT '0', 
`type` VARCHAR(32) NOT NULL DEFAULT '', 
`callback` VARCHAR(255) NOT NULL DEFAULT '', 
`parameters` LONGBLOB NOT NULL, 
`label` VARCHAR(255) NOT NULL DEFAULT '0'
);
CREATE TABLE d7_batch (
`bid` INT NOT NULL, 
`token` VARCHAR(64) NOT NULL, 
`timestamp` INT NOT NULL, 
`batch` LONGBLOB NULL
);
CREATE TABLE d7_blocked_ips (
`iid` INT NOT NULL auto_increment, 
`ip` VARCHAR(40) NOT NULL DEFAULT ''
);
CREATE TABLE d7_cache (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_bootstrap (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_form (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_page (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_menu (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_path (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_format_type (
`type` VARCHAR(64) NOT NULL, 
`title` VARCHAR(255) NOT NULL, 
`locked` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_formats (
`dfid` INT NOT NULL auto_increment, 
`format` VARCHAR(100) NOT NULL, 
`type` VARCHAR(64) NOT NULL, 
`locked` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_format_locale (
`format` VARCHAR(100) NOT NULL, 
`type` VARCHAR(64) NOT NULL, 
`language` VARCHAR(12) NOT NULL
);
CREATE TABLE d7_file_managed (
`fid` INT NOT NULL auto_increment, 
`uid` INT NOT NULL DEFAULT 0, 
`filename` VARCHAR(255) NOT NULL DEFAULT '', 
`uri` VARCHAR(255) NOT NULL DEFAULT '', 
`filemime` VARCHAR(255) NOT NULL DEFAULT '', 
`filesize` INT NOT NULL DEFAULT 0, 
`status` TINYINT NOT NULL DEFAULT 0, 
`timestamp` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_file_usage (
`fid` INT NOT NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(64) NOT NULL DEFAULT '', 
`id` INT NOT NULL DEFAULT 0, 
`count` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_flood (
`fid` INT NOT NULL auto_increment, 
`event` VARCHAR(64) NOT NULL DEFAULT '', 
`identifier` VARCHAR(128) NOT NULL DEFAULT '', 
`timestamp` INT NOT NULL DEFAULT 0, 
`expiration` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_menu_router (
`path` VARCHAR(255) NOT NULL DEFAULT '', 
`load_functions` BLOB NOT NULL, 
`to_arg_functions` BLOB NOT NULL, 
`access_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`access_arguments` BLOB NULL, 
`page_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`page_arguments` BLOB NULL, 
`delivery_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`fit` INT NOT NULL DEFAULT 0, 
`number_parts` SMALLINT NOT NULL DEFAULT 0, 
`context` INT NOT NULL DEFAULT 0, 
`tab_parent` VARCHAR(255) NOT NULL DEFAULT '', 
`tab_root` VARCHAR(255) NOT NULL DEFAULT '', 
`title` VARCHAR(255) NOT NULL DEFAULT '', 
`title_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`title_arguments` VARCHAR(255) NOT NULL DEFAULT '', 
`theme_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`theme_arguments` VARCHAR(255) NOT NULL DEFAULT '', 
`type` INT NOT NULL DEFAULT 0, 
`description` TEXT NOT NULL, 
`position` VARCHAR(255) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0, 
`include_file` MEDIUMTEXT
);
CREATE TABLE d7_menu_links (
`menu_name` VARCHAR(32) NOT NULL DEFAULT '', 
`mlid` INT NOT NULL auto_increment, 
`plid` INT NOT NULL DEFAULT 0, 
`link_path` VARCHAR(255) NOT NULL DEFAULT '', 
`router_path` VARCHAR(255) NOT NULL DEFAULT '', 
`link_title` VARCHAR(255) NOT NULL DEFAULT '', 
`options` BLOB NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT 'system', 
`hidden` SMALLINT NOT NULL DEFAULT 0, 
`external` SMALLINT NOT NULL DEFAULT 0, 
`has_children` SMALLINT NOT NULL DEFAULT 0, 
`expanded` SMALLINT NOT NULL DEFAULT 0, 
`weight` INT NOT NULL DEFAULT 0, 
`depth` SMALLINT NOT NULL DEFAULT 0, 
`customized` SMALLINT NOT NULL DEFAULT 0, 
`p1` INT NOT NULL DEFAULT 0, 
`p2` INT NOT NULL DEFAULT 0, 
`p3` INT NOT NULL DEFAULT 0, 
`p4` INT NOT NULL DEFAULT 0, 
`p5` INT NOT NULL DEFAULT 0, 
`p6` INT NOT NULL DEFAULT 0, 
`p7` INT NOT NULL DEFAULT 0, 
`p8` INT NOT NULL DEFAULT 0, 
`p9` INT NOT NULL DEFAULT 0, 
`updated` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_queue (
`item_id` INT NOT NULL auto_increment, 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_registry (
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(9) NOT NULL DEFAULT '', 
`filename` VARCHAR(255) NOT NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_registry_file (
`filename` VARCHAR(255) NOT NULL, 
`hash` VARCHAR(64) NOT NULL
);
CREATE TABLE d7_semaphore (
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`value` VARCHAR(255) NOT NULL DEFAULT '', 
`expire` DOUBLE NOT NULL
);
CREATE TABLE d7_sequences (
`value` INT NOT NULL auto_increment
);
CREATE TABLE d7_sessions (
`uid` INT NOT NULL, 
`sid` VARCHAR(128) NOT NULL, 
`ssid` VARCHAR(128) NOT NULL DEFAULT '', 
`hostname` VARCHAR(128) NOT NULL DEFAULT '', 
`timestamp` INT NOT NULL DEFAULT 0, 
`cache` INT NOT NULL DEFAULT 0, 
`session` LONGBLOB NULL
);
CREATE TABLE d7_system (
`filename` VARCHAR(255) NOT NULL DEFAULT '', 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(12) NOT NULL DEFAULT '', 
`owner` VARCHAR(255) NOT NULL DEFAULT '', 
`status` INT NOT NULL DEFAULT 0, 
`bootstrap` INT NOT NULL DEFAULT 0, 
`schema_version` SMALLINT NOT NULL DEFAULT -1, 
`weight` INT NOT NULL DEFAULT 0, 
`info` BLOB NULL
);
CREATE TABLE d7_url_alias (
`pid` INT NOT NULL auto_increment, 
`source` VARCHAR(255) NOT NULL DEFAULT '', 
`alias` VARCHAR(255) NOT NULL DEFAULT '', 
`language` VARCHAR(12) NOT NULL DEFAULT ''
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_variable (
`name` VARCHAR(128) NOT NULL DEFAULT '', 
`value` LONGBLOB NOT NULL
);
CREATE TABLE d7_actions (
`aid` VARCHAR(255) NOT NULL DEFAULT '0', 
`type` VARCHAR(32) NOT NULL DEFAULT '', 
`callback` VARCHAR(255) NOT NULL DEFAULT '', 
`parameters` LONGBLOB NOT NULL, 
`label` VARCHAR(255) NOT NULL DEFAULT '0'
);
CREATE TABLE d7_batch (
`bid` INT NOT NULL, 
`token` VARCHAR(64) NOT NULL, 
`timestamp` INT NOT NULL, 
`batch` LONGBLOB NULL
);
CREATE TABLE d7_blocked_ips (
`iid` INT NOT NULL auto_increment, 
`ip` VARCHAR(40) NOT NULL DEFAULT ''
);
CREATE TABLE d7_cache (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_bootstrap (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_form (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_page (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_menu (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_path (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_format_type (
`type` VARCHAR(64) NOT NULL, 
`title` VARCHAR(255) NOT NULL, 
`locked` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_formats (
`dfid` INT NOT NULL auto_increment, 
`format` VARCHAR(100) NOT NULL, 
`type` VARCHAR(64) NOT NULL, 
`locked` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_date_format_locale (
`format` VARCHAR(100) NOT NULL, 
`type` VARCHAR(64) NOT NULL, 
`language` VARCHAR(12) NOT NULL
);
CREATE TABLE d7_file_managed (
`fid` INT NOT NULL auto_increment, 
`uid` INT NOT NULL DEFAULT 0, 
`filename` VARCHAR(255) NOT NULL DEFAULT '', 
`uri` VARCHAR(255) NOT NULL DEFAULT '', 
`filemime` VARCHAR(255) NOT NULL DEFAULT '', 
`filesize` INT NOT NULL DEFAULT 0, 
`status` TINYINT NOT NULL DEFAULT 0, 
`timestamp` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_file_usage (
`fid` INT NOT NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(64) NOT NULL DEFAULT '', 
`id` INT NOT NULL DEFAULT 0, 
`count` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_flood (
`fid` INT NOT NULL auto_increment, 
`event` VARCHAR(64) NOT NULL DEFAULT '', 
`identifier` VARCHAR(128) NOT NULL DEFAULT '', 
`timestamp` INT NOT NULL DEFAULT 0, 
`expiration` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_menu_router (
`path` VARCHAR(255) NOT NULL DEFAULT '', 
`load_functions` BLOB NOT NULL, 
`to_arg_functions` BLOB NOT NULL, 
`access_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`access_arguments` BLOB NULL, 
`page_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`page_arguments` BLOB NULL, 
`delivery_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`fit` INT NOT NULL DEFAULT 0, 
`number_parts` SMALLINT NOT NULL DEFAULT 0, 
`context` INT NOT NULL DEFAULT 0, 
`tab_parent` VARCHAR(255) NOT NULL DEFAULT '', 
`tab_root` VARCHAR(255) NOT NULL DEFAULT '', 
`title` VARCHAR(255) NOT NULL DEFAULT '', 
`title_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`title_arguments` VARCHAR(255) NOT NULL DEFAULT '', 
`theme_callback` VARCHAR(255) NOT NULL DEFAULT '', 
`theme_arguments` VARCHAR(255) NOT NULL DEFAULT '', 
`type` INT NOT NULL DEFAULT 0, 
`description` TEXT NOT NULL, 
`position` VARCHAR(255) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0, 
`include_file` MEDIUMTEXT
);
CREATE TABLE d7_menu_links (
`menu_name` VARCHAR(32) NOT NULL DEFAULT '', 
`mlid` INT NOT NULL auto_increment, 
`plid` INT NOT NULL DEFAULT 0, 
`link_path` VARCHAR(255) NOT NULL DEFAULT '', 
`router_path` VARCHAR(255) NOT NULL DEFAULT '', 
`link_title` VARCHAR(255) NOT NULL DEFAULT '', 
`options` BLOB NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT 'system', 
`hidden` SMALLINT NOT NULL DEFAULT 0, 
`external` SMALLINT NOT NULL DEFAULT 0, 
`has_children` SMALLINT NOT NULL DEFAULT 0, 
`expanded` SMALLINT NOT NULL DEFAULT 0, 
`weight` INT NOT NULL DEFAULT 0, 
`depth` SMALLINT NOT NULL DEFAULT 0, 
`customized` SMALLINT NOT NULL DEFAULT 0, 
`p1` INT NOT NULL DEFAULT 0, 
`p2` INT NOT NULL DEFAULT 0, 
`p3` INT NOT NULL DEFAULT 0, 
`p4` INT NOT NULL DEFAULT 0, 
`p5` INT NOT NULL DEFAULT 0, 
`p6` INT NOT NULL DEFAULT 0, 
`p7` INT NOT NULL DEFAULT 0, 
`p8` INT NOT NULL DEFAULT 0, 
`p9` INT NOT NULL DEFAULT 0, 
`updated` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_queue (
`item_id` INT NOT NULL auto_increment, 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_registry (
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(9) NOT NULL DEFAULT '', 
`filename` VARCHAR(255) NOT NULL, 
`module` VARCHAR(255) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_registry_file (
`filename` VARCHAR(255) NOT NULL, 
`hash` VARCHAR(64) NOT NULL
);
CREATE TABLE d7_semaphore (
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`value` VARCHAR(255) NOT NULL DEFAULT '', 
`expire` DOUBLE NOT NULL
);
CREATE TABLE d7_sequences (
`value` INT NOT NULL auto_increment
);
CREATE TABLE d7_sessions (
`uid` INT NOT NULL, 
`sid` VARCHAR(128) NOT NULL, 
`ssid` VARCHAR(128) NOT NULL DEFAULT '', 
`hostname` VARCHAR(128) NOT NULL DEFAULT '', 
`timestamp` INT NOT NULL DEFAULT 0, 
`cache` INT NOT NULL DEFAULT 0, 
`session` LONGBLOB NULL
);
CREATE TABLE d7_system (
`filename` VARCHAR(255) NOT NULL DEFAULT '', 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`type` VARCHAR(12) NOT NULL DEFAULT '', 
`owner` VARCHAR(255) NOT NULL DEFAULT '', 
`status` INT NOT NULL DEFAULT 0, 
`bootstrap` INT NOT NULL DEFAULT 0, 
`schema_version` SMALLINT NOT NULL DEFAULT -1, 
`weight` INT NOT NULL DEFAULT 0, 
`info` BLOB NULL
);
CREATE TABLE d7_url_alias (
`pid` INT NOT NULL auto_increment, 
`source` VARCHAR(255) NOT NULL DEFAULT '', 
`alias` VARCHAR(255) NOT NULL DEFAULT '', 
`language` VARCHAR(12) NOT NULL DEFAULT ''
);
CREATE TABLE d7_authmap (
`aid` INT NOT NULL auto_increment, 
`uid` INT NOT NULL DEFAULT 0, 
`authname` VARCHAR(128) NOT NULL DEFAULT '', 
`module` VARCHAR(128) NOT NULL DEFAULT ''
);
CREATE TABLE d7_role_permission (
`rid` INT NOT NULL, 
`permission` VARCHAR(128) NOT NULL DEFAULT '', 
`module` VARCHAR(255) NOT NULL DEFAULT ''
);
CREATE TABLE d7_role (
`rid` INT NOT NULL auto_increment, 
`name` VARCHAR(64) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_users (
`uid` INT NOT NULL DEFAULT 0, 
`name` VARCHAR(60) NOT NULL DEFAULT '', 
`pass` VARCHAR(128) NOT NULL DEFAULT '', 
`mail` VARCHAR(254) NULL DEFAULT '', 
`theme` VARCHAR(255) NOT NULL DEFAULT '', 
`signature` VARCHAR(255) NOT NULL DEFAULT '', 
`signature_format` VARCHAR(255) NULL, 
`created` INT NOT NULL DEFAULT 0, 
`access` INT NOT NULL DEFAULT 0, 
`login` INT NOT NULL DEFAULT 0, 
`status` TINYINT NOT NULL DEFAULT 0, 
`timezone` VARCHAR(32) NULL, 
`language` VARCHAR(12) NOT NULL DEFAULT '', 
`picture` INT NOT NULL DEFAULT 0, 
`init` VARCHAR(254) NULL DEFAULT '', 
`data` LONGBLOB NULL
);
CREATE TABLE d7_users_roles (
`uid` INT NOT NULL DEFAULT 0, 
`rid` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_filter (
`format` VARCHAR(255) NOT NULL, 
`module` VARCHAR(64) NOT NULL DEFAULT '', 
`name` VARCHAR(32) NOT NULL DEFAULT '', 
`weight` INT NOT NULL DEFAULT 0, 
`status` INT NOT NULL DEFAULT 0, 
`settings` LONGBLOB NULL
);
CREATE TABLE d7_filter_format (
`format` VARCHAR(255) NOT NULL, 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`cache` TINYINT NOT NULL DEFAULT 0, 
`status` TINYINT NOT NULL DEFAULT 1, 
`weight` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_filter (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_node (
`nid` INT NOT NULL auto_increment, 
`vid` INT NOT NULL DEFAULT 0, 
`type` VARCHAR(32) NOT NULL DEFAULT '', 
`language` VARCHAR(12) NOT NULL DEFAULT '', 
`title` VARCHAR(255) NOT NULL DEFAULT '', 
`uid` INT NOT NULL DEFAULT 0, 
`status` INT NOT NULL DEFAULT 1, 
`created` INT NOT NULL DEFAULT 0, 
`changed` INT NOT NULL DEFAULT 0, 
`comment` INT NOT NULL DEFAULT 0, 
`promote` INT NOT NULL DEFAULT 0, 
`sticky` INT NOT NULL DEFAULT 0, 
`tnid` INT NOT NULL DEFAULT 0, 
`translate` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_node_access (
`nid` INT NOT NULL DEFAULT 0, 
`gid` INT NOT NULL DEFAULT 0, 
`realm` VARCHAR(255) NOT NULL DEFAULT '', 
`grant_view` TINYINT NOT NULL DEFAULT 0, 
`grant_update` TINYINT NOT NULL DEFAULT 0, 
`grant_delete` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_node_revision (
`nid` INT NOT NULL DEFAULT 0, 
`vid` INT NOT NULL auto_increment, 
`uid` INT NOT NULL DEFAULT 0, 
`title` VARCHAR(255) NOT NULL DEFAULT '', 
`log` LONGTEXT NOT NULL, 
`timestamp` INT NOT NULL DEFAULT 0, 
`status` INT NOT NULL DEFAULT 1, 
`comment` INT NOT NULL DEFAULT 0, 
`promote` INT NOT NULL DEFAULT 0, 
`sticky` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_node_type (
`type` VARCHAR(32) NOT NULL, 
`name` VARCHAR(255) NOT NULL DEFAULT '', 
`base` VARCHAR(255) NOT NULL, 
`module` VARCHAR(255) NOT NULL, 
`description` MEDIUMTEXT NOT NULL, 
`help` MEDIUMTEXT NOT NULL, 
`has_title` TINYINT NOT NULL, 
`title_label` VARCHAR(255) NOT NULL DEFAULT '', 
`custom` TINYINT NOT NULL DEFAULT 0, 
`modified` TINYINT NOT NULL DEFAULT 0, 
`locked` TINYINT NOT NULL DEFAULT 0, 
`disabled` TINYINT NOT NULL DEFAULT 0, 
`orig_type` VARCHAR(255) NOT NULL DEFAULT ''
);
CREATE TABLE d7_block_node_type (
`module` VARCHAR(64) NOT NULL, 
`delta` VARCHAR(32) NOT NULL, 
`type` VARCHAR(32) NOT NULL
);
CREATE TABLE d7_history (
`uid` INT NOT NULL DEFAULT 0, 
`nid` INT NOT NULL DEFAULT 0, 
`timestamp` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_field_config (
`id` INT NOT NULL auto_increment, 
`field_name` VARCHAR(32) NOT NULL, 
`type` VARCHAR(128) NOT NULL, 
`module` VARCHAR(128) NOT NULL DEFAULT '', 
`active` TINYINT NOT NULL DEFAULT 0, 
`storage_type` VARCHAR(128) NOT NULL, 
`storage_module` VARCHAR(128) NOT NULL DEFAULT '', 
`storage_active` TINYINT NOT NULL DEFAULT 0, 
`locked` TINYINT NOT NULL DEFAULT 0, 
`data` LONGBLOB NOT NULL, 
`cardinality` TINYINT NOT NULL DEFAULT 0, 
`translatable` TINYINT NOT NULL DEFAULT 0, 
`deleted` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_field_config_instance (
`id` INT NOT NULL auto_increment, 
`field_id` INT NOT NULL, 
`field_name` VARCHAR(32) NOT NULL DEFAULT '', 
`entity_type` VARCHAR(32) NOT NULL DEFAULT '', 
`bundle` VARCHAR(128) NOT NULL DEFAULT '', 
`data` LONGBLOB NOT NULL, 
`deleted` TINYINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_cache_field (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_block (
`bid` INT NOT NULL auto_increment, 
`module` VARCHAR(64) NOT NULL DEFAULT '', 
`delta` VARCHAR(32) NOT NULL DEFAULT '0', 
`theme` VARCHAR(64) NOT NULL DEFAULT '', 
`status` TINYINT NOT NULL DEFAULT 0, 
`weight` INT NOT NULL DEFAULT 0, 
`region` VARCHAR(64) NOT NULL DEFAULT '', 
`custom` TINYINT NOT NULL DEFAULT 0, 
`visibility` TINYINT NOT NULL DEFAULT 0, 
`pages` TEXT NOT NULL, 
`title` VARCHAR(64) NOT NULL DEFAULT '', 
`cache` TINYINT NOT NULL DEFAULT 1
);
CREATE TABLE d7_block_role (
`module` VARCHAR(64) NOT NULL, 
`delta` VARCHAR(32) NOT NULL, 
`rid` INT NOT NULL
);
CREATE TABLE d7_block_custom (
`bid` INT NOT NULL auto_increment, 
`body` LONGTEXT NULL, 
`info` VARCHAR(128) NOT NULL DEFAULT '', 
`format` VARCHAR(255) NULL
);
CREATE TABLE d7_cache_block (
`cid` VARCHAR(255) NOT NULL DEFAULT '', 
`data` LONGBLOB NULL, 
`expire` INT NOT NULL DEFAULT 0, 
`created` INT NOT NULL DEFAULT 0, 
`serialized` SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_watchdog (
`wid` INT NOT NULL auto_increment, 
`uid` INT NOT NULL DEFAULT 0, 
`type` VARCHAR(64) NOT NULL DEFAULT '', 
`message` LONGTEXT NOT NULL, 
`variables` LONGBLOB NOT NULL, 
`severity` TINYINT NOT NULL DEFAULT 0, 
`link` VARCHAR(255) NULL DEFAULT '', 
`location` TEXT NOT NULL, 
`referer` TEXT NULL, 
`hostname` VARCHAR(128) NOT NULL DEFAULT '', 
`timestamp` INT NOT NULL DEFAULT 0
);
CREATE TABLE d7_drupal_install_test (id int NULL);
CREATE TABLE d7_drupal_install_test (id int NULL);
