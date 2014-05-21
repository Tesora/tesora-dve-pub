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
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_system (filename, name, type, owner, info) VALUES ('themes/bartik/bartik.info', 'bartik', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/garland/garland.info', 'garland', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/seven/seven.info', 'seven', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:1:{i:0;s:13:"sidebar_first";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/stark/stark.info', 'stark', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/tests/test_theme/test_theme.info', 'test_theme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/tests/update_test_basetheme/update_test_basetheme.info', 'update_test_basetheme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}}'), ('themes/tests/update_test_subtheme/update_test_subtheme.info', 'update_test_subtheme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}}');
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '13228538164e299701e4fb29.21264293', '1311348482.9359');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_system (filename, name, type, owner, info) VALUES ('themes/bartik/bartik.info', 'bartik', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/garland/garland.info', 'garland', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/seven/seven.info', 'seven', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:1:{i:0;s:13:"sidebar_first";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/stark/stark.info', 'stark', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/tests/test_theme/test_theme.info', 'test_theme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'), ('themes/tests/update_test_basetheme/update_test_basetheme.info', 'update_test_basetheme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:14:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}}'), ('themes/tests/update_test_subtheme/update_test_subtheme.info', 'update_test_subtheme', 'theme', 'themes/engines/phptemplate/phptemplate.engine', 'a:15:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}}');
INSERT INTO d7_variable (name, value) VALUES ('theme_default', 's:6:"bartik";');
INSERT INTO d7_variable (name, value) VALUES ('cron_key', 's:43:"U3H7MCqaV-btmltWUuqQZ-5ZhmSGHW3ktx4fAre1uoc";');
INSERT INTO d7_system (filename, name, type, owner, status, schema_version, bootstrap) VALUES ('modules/system/system.module', 'system', 'module', '', '1', '7071', '0');
INSERT INTO d7_system (filename, name, type, owner, info) VALUES ('modules/update/tests/aaa_update_test.module', 'aaa_update_test', 'module', '', 'a:12:{s:4:"name";s:15:"AAA Update test";s:11:"description";s:41:"Support module for update module testing.";s:7:"package";s:7:"Testing";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/actions_loop_test.module', 'actions_loop_test', 'module', '', 'a:12:{s:4:"name";s:17:"Actions loop test";s:11:"description";s:39:"Support module for action loop testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/aggregator/aggregator.module', 'aggregator', 'module', '', 'a:13:{s:4:"name";s:10:"Aggregator";s:11:"description";s:57:"Aggregates syndicated content (RSS, RDF, and Atom feeds).";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:15:"aggregator.test";}s:9:"configure";s:41:"admin/config/services/aggregator/settings";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:14:"aggregator.css";s:33:"modules/aggregator/aggregator.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/aggregator/tests/aggregator_test.module', 'aggregator_test', 'module', '', 'a:12:{s:4:"name";s:23:"Aggregator module tests";s:11:"description";s:46:"Support module for aggregator related testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/ajax_forms_test.module', 'ajax_forms_test', 'module', '', 'a:12:{s:4:"name";s:26:"AJAX form test mock module";s:11:"description";s:25:"Test for AJAX form calls.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/ajax_test.module', 'ajax_test', 'module', '', 'a:12:{s:4:"name";s:9:"AJAX Test";s:11:"description";s:40:"Support module for AJAX framework tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/batch_test.module', 'batch_test', 'module', '', 'a:12:{s:4:"name";s:14:"Batch API test";s:11:"description";s:35:"Support module for Batch API tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/update/tests/bbb_update_test.module', 'bbb_update_test', 'module', '', 'a:12:{s:4:"name";s:15:"BBB Update test";s:11:"description";s:41:"Support module for update module testing.";s:7:"package";s:7:"Testing";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/block/block.module', 'block', 'module', '', 'a:12:{s:4:"name";s:5:"Block";s:11:"description";s:140:"Controls the visual building blocks a page is constructed with. Blocks are boxes of content rendered into an area, or region, of a web page.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:10:"block.test";}s:9:"configure";s:21:"admin/structure/block";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/block/tests/block_test.module', 'block_test', 'module', '', 'a:12:{s:4:"name";s:10:"Block test";s:11:"description";s:21:"Provides test blocks.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/blog/blog.module', 'blog', 'module', '', 'a:11:{s:4:"name";s:4:"Blog";s:11:"description";s:25:"Enables multi-user blogs.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"blog.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/book/book.module', 'book', 'module', '', 'a:13:{s:4:"name";s:4:"Book";s:11:"description";s:66:"Allows users to create and organize related content in an outline.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"book.test";}s:9:"configure";s:27:"admin/content/book/settings";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:8:"book.css";s:21:"modules/book/book.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/update/tests/ccc_update_test.module', 'ccc_update_test', 'module', '', 'a:12:{s:4:"name";s:15:"CCC Update test";s:11:"description";s:41:"Support module for update module testing.";s:7:"package";s:7:"Testing";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/color/color.module', 'color', 'module', '', 'a:11:{s:4:"name";s:5:"Color";s:11:"description";s:70:"Allows administrators to change the color scheme of compatible themes.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:10:"color.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/comment/comment.module', 'comment', 'module', '', 'a:13:{s:4:"name";s:7:"Comment";s:11:"description";s:57:"Allows users to comment on and discuss published content.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:4:"text";}s:5:"files";a:2:{i:0;s:14:"comment.module";i:1;s:12:"comment.test";}s:9:"configure";s:21:"admin/content/comment";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:11:"comment.css";s:27:"modules/comment/comment.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/common_test.module', 'common_test', 'module', '', 'a:13:{s:4:"name";s:11:"Common Test";s:11:"description";s:32:"Support module for Common tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:15:"common_test.css";s:40:"modules/simpletest/tests/common_test.css";}s:5:"print";a:1:{s:21:"common_test.print.css";s:46:"modules/simpletest/tests/common_test.print.css";}}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/contact/contact.module', 'contact', 'module', '', 'a:12:{s:4:"name";s:7:"Contact";s:11:"description";s:61:"Enables the use of both personal and site-wide contact forms.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:12:"contact.test";}s:9:"configure";s:23:"admin/structure/contact";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/contextual/contextual.module', 'contextual', 'module', '', 'a:11:{s:4:"name";s:16:"Contextual links";s:11:"description";s:75:"Provides contextual links to perform actions related to elements on a page.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/dashboard/dashboard.module', 'dashboard', 'module', '', 'a:12:{s:4:"name";s:9:"Dashboard";s:11:"description";s:136:"Provides a dashboard page in the administrative interface for organizing administrative tasks and tracking information within your site.";s:4:"core";s:3:"7.x";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:5:"files";a:1:{i:0;s:14:"dashboard.test";}s:12:"dependencies";a:1:{i:0;s:5:"block";}s:9:"configure";s:25:"admin/dashboard/customize";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/database_test.module', 'database_test', 'module', '', 'a:12:{s:4:"name";s:13:"Database Test";s:11:"description";s:40:"Support module for Database layer tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/dblog/dblog.module', 'dblog', 'module', '', 'a:11:{s:4:"name";s:16:"Database logging";s:11:"description";s:47:"Logs and records system events to the database.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:10:"dblog.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/drupal_system_listing_compatible_test/drupal_system_listing_compatible_test.module', 'drupal_system_listing_compatible_test', 'module', '', 'a:12:{s:4:"name";s:37:"Drupal system listing compatible test";s:11:"description";s:62:"Support module for testing the drupal_system_listing function.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/drupal_system_listing_incompatible_test/drupal_system_listing_incompatible_test.module', 'drupal_system_listing_incompatible_test', 'module', '', 'a:12:{s:4:"name";s:39:"Drupal system listing incompatible test";s:11:"description";s:62:"Support module for testing the drupal_system_listing function.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/entity_cache_test.module', 'entity_cache_test', 'module', '', 'a:12:{s:4:"name";s:17:"Entity cache test";s:11:"description";s:40:"Support module for testing entity cache.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:28:"entity_cache_test_dependency";}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/entity_cache_test_dependency.module', 'entity_cache_test_dependency', 'module', '', 'a:12:{s:4:"name";s:28:"Entity cache test dependency";s:11:"description";s:51:"Support dependency module for testing entity cache.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/entity_crud_hook_test.module', 'entity_crud_hook_test', 'module', '', 'a:12:{s:4:"name";s:22:"Entity CRUD Hooks Test";s:11:"description";s:35:"Support module for CRUD hook tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/error_test.module', 'error_test', 'module', '', 'a:12:{s:4:"name";s:10:"Error test";s:11:"description";s:47:"Support module for error and exception testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/field/field.module', 'field', 'module', '', 'a:13:{s:4:"name";s:5:"Field";s:11:"description";s:57:"Field API to add fields to entities like nodes and users.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:3:{i:0;s:12:"field.module";i:1;s:16:"field.attach.inc";i:2;s:16:"tests/field.test";}s:12:"dependencies";a:1:{i:0;s:17:"field_sql_storage";}s:8:"required";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"theme/field.css";s:29:"modules/field/theme/field.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/field/modules/field_sql_storage/field_sql_storage.module', 'field_sql_storage', 'module', '', 'a:12:{s:4:"name";s:17:"Field SQL storage";s:11:"description";s:37:"Stores field data in an SQL database.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:22:"field_sql_storage.test";}s:8:"required";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/field/tests/field_test.module', 'field_test', 'module', '', 'a:12:{s:4:"name";s:14:"Field API Test";s:11:"description";s:39:"Support module for the Field API tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:5:"files";a:1:{i:0;s:21:"field_test.entity.inc";}s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/field_ui/field_ui.module', 'field_ui', 'module', '', 'a:11:{s:4:"name";s:8:"Field UI";s:11:"description";s:33:"User interface for the Field API.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:13:"field_ui.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/file/file.module', 'file', 'module', '', 'a:11:{s:4:"name";s:4:"File";s:11:"description";s:26:"Defines a file field type.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:15:"tests/file.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/file/tests/file_module_test.module', 'file_module_test', 'module', '', 'a:12:{s:4:"name";s:9:"File test";s:11:"description";s:53:"Provides hooks for testing File module functionality.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/file_test.module', 'file_test', 'module', '', 'a:12:{s:4:"name";s:9:"File test";s:11:"description";s:39:"Support module for file handling tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:16:"file_test.module";}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/filter/filter.module', 'filter', 'module', '', 'a:13:{s:4:"name";s:6:"Filter";s:11:"description";s:43:"Filters content in preparation for display.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:11:"filter.test";}s:8:"required";b:1;s:9:"configure";s:28:"admin/config/content/formats";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/filter_test.module', 'filter_test', 'module', '', 'a:12:{s:4:"name";s:18:"Filter test module";s:11:"description";s:33:"Tests filter hooks and functions.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/form_test.module', 'form_test', 'module', '', 'a:12:{s:4:"name";s:12:"FormAPI Test";s:11:"description";s:34:"Support module for Form API tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/forum/forum.module', 'forum', 'module', '', 'a:13:{s:4:"name";s:5:"Forum";s:11:"description";s:27:"Provides discussion forums.";s:12:"dependencies";a:2:{i:0;s:8:"taxonomy";i:1;s:7:"comment";}s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:10:"forum.test";}s:9:"configure";s:21:"admin/structure/forum";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:9:"forum.css";s:23:"modules/forum/forum.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/help/help.module', 'help', 'module', '', 'a:11:{s:4:"name";s:4:"Help";s:11:"description";s:35:"Manages the display of online help.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"help.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/image/image.module', 'image', 'module', '', 'a:12:{s:4:"name";s:5:"Image";s:11:"description";s:34:"Provides image manipulation tools.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:4:"file";}s:5:"files";a:1:{i:0;s:10:"image.test";}s:9:"configure";s:31:"admin/config/media/image-styles";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/image/tests/image_module_test.module', 'image_module_test', 'module', '', 'a:12:{s:4:"name";s:10:"Image test";s:11:"description";s:69:"Provides hook implementations for testing Image module functionality.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:24:"image_module_test.module";}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/image_test.module', 'image_test', 'module', '', 'a:12:{s:4:"name";s:10:"Image test";s:11:"description";s:39:"Support module for image toolkit tests.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/field/modules/list/list.module', 'list', 'module', '', 'a:11:{s:4:"name";s:4:"List";s:11:"description";s:69:"Defines list field types. Use with Options to create selection lists.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:2:{i:0;s:5:"field";i:1;s:7:"options";}s:5:"files";a:1:{i:0;s:15:"tests/list.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/field/modules/list/tests/list_test.module', 'list_test', 'module', '', 'a:12:{s:4:"name";s:9:"List test";s:11:"description";s:41:"Support module for the List module tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/locale/locale.module', 'locale', 'module', '', 'a:12:{s:4:"name";s:6:"Locale";s:11:"description";s:119:"Adds language handling functionality and enables the translation of the user interface to languages other than English.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:11:"locale.test";}s:9:"configure";s:30:"admin/config/regional/language";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/locale/tests/locale_test.module', 'locale_test', 'module', '', 'a:12:{s:4:"name";s:11:"Locale Test";s:11:"description";s:42:"Support module for the locale layer tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/menu/menu.module', 'menu', 'module', '', 'a:12:{s:4:"name";s:4:"Menu";s:11:"description";s:60:"Allows administrators to customize the site navigation menu.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"menu.test";}s:9:"configure";s:20:"admin/structure/menu";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/menu_test.module', 'menu_test', 'module', '', 'a:12:{s:4:"name";s:15:"Hook menu tests";s:11:"description";s:37:"Support module for menu hook testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('profiles/minimal/minimal.profile', 'minimal', 'module', '', 'a:14:{s:4:"name";s:7:"Minimal";s:11:"description";s:38:"Start with only a few modules enabled.";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:2:{i:0;s:5:"block";i:1;s:5:"dblog";}s:5:"files";a:1:{i:0;s:15:"minimal.profile";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:7:"package";s:5:"Other";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;s:6:"hidden";b:1;s:8:"required";b:1;s:17:"distribution_name";s:6:"Drupal";}'), ('modules/simpletest/tests/module_test.module', 'module_test', 'module', '', 'a:12:{s:4:"name";s:11:"Module test";s:11:"description";s:41:"Support module for module system testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/node/node.module', 'node', 'module', '', 'a:14:{s:4:"name";s:4:"Node";s:11:"description";s:66:"Allows content to be submitted to the site and displayed on pages.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:2:{i:0;s:11:"node.module";i:1;s:9:"node.test";}s:8:"required";b:1;s:9:"configure";s:21:"admin/structure/types";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:8:"node.css";s:21:"modules/node/node.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/node/tests/node_access_test.module', 'node_access_test', 'module', '', 'a:12:{s:4:"name";s:24:"Node module access tests";s:11:"description";s:43:"Support module for node permission testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/node/tests/node_test.module', 'node_test', 'module', '', 'a:12:{s:4:"name";s:17:"Node module tests";s:11:"description";s:40:"Support module for node related testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/node/tests/node_test_exception.module', 'node_test_exception', 'module', '', 'a:12:{s:4:"name";s:27:"Node module exception tests";s:11:"description";s:50:"Support module for node related exception testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/field/modules/number/number.module', 'number', 'module', '', 'a:11:{s:4:"name";s:6:"Number";s:11:"description";s:28:"Defines numeric field types.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:11:"number.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/openid/openid.module', 'openid', 'module', '', 'a:11:{s:4:"name";s:6:"OpenID";s:11:"description";s:48:"Allows users to log into your site using OpenID.";s:7:"version";s:3:"7.4";s:7:"package";s:4:"Core";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:11:"openid.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/openid/tests/openid_test.module', 'openid_test', 'module', '', 'a:12:{s:4:"name";s:21:"OpenID dummy provider";s:11:"description";s:33:"OpenID provider used for testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:6:"openid";}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/field/modules/options/options.module', 'options', 'module', '', 'a:11:{s:4:"name";s:7:"Options";s:11:"description";s:82:"Defines selection, check box and radio button widgets for text and numeric fields.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:12:"options.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/overlay/overlay.module', 'overlay', 'module', '', 'a:11:{s:4:"name";s:7:"Overlay";s:11:"description";s:59:"Displays the Drupal administration interface in an overlay.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/path/path.module', 'path', 'module', '', 'a:12:{s:4:"name";s:4:"Path";s:11:"description";s:28:"Allows users to rename URLs.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"path.test";}s:9:"configure";s:24:"admin/config/search/path";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/php/php.module', 'php', 'module', '', 'a:11:{s:4:"name";s:10:"PHP filter";s:11:"description";s:50:"Allows embedded PHP code/snippets to be evaluated.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:8:"php.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/poll/poll.module', 'poll', 'module', '', 'a:12:{s:4:"name";s:4:"Poll";s:11:"description";s:95:"Allows your site to capture votes on different topics in the form of multiple choice questions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:9:"poll.test";}s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:8:"poll.css";s:21:"modules/poll/poll.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/profile/profile.module', 'profile', 'module', '', 'a:13:{s:4:"name";s:7:"Profile";s:11:"description";s:36:"Supports configurable user profiles.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:12:"profile.test";}s:9:"configure";s:27:"admin/config/people/profile";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/rdf/rdf.module', 'rdf', 'module', '', 'a:11:{s:4:"name";s:3:"RDF";s:11:"description";s:148:"Enriches your content with metadata to let other applications (e.g. search engines, aggregators) better understand its relationships and attributes.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:8:"rdf.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/rdf/tests/rdf_test.module', 'rdf_test', 'module', '', 'a:12:{s:4:"name";s:16:"RDF module tests";s:11:"description";s:38:"Support module for RDF module testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/requirements1_test.module', 'requirements1_test', 'module', '', 'a:12:{s:4:"name";s:19:"Requirements 1 Test";s:11:"description";s:80:"Tests that a module is not installed when it fails hook_requirements(''install'').";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/requirements2_test.module', 'requirements2_test', 'module', '', 'a:12:{s:4:"name";s:19:"Requirements 2 Test";s:11:"description";s:98:"Tests that a module is not installed when the one it depends on fails hook_requirements(''install).";s:12:"dependencies";a:2:{i:0;s:18:"requirements1_test";i:1;s:7:"comment";}s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/search/search.module', 'search', 'module', '', 'a:13:{s:4:"name";s:6:"Search";s:11:"description";s:36:"Enables site-wide keyword searching.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:2:{i:0;s:19:"search.extender.inc";i:1;s:11:"search.test";}s:9:"configure";s:28:"admin/config/search/settings";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"search.css";s:25:"modules/search/search.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/search/tests/search_embedded_form.module', 'search_embedded_form', 'module', '', 'a:12:{s:4:"name";s:20:"Search embedded form";s:11:"description";s:59:"Support module for search module testing of embedded forms.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/search/tests/search_extra_type.module', 'search_extra_type', 'module', '', 'a:12:{s:4:"name";s:16:"Test search type";s:11:"description";s:41:"Support module for search module testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/session_test.module', 'session_test', 'module', '', 'a:12:{s:4:"name";s:12:"Session test";s:11:"description";s:40:"Support module for session data testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/shortcut/shortcut.module', 'shortcut', 'module', '', 'a:12:{s:4:"name";s:8:"Shortcut";s:11:"description";s:60:"Allows users to manage customizable lists of shortcut links.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:13:"shortcut.test";}s:9:"configure";s:36:"admin/config/user-interface/shortcut";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/simpletest.module', 'simpletest', 'module', '', 'a:12:{s:4:"name";s:7:"Testing";s:11:"description";s:53:"Provides a framework for unit and functional testing.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:41:{i:0;s:15:"simpletest.test";i:1;s:24:"drupal_web_test_case.php";i:2;s:18:"tests/actions.test";i:3;s:15:"tests/ajax.test";i:4;s:16:"tests/batch.test";i:5;s:20:"tests/bootstrap.test";i:6;s:16:"tests/cache.test";i:7;s:17:"tests/common.test";i:8;s:24:"tests/database_test.test";i:9;s:32:"tests/entity_crud_hook_test.test";i:10;s:23:"tests/entity_query.test";i:11;s:16:"tests/error.test";i:12;s:15:"tests/file.test";i:13;s:23:"tests/filetransfer.test";i:14;s:15:"tests/form.test";i:15;s:16:"tests/graph.test";i:16;s:16:"tests/image.test";i:17;s:15:"tests/lock.test";i:18;s:15:"tests/mail.test";i:19;s:15:"tests/menu.test";i:20;s:17:"tests/module.test";i:21;s:19:"tests/password.test";i:22;s:15:"tests/path.test";i:23;s:19:"tests/registry.test";i:24;s:17:"tests/schema.test";i:25;s:18:"tests/session.test";i:26;s:20:"tests/tablesort.test";i:27;s:16:"tests/theme.test";i:28;s:18:"tests/unicode.test";i:29;s:17:"tests/update.test";i:30;s:17:"tests/xmlrpc.test";i:31;s:26:"tests/upgrade/upgrade.test";i:32;s:34:"tests/upgrade/upgrade.comment.test";i:33;s:33:"tests/upgrade/upgrade.filter.test";i:34;s:32:"tests/upgrade/upgrade.forum.test";i:35;s:33:"tests/upgrade/upgrade.locale.test";i:36;s:31:"tests/upgrade/upgrade.menu.test";i:37;s:31:"tests/upgrade/upgrade.node.test";i:38;s:35:"tests/upgrade/upgrade.taxonomy.test";i:39;s:33:"tests/upgrade/upgrade.upload.test";i:40;s:31:"tests/upgrade/upgrade.user.test";}s:9:"configure";s:41:"admin/config/development/testing/settings";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/statistics/statistics.module', 'statistics', 'module', '', 'a:12:{s:4:"name";s:10:"Statistics";s:11:"description";s:37:"Logs access statistics for your site.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:15:"statistics.test";}s:9:"configure";s:30:"admin/config/system/statistics";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/syslog/syslog.module', 'syslog', 'module', '', 'a:11:{s:4:"name";s:6:"Syslog";s:11:"description";s:41:"Logs and records system events to syslog.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:11:"syslog.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/system_dependencies_test.module', 'system_dependencies_test', 'module', '', 'a:12:{s:4:"name";s:22:"System dependency test";s:11:"description";s:47:"Support module for testing system dependencies.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:12:"dependencies";a:1:{i:0;s:19:"_missing_dependency";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/system_test.module', 'system_test', 'module', '', 'a:12:{s:4:"name";s:11:"System test";s:11:"description";s:34:"Support module for system testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:18:"system_test.module";}s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/taxonomy/taxonomy.module', 'taxonomy', 'module', '', 'a:12:{s:4:"name";s:8:"Taxonomy";s:11:"description";s:38:"Enables the categorization of content.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:7:"options";}s:5:"files";a:2:{i:0;s:15:"taxonomy.module";i:1;s:13:"taxonomy.test";}s:9:"configure";s:24:"admin/structure/taxonomy";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/taxonomy_test.module', 'taxonomy_test', 'module', '', 'a:12:{s:4:"name";s:20:"Taxonomy test module";s:11:"description";s:45:""Tests functions and hooks not used in core".";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:12:"dependencies";a:1:{i:0;s:8:"taxonomy";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/field/modules/text/text.module', 'text', 'module', '', 'a:12:{s:4:"name";s:4:"Text";s:11:"description";s:32:"Defines simple text field types.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:12:"dependencies";a:1:{i:0;s:5:"field";}s:5:"files";a:1:{i:0;s:9:"text.test";}s:8:"required";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/theme_test.module', 'theme_test', 'module', '', 'a:12:{s:4:"name";s:10:"Theme test";s:11:"description";s:40:"Support module for theme system testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/toolbar/toolbar.module', 'toolbar', 'module', '', 'a:11:{s:4:"name";s:7:"Toolbar";s:11:"description";s:99:"Provides a toolbar that shows the top-level administration menu items and links from other modules.";s:4:"core";s:3:"7.x";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/tracker/tracker.module', 'tracker', 'module', '', 'a:11:{s:4:"name";s:7:"Tracker";s:11:"description";s:45:"Enables tracking of recent content for users.";s:12:"dependencies";a:1:{i:0;s:7:"comment";}s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:12:"tracker.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/translation/translation.module', 'translation', 'module', '', 'a:11:{s:4:"name";s:19:"Content translation";s:11:"description";s:57:"Allows content to be translated into different languages.";s:12:"dependencies";a:1:{i:0;s:6:"locale";}s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:16:"translation.test";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/translation/tests/translation_test.module', 'translation_test', 'module', '', 'a:12:{s:4:"name";s:24:"Content Translation Test";s:11:"description";s:49:"Support module for the content translation tests.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/trigger/trigger.module', 'trigger', 'module', '', 'a:12:{s:4:"name";s:7:"Trigger";s:11:"description";s:90:"Enables actions to be fired on certain system events, such as when new content is created.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:12:"trigger.test";}s:9:"configure";s:23:"admin/structure/trigger";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/trigger/tests/trigger_test.module', 'trigger_test', 'module', '', 'a:12:{s:4:"name";s:12:"Trigger Test";s:11:"description";s:33:"Support module for Trigger tests.";s:7:"package";s:7:"Testing";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/update/update.module', 'update', 'module', '', 'a:12:{s:4:"name";s:14:"Update manager";s:11:"description";s:104:"Checks for available updates, and can securely install or update modules and themes via a web interface.";s:7:"version";s:3:"7.4";s:7:"package";s:4:"Core";s:4:"core";s:3:"7.x";s:5:"files";a:1:{i:0;s:11:"update.test";}s:9:"configure";s:30:"admin/reports/updates/settings";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/update/tests/update_test.module', 'update_test', 'module', '', 'a:12:{s:4:"name";s:11:"Update test";s:11:"description";s:41:"Support module for update module testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/update_test_1.module', 'update_test_1', 'module', '', 'a:12:{s:4:"name";s:11:"Update test";s:11:"description";s:34:"Support module for update testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/update_test_2.module', 'update_test_2', 'module', '', 'a:12:{s:4:"name";s:11:"Update test";s:11:"description";s:34:"Support module for update testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/update_test_3.module', 'update_test_3', 'module', '', 'a:12:{s:4:"name";s:11:"Update test";s:11:"description";s:34:"Support module for update testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/url_alter_test.module', 'url_alter_test', 'module', '', 'a:12:{s:4:"name";s:15:"Url_alter tests";s:11:"description";s:45:"A support modules for url_alter hook testing.";s:4:"core";s:3:"7.x";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/user/user.module', 'user', 'module', '', 'a:14:{s:4:"name";s:4:"User";s:11:"description";s:47:"Manages the user registration and login system.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:2:{i:0;s:11:"user.module";i:1;s:9:"user.test";}s:8:"required";b:1;s:9:"configure";s:19:"admin/config/people";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:8:"user.css";s:21:"modules/user/user.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'), ('modules/user/tests/user_form_test.module', 'user_form_test', 'module', '', 'a:12:{s:4:"name";s:22:"User module form tests";s:11:"description";s:37:"Support module for user form testing.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}'), ('modules/simpletest/tests/xmlrpc_test.module', 'xmlrpc_test', 'module', '', 'a:12:{s:4:"name";s:12:"XML-RPC Test";s:11:"description";s:75:"Support module for XML-RPC tests according to the validator1 specification.";s:7:"package";s:7:"Testing";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:5:"files";a:0:{}s:9:"bootstrap";i:0;}');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('ArchiverTar', 'class', 'modules/system/system.archiver.inc', 'system', '0'), ('ArchiverZip', 'class', 'modules/system/system.archiver.inc', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.archiver.inc', 'faa849f3e646a910ab82fd6c8bbf0a4e6b8c60725d7ba81ec0556bd716616cd1');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DefaultMailSystem', 'class', 'modules/system/system.mail.inc', 'system', '0'), ('TestingMailSystem', 'class', 'modules/system/system.mail.inc', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.mail.inc', 'b7ee9ea80059788d8b53823a91db015dfa6c2a66589a6eca6f97b99340e12d6f');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalQueue', 'class', 'modules/system/system.queue.inc', 'system', '0'), ('DrupalQueueInterface', 'interface', 'modules/system/system.queue.inc', 'system', '0'), ('DrupalReliableQueueInterface', 'interface', 'modules/system/system.queue.inc', 'system', '0'), ('SystemQueue', 'class', 'modules/system/system.queue.inc', 'system', '0'), ('MemoryQueue', 'class', 'modules/system/system.queue.inc', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.queue.inc', '4bfc1845db9c888f3df0937a9ff6d783de77880e6301431db9eb2268b9fe572d');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('Archive_Tar', 'class', 'modules/system/system.tar.inc', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.tar.inc', '743529eab763be74e8169c945e875381fd77d71e336c6d77b7d28e2dfd023a21');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('ModuleUpdater', 'class', 'modules/system/system.updater.inc', 'system', '0'), ('ThemeUpdater', 'class', 'modules/system/system.updater.inc', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.updater.inc', 'e2eeed65b833a6215f807b113f6fb4cc3cc487e93efcb1402ed87c536d2c9ea6');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('ModuleTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('EnableDisableTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('HookRequirementsTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('ModuleDependencyTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('ModuleVersionTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('ModuleRequiredTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('IPAddressBlockingTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('CronRunTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('AdminMetaTagTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('AccessDeniedTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('PageNotFoundTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('SiteMaintenanceTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('DateTimeFunctionalTest', 'class', 'modules/system/system.test', 'system', '0'), ('PageTitleFiltering', 'class', 'modules/system/system.test', 'system', '0'), ('FrontPageTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('SystemBlockTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('SystemMainContentFallback', 'class', 'modules/system/system.test', 'system', '0'), ('SystemThemeFunctionalTest', 'class', 'modules/system/system.test', 'system', '0'), ('QueueTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('TokenReplaceTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('InfoFileParserTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('SystemInfoAlterTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('UpdateScriptFunctionalTest', 'class', 'modules/system/system.test', 'system', '0'), ('FloodFunctionalTest', 'class', 'modules/system/system.test', 'system', '0'), ('RetrieveFileTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('ShutdownFunctionsTest', 'class', 'modules/system/system.test', 'system', '0'), ('SystemAdminTestCase', 'class', 'modules/system/system.test', 'system', '0'), ('SystemAuthorizeCase', 'class', 'modules/system/system.test', 'system', '0'), ('SystemIndexPhpTest', 'class', 'modules/system/system.test', 'system', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/system/system.test', 'cd76c21c50012b749099d0e2123ff3d10526fc8a99acd60e0cb06a2e87275bcc');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('UserController', 'class', 'modules/user/user.module', 'user', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/user/user.module', '1975a044c7fbd8c52c2aaaaff14263bf9e3dc3e71417ddf54cfe456362dce638');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('UserRegistrationTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserValidationTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserLoginTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserCancelTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserPictureTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserPermissionsTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserAdminTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserTimeZoneFunctionalTest', 'class', 'modules/user/user.test', 'user', '0'), ('UserAutocompleteTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserAccountLinksUnitTests', 'class', 'modules/user/user.test', 'user', '0'), ('UserBlocksUnitTests', 'class', 'modules/user/user.test', 'user', '0'), ('UserSaveTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserCreateTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserEditTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserSignatureTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserEditedOwnAccountTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserRoleAdminTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserTokenReplaceTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserUserSearchTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserRolesAssignmentTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserAuthmapAssignmentTestCase', 'class', 'modules/user/user.test', 'user', '0'), ('UserValidateCurrentPassCustomForm', 'class', 'modules/user/user.test', 'user', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/user/user.test', '9b6cc136d3a267ad039069ac040f9ec33481ef43fd1fb888929750a076302c7c');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FileTransfer', 'class', 'includes/filetransfer/filetransfer.inc', '', '0'), ('FileTransferException', 'class', 'includes/filetransfer/filetransfer.inc', '', '0'), ('FileTransferChmodInterface', 'interface', 'includes/filetransfer/filetransfer.inc', '', '0'), ('SkipDotsRecursiveDirectoryIterator', 'class', 'includes/filetransfer/filetransfer.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/filetransfer/filetransfer.inc', 'ee9393beddc7190f7a161f5563953d1b58e355026fcc2392443a9e6b4c600531');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FileTransferSSH', 'class', 'includes/filetransfer/ssh.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/filetransfer/ssh.inc', '002e24a24cac133d12728bd3843868ce378681237d7fad420761af84e6efe5ad');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FileTransferFTP', 'class', 'includes/filetransfer/ftp.inc', '', '0'), ('FileTransferFTPExtension', 'class', 'includes/filetransfer/ftp.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/filetransfer/ftp.inc', '589ebf4b8bd4a2973aa56a156ac1fa83b6c73e703391361fb573167670e0d832');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FileTransferLocal', 'class', 'includes/filetransfer/local.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/filetransfer/local.inc', '7cbfdb46abbdf539640db27e66fb30e5265128f31002bd0dfc3af16ae01a9492');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('InsertQuery_mysql', 'class', 'includes/database/mysql/query.inc', '', '0'), ('TruncateQuery_mysql', 'class', 'includes/database/mysql/query.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/mysql/query.inc', 'e2a5457ec40a8f88f6a822bdc77f74865e4c02206fc733c2945c8897f46093de');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseConnection_mysql', 'class', 'includes/database/mysql/database.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/mysql/database.inc', '492d898b527af9af579e9ee4c390bc9f218faddf3783350d433de48106083b82');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseSchema_mysql', 'class', 'includes/database/mysql/schema.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/mysql/schema.inc', 'f71af9dda287f0f37e7bd0077b801a6d3f38c951a9c2c6ea3b71dff1b69077d3');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseTasks_mysql', 'class', 'includes/database/mysql/install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/mysql/install.inc', '6ae316941f771732fbbabed7e1d6b4cbb41b1f429dd097d04b3345aa15e461a0');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('SelectQuery_pgsql', 'class', 'includes/database/pgsql/select.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/pgsql/select.inc', 'ce06c4353d4322e519e1c90ca863e6666edc370fb3c12568fedf466334b2e2be');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('InsertQuery_pgsql', 'class', 'includes/database/pgsql/query.inc', '', '0'), ('UpdateQuery_pgsql', 'class', 'includes/database/pgsql/query.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/pgsql/query.inc', 'cb4c84f8f1ffc73098ed71137248dcd078a505a7530e60d979d74b3a3cdaa658');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseConnection_pgsql', 'class', 'includes/database/pgsql/database.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/pgsql/database.inc', '30bdaa3761cb93f4596f81320f89138fd9e31b2a976b4d03ce81c59e32129a31');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseSchema_pgsql', 'class', 'includes/database/pgsql/schema.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/pgsql/schema.inc', '1442123ab5040a55797126cfa1cf9103f7a9df22b3c23b5055c9c27e4f12d262');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseTasks_pgsql', 'class', 'includes/database/pgsql/install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/pgsql/install.inc', '585b80c5bbd6f134bff60d06397f15154657a577d4da8d1b181858905f09dea5');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('SelectQuery_parelastic', 'class', 'includes/database/parelastic/select.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/parelastic/select.inc', '487414d4f3eae131f346ad7645d9fb1b9136b38a061683e61e3ef76bdb3b7297');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('InsertQuery_parelastic', 'class', 'includes/database/parelastic/query.inc', '', '0'), ('TruncateQuery_parelastic', 'class', 'includes/database/parelastic/query.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/parelastic/query.inc', '69cb664327b16e08e76707bf787a2c60c1cd75c15c0c6f10d8f2972f513154fd');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseConnection_parelastic', 'class', 'includes/database/parelastic/database.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/parelastic/database.inc', '6626a1537b486d239339f02d852f97f659c43d37901e5200d072b6eda5932f53');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseSchema_parelastic', 'class', 'includes/database/parelastic/schema.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/parelastic/schema.inc', '0fcc20362f2b42a265652dd429a8059d263611e4e7806ccfc3135fd35c95e78a');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseTasks_parelastic', 'class', 'includes/database/parelastic/install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/parelastic/install.inc', 'b4f0b729436ee4ad31cfac382b503d9f5bcd989b8c03c0d216206e83380b8a99');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('SelectQuery_sqlite', 'class', 'includes/database/sqlite/select.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/sqlite/select.inc', '4ecb9d21d2f07237f7603e925519886dde0b8da82f96999b865ff0803438744e');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('InsertQuery_sqlite', 'class', 'includes/database/sqlite/query.inc', '', '0'), ('UpdateQuery_sqlite', 'class', 'includes/database/sqlite/query.inc', '', '0'), ('DeleteQuery_sqlite', 'class', 'includes/database/sqlite/query.inc', '', '0'), ('TruncateQuery_sqlite', 'class', 'includes/database/sqlite/query.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/sqlite/query.inc', '61e0459c0c9252ca465b86c475d88e09ea34c8cdb28220eb37a7d44357f5474f');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseConnection_sqlite', 'class', 'includes/database/sqlite/database.inc', '', '0'), ('DatabaseStatement_sqlite', 'class', 'includes/database/sqlite/database.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/sqlite/database.inc', '644fe9e0b31303b46b080cbc41fe4c2b3fc972071dcb34f754d7f83f0ce79083');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseSchema_sqlite', 'class', 'includes/database/sqlite/schema.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/sqlite/schema.inc', '4a42520b90765b970f285ad14f94ded061cee70a2dc6d630c3261649852daf0f');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseTasks_sqlite', 'class', 'includes/database/sqlite/install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/sqlite/install.inc', '381f3db8c59837d961978ba3097bb6443534ed1659fd713aa563963fa0c42cc5');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseStatementPrefetch', 'class', 'includes/database/prefetch.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/prefetch.inc', '8d39658800c5b648b8aafc253cd36162c22ba54febe543ad14fc50016b02ba93');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('QueryExtendableInterface', 'interface', 'includes/database/select.inc', '', '0'), ('SelectQueryInterface', 'interface', 'includes/database/select.inc', '', '0'), ('SelectQueryExtender', 'class', 'includes/database/select.inc', '', '0'), ('SelectQuery', 'class', 'includes/database/select.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/select.inc', '8ac2735a840f1de662412e590d7de2eb3cf85925b34ec945fbd24a51be817e5c');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseLog', 'class', 'includes/database/log.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/log.inc', '4ecbdf9022d8c612310b41af575f10b0d4c041c0fbc41c6dc7e1f2ab6eacce6b');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('QueryConditionInterface', 'interface', 'includes/database/query.inc', '', '0'), ('QueryAlterableInterface', 'interface', 'includes/database/query.inc', '', '0'), ('QueryPlaceholderInterface', 'interface', 'includes/database/query.inc', '', '0'), ('Query', 'class', 'includes/database/query.inc', '', '0'), ('InsertQuery', 'class', 'includes/database/query.inc', '', '0'), ('DeleteQuery', 'class', 'includes/database/query.inc', '', '0'), ('TruncateQuery', 'class', 'includes/database/query.inc', '', '0'), ('UpdateQuery', 'class', 'includes/database/query.inc', '', '0'), ('MergeQuery', 'class', 'includes/database/query.inc', '', '0'), ('DatabaseCondition', 'class', 'includes/database/query.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/query.inc', '6043f7d99db75dff989d48f38dd4300c7ee57e3c10a0e09fe3c632f1b03c2f18');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseConnection', 'class', 'includes/database/database.inc', '', '0'), ('Database', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseTransactionNoActiveException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseTransactionNameNonUniqueException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseTransactionCommitFailedException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseTransactionExplicitCommitNotAllowedException', 'class', 'includes/database/database.inc', '', '0'), ('InvalidMergeQueryException', 'class', 'includes/database/database.inc', '', '0'), ('FieldsOverlapException', 'class', 'includes/database/database.inc', '', '0'), ('NoFieldsException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseConnectionNotDefinedException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseDriverNotSpecifiedException', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseTransaction', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseStatementInterface', 'interface', 'includes/database/database.inc', '', '0'), ('DatabaseStatementBase', 'class', 'includes/database/database.inc', '', '0'), ('DatabaseStatementEmpty', 'class', 'includes/database/database.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/database.inc', '049e642a52e3241283209c1736f0e82ffdbf3201c5ae13c327533c0481e1f61f');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseSchema', 'class', 'includes/database/schema.inc', '', '0'), ('DatabaseSchemaObjectExistsException', 'class', 'includes/database/schema.inc', '', '0'), ('DatabaseSchemaObjectDoesNotExistException', 'class', 'includes/database/schema.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/database/schema.inc', '11ab1a9b145b332176ac8bb1be62ccf8765093275867d855fa838a9ba2b5793f');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/language.inc', '5e0595c6def071694fa500b4636d15d482dafddb612a977201b5406b813be7a6');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/file.mimetypes.inc', 'dbc1d9a63bc22986a039b246f30053dab5985f0120f6cebf06f79251da18643e');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/xmlrpcs.inc', '2df450cf2153959a4581d8e61867c587f61fcdea5dd2653e36c6a6028fc1b395');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('MailSystemInterface', 'interface', 'includes/mail.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/mail.inc', '702174958dad7b460a75435abc557f472a2549bc53362735f76baba9b2265c27');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/path.inc', '992d563dbd6e67f58e358176be09eb1fb75f95510a93cb45bf463ce798670bff');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/locale.inc', '5e6890eaaab5acd3bda2f2d2dc6e3faa9a4a77649ca2cae4225b9d0f24b50fda');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/xmlrpc.inc', '1176a9e5b5990f8219b48c49bb858a2a980f95428aa519acea19282109e27e83');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/theme.inc', 'fa657c7ab6573ae41acadd87a052c4eafda0ed75eac2adc7e65ef4d568671e94');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/menu.inc', 'a0fd18e4feef029acff5977429ee71f79fa3f59d85061d4b521f6e9bf551a142');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/graph.inc', '577a50594521edecd7139fcecd6af3a941e456ea0e0577e38ee14b785422aabb');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/form.inc', '0cd688e29d4099b82578b3f10a0865ef8c1b339092a69676054c555961902fac');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('BatchQueue', 'class', 'includes/batch.queue.inc', '', '0'), ('BatchMemoryQueue', 'class', 'includes/batch.queue.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/batch.queue.inc', '34face1f61857ec26e414712c647b06acbd5ec4437a71abf090e83cbdf691794');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('ArchiverInterface', 'interface', 'includes/archiver.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/archiver.inc', '097a78f5794237e8242b540649d824b930008027362c1359773e22c2b21cd6e5');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('TableSort', 'class', 'includes/tablesort.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/tablesort.inc', '3f3cb2820920f7edc0c3d046ffba4fc4e3b73a699a2492780371141cf501aa50');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/session.inc', '2a904d6eb8f561b5c6b08c6e7528b76b2b8d7f1de34e9738cebe69a1a0b922b2');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalUpdateException', 'class', 'includes/update.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/update.inc', 'bf8e3ed2e93d680c617f905b888cd42ae6a9dca011f7c804e98e5e372e114402');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('StreamWrapperInterface', 'interface', 'includes/stream_wrappers.inc', '', '0'), ('DrupalStreamWrapperInterface', 'interface', 'includes/stream_wrappers.inc', '', '0'), ('DrupalLocalStreamWrapper', 'class', 'includes/stream_wrappers.inc', '', '0'), ('DrupalPublicStreamWrapper', 'class', 'includes/stream_wrappers.inc', '', '0'), ('DrupalPrivateStreamWrapper', 'class', 'includes/stream_wrappers.inc', '', '0'), ('DrupalTemporaryStreamWrapper', 'class', 'includes/stream_wrappers.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/stream_wrappers.inc', '0943efcecb78427b0de6d014ece9825e9d5d1102baa4b57805275f56e914af1e');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/password.inc', 'b6fab301352a9baa01b589fb96c6d786d997485bd9e202818f2c8cbf439229ab');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/ajax.inc', '7cd37e59457753342168803fe65cd1af7da1c41e8b8a444304e941b435b43f05');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/token.inc', '015328d6573ceece0a0712fcbad890719cff8d65a37839ece36bc64e97d63466');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalEntityControllerInterface', 'interface', 'includes/entity.inc', '', '0'), ('DrupalDefaultEntityController', 'class', 'includes/entity.inc', '', '0'), ('EntityFieldQueryException', 'class', 'includes/entity.inc', '', '0'), ('EntityFieldQuery', 'class', 'includes/entity.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/entity.inc', 'f6cfb2fdc4ab6ffb078e8a25fdcb3df3bb24ef1f904e313a313bf9ddab5af627');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/authorize.inc', 'f732037ded6a22e2e8493da9828a976b0dd2f1aaa57d135d4cb3258f8815a1f1');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/iso.inc', '379c2bd8332ce03a3772787b004e9b1a4468fd23f18d1b9397902fd6d9dbf2d8');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalFakeCache', 'class', 'includes/cache-install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/cache-install.inc', '2d223093cf3740d190746d1c348607694a8475c0da91ffd2386454995a17995e');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/bootstrap.inc', 'b375d9abc41fd916470758b6d1a4fd4a3f09c9ee91e235feaf70eb50a98c2f4a');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/unicode.entities.inc', '2b858138596d961fbaa4c6e3986e409921df7f76b6ee1b109c4af5970f1e0f54');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/module.inc', 'ac27386cf8bfb02192580b8f51ca354a7ea30e444084203d02f2ce9285e85b60');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/install.core.inc', 'b230ede834f21c75dc736d5f583b41b02f3c88eef37b1ee9036d294c7f9dbba2');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/unicode.inc', '5409227091e55cef4c1b0bb9c159720aecaa07e7bf2683fe7c242691aa1036cc');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/batch.inc', 'f32d52b88dc6bd0b3117fef0057d6084fbe132afb0fbf638daaa42bce7a2a3c5');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/common.inc', 'a849f0b777eb03cce3394002a92287b37ff1534fbfd098d3a1707d79b8c4fe12');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/theme.maintenance.inc', 'd110314b4d943c3e965fcefe452f6873b53cd6a8844154467dfcbb2b6142dc82');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/actions.inc', '3e1b89b7dd2465549361915ffc50b713a8f3a568baa703f034e48b24977f83c8');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('PagerDefault', 'class', 'includes/pager.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/pager.inc', 'd3cd989a55da56b91d72223c93cdea125e4d86a3ffdc2782ddb70286aa4c4ecf');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/date.inc', '32124baa81a1411631de23ade5615be0d81bc7c7169ea60d22f9badd333cf284');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/errors.inc', 'aa17eaa99c92e611b549d87e504ca311dd4c59fa478053ed739319c9269c875e');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/utility.inc', '04865d2c973e3df62f673b68631a880dbf39fdc5e22a5935e2e6b86576fa9030');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalCacheInterface', 'interface', 'includes/cache.inc', '', '0'), ('DrupalDatabaseCache', 'class', 'includes/cache.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/cache.inc', '5239bd1707257d0abdd5396d6febd34bea08dd6ceb318265f3c9b7b21b38f23f');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/registry.inc', 'ce22f7715c8037095e6b7dfb86594d3f5ca0908e0614862cc60334852ce3b971');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DatabaseTasks', 'class', 'includes/install.inc', '', '0'), ('DatabaseTaskException', 'class', 'includes/install.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/install.inc', '2dda96a9169cd82e5f49c08c311a1be9decd6c259a385c57b370284072078ec5');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/image.inc', '2fefbd7e73eb51eece8acd7b374b23bf77a717c1553ad83b321a7be194c9e531');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/file.inc', 'da91107f7adecc6d76d9ef6f0acf895b409aefd25db28e1a48b9070deed9301b');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/lock.inc', '7af1b1b93e20db33f0ef81a2577263cf152b363d358d65707539cc952b8272e5');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DrupalUpdaterInterface', 'interface', 'includes/updater.inc', '', '0'), ('Updater', 'class', 'includes/updater.inc', '', '0'), ('UpdaterException', 'class', 'includes/updater.inc', '', '0'), ('UpdaterFileTransferException', 'class', 'includes/updater.inc', '', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('includes/updater.inc', '1715fbda750de86ecde8abd71d61886423367d3c9a17c31138638361b1c27e51');
INSERT INTO d7_users (uid, name, mail) VALUES ('0', '', '');
INSERT INTO d7_users (uid, name, mail, created, status, data) VALUES ('1', 'placeholder-for-uid-1', 'placeholder-for-uid-1', '1311348568', '1', NULL);
INSERT INTO d7_role (name, weight) VALUES ('anonymous user', '0');
INSERT INTO d7_role (name, weight) VALUES ('authenticated user', '1');
INSERT INTO d7_variable (name, value) VALUES ('install_profile_modules', 'a:9:{i:0;s:17:"field_sql_storage";i:1;s:4:"text";i:2;s:5:"field";i:3;s:6:"filter";i:4;s:4:"node";i:5;s:4:"user";i:7;s:5:"block";i:8;s:5:"dblog";i:9;s:7:"minimal";}');
INSERT INTO d7_variable (name, value) VALUES ('install_task', 's:21:"install_system_module";');
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '17282909654e2997f839bc89.47473471', '1311348729.2364');
INSERT INTO d7_variable (name, value) VALUES ('path_alias_whitelist', 'a:0:{}');
INSERT INTO d7_variable (name, value) VALUES ('install_current_batch', 's:23:"install_profile_modules";');
INSERT INTO d7_sequences (value) VALUES (NULL);
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:6:"filter";i:1;s:6:"Filter";}}', '1311348734');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:17:"field_sql_storage";i:1;s:17:"Field SQL storage";}}', '1311348735');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:4:"user";i:1;s:4:"User";}}', '1311348736');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:4:"node";i:1;s:4:"Node";}}', '1311348737');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:5:"field";i:1;s:5:"Field";}}', '1311348738');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:4:"text";i:1;s:4:"Text";}}', '1311348739');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:5:"block";i:1;s:5:"Block";}}', '1311348739');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:5:"dblog";i:1;s:16:"Database logging";}}', '1311348740');
INSERT INTO d7_queue (name, data, created) VALUES ('drupal_batch:1:0', 'a:2:{i:0;s:21:"_install_module_batch";i:1;a:2:{i:0;s:7:"minimal";i:1;s:7:"Minimal";}}', '1311348740');
INSERT INTO d7_variable (name, value) VALUES ('drupal_private_key', 's:43:"CUJnswtKy8nLhqcpD5QtFpFu0Ke7C4LSHb3GudS5-cs";');
INSERT INTO d7_batch (bid, timestamp, token, batch) VALUES ('1', '1311348568', 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo', 'a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:0:{}s:7:"success";b:0;s:5:"start";i:0;s:7:"elapsed";i:0;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:9;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}');
INSERT INTO d7_sessions (sid, ssid, uid, cache, hostname, session, timestamp) VALUES ('nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw', '', '0', '0', '127.0.0.1', 'batches|a:1:{i:1;b:1;}', '1311348568');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '7511433144e29980c267b02.15736042', '1311348749.1564');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '3348278214e2998139d94c6.24180592', '1311348756.6443');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FilterCRUDTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterAdminTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterFormatAccessTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterDefaultFormatTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterNoFormatTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterSecurityTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterUnitTestCase', 'class', 'modules/filter/filter.test', 'filter', '0'), ('FilterHooksTestCase', 'class', 'modules/filter/filter.test', 'filter', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/filter/filter.test', '10051d177985a2bd0f4709b2d94cb2981e2c072ed999d72baae92684ee74e754');
INSERT INTO d7_filter_format (format, name, cache, status, weight) VALUES ('plain_text', 'Plain text', '1', '1', '10');
INSERT INTO d7_filter (format, name, weight, status, module, settings) VALUES ('plain_text', 'filter_url', '1', '1', 'filter', 'a:1:{s:17:"filter_url_length";i:72;}');
INSERT INTO d7_filter (format, name, weight, status, module, settings) VALUES ('plain_text', 'filter_autop', '2', '1', 'filter', 'a:0:{}');
INSERT INTO d7_filter (format, name, weight, status, module, settings) VALUES ('plain_text', 'filter_htmlcorrector', '10', '0', 'filter', 'a:0:{}');
INSERT INTO d7_filter (format, name, weight, status, module, settings) VALUES ('plain_text', 'filter_html_escape', '0', '1', 'filter', 'a:0:{}');
INSERT INTO d7_filter (format, name, weight, status, module, settings) VALUES ('plain_text', 'filter_html', '10', '0', 'filter', 'a:3:{s:12:"allowed_html";s:74:"<a> <em> <strong> <cite> <blockquote> <code> <ul> <ol> <li> <dl> <dt> <dd>";s:16:"filter_html_help";i:1;s:20:"filter_html_nofollow";i:0;}');
INSERT INTO d7_variable (name, value) VALUES ('filter_fallback_format', 's:10:"plain_text";');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '13528603224e29982b2fdbf9.30951523', '1311348780.1942');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FieldSqlStorageTestCase', 'class', 'modules/field/modules/field_sql_storage/field_sql_storage.test', 'field_sql_storage', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/field/modules/field_sql_storage/field_sql_storage.test', 'fa8f69ac79f4190a612faa6d1df3075be39571da2e897265c13e219dccf65b1c');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '13948105504e299838c4b607.67968046', '1311348793.804');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('NodeController', 'class', 'modules/node/node.module', 'node', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/node/node.module', '815c6861cbd0983e3e56cf7322ccaedc0de7cc1f46b2b6c8f9ea2f66a6e2d68b');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('NodeLoadMultipleUnitTest', 'class', 'modules/node/node.test', 'node', '0'), ('NodeLoadHooksTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeRevisionsTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('PageEditTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('PagePreviewTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeCreationTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('PageViewTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('SummaryLengthTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeTitleXSSTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeBlockTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodePostSettingsTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeRSSContentTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeAccessUnitTest', 'class', 'modules/node/node.test', 'node', '0'), ('NodeAccessRecordsUnitTest', 'class', 'modules/node/node.test', 'node', '0'), ('NodeAccessBaseTableTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeSaveTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeTypeTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeTypePersistenceTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeAccessRebuildTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeAdminTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeTitleTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeFeedTestCase', 'class', 'modules/node/node.test', 'node', '0'), ('NodeBlockFunctionalTest', 'class', 'modules/node/node.test', 'node', '0'), ('MultiStepNodeFormBasicOptionsTest', 'class', 'modules/node/node.test', 'node', '0'), ('NodeBuildContent', 'class', 'modules/node/node.test', 'node', '0'), ('NodeQueryAlter', 'class', 'modules/node/node.test', 'node', '0'), ('NodeEntityFieldQueryAlter', 'class', 'modules/node/node.test', 'node', '0'), ('NodeTokenReplaceTestCase', 'class', 'modules/node/node.test', 'node', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/node/node.test', 'e37213792452a8f98cd0cca66396d9de27464dcc53c50c9032d1225fd794f356');
INSERT INTO d7_node_access (nid, gid, realm, grant_view, grant_update, grant_delete) VALUES ('0', '0', 'all', '1', '0', '0');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '589947794e29985462fca9.97850267', '1311348821.4043');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FieldException', 'class', 'modules/field/field.module', 'field', '0'), ('FieldUpdateForbiddenException', 'class', 'modules/field/field.module', 'field', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/field/field.module', '19d5c7a30995064c426de50e51b8d7e2295fd5af5784a41cd94c00bcae7f9d4a');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FieldValidationException', 'class', 'modules/field/field.attach.inc', 'field', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/field/field.attach.inc', '65b6f010e3f9d1868098d771963113307b63fd6e6454c01f0086cd4c0de71414');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('FieldTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldAttachTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldAttachStorageTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldAttachOtherTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldInfoTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldFormTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldDisplayAPITestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldCrudTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldInstanceCrudTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldTranslationsTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('FieldBulkDeleteTestCase', 'class', 'modules/field/tests/field.test', 'field', '0'), ('EntityPropertiesTestCase', 'class', 'modules/field/tests/field.test', 'field', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/field/tests/field.test', '896003bafc6e1ccd0f6f32f96095318aadc9f6ee3c5436ce597dcaaa084dbcbe');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '8129432104e29986a7695f5.12489159', '1311348843.4845');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('TextFieldTestCase', 'class', 'modules/field/modules/text/text.test', 'text', '0'), ('TextSummaryTestCase', 'class', 'modules/field/modules/text/text.test', 'text', '0'), ('TextTranslationTestCase', 'class', 'modules/field/modules/text/text.test', 'text', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/field/modules/text/text.test', 'e4ad6c79d4f4e3f90c3ac960d2479ee65d51bda7ba93467bc0340bc32201cd11');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '14294613174e29987bae7188.98654661', '1311348860.7134');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('BlockTestCase', 'class', 'modules/block/block.test', 'block', '0'), ('NonDefaultBlockAdmin', 'class', 'modules/block/block.test', 'block', '0'), ('NewDefaultThemeBlocks', 'class', 'modules/block/block.test', 'block', '0'), ('BlockAdminThemeTestCase', 'class', 'modules/block/block.test', 'block', '0'), ('BlockCacheTestCase', 'class', 'modules/block/block.test', 'block', '0'), ('BlockHTMLIdTestCase', 'class', 'modules/block/block.test', 'block', '0'), ('BlockTemplateSuggestionsUnitTest', 'class', 'modules/block/block.test', 'block', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/block/block.test', 'f467ba7728986636a5e0a5b73e7db4819be04fc817ed4a675e0c9236f63a3b50');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '11340947494e2998919cb7e1.30424089', '1311348882.6407');
INSERT INTO d7_registry (name, type, filename, module, weight) VALUES ('DBLogTestCase', 'class', 'modules/dblog/dblog.test', 'dblog', '0');
INSERT INTO d7_registry_file (filename, hash) VALUES ('modules/dblog/dblog.test', 'c20371096cd2d2c1018e31bcc719f44e3cc31a2fdb75feb50944508380077259');
INSERT INTO d7_watchdog (uid, type, message, variables, severity, link, location, referer, hostname, timestamp) VALUES ('0', 'system', '%module module installed.', 'a:1:{s:7:"%module";s:5:"dblog";}', '6', '', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&id=1&op=do', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&op=start&id=1', '127.0.0.1', '1311348877');
INSERT INTO d7_watchdog (uid, type, message, variables, severity, link, location, referer, hostname, timestamp) VALUES ('0', 'system', '%module module enabled.', 'a:1:{s:7:"%module";s:5:"dblog";}', '6', '', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&id=1&op=do', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&op=start&id=1', '127.0.0.1', '1311348877');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '8037725394e2998a584f1b3.96730055', '1311348902.5434');
INSERT INTO d7_registry_file (filename, hash) VALUES ('profiles/minimal/minimal.profile', '293337f964f24fb7e503b200376c38bde411de1c38de154520eb414c44eba935');
INSERT INTO d7_block (module, delta, theme, status, weight, region, pages, cache) VALUES ('system', 'main', 'bartik', '1', '0', 'content', '', '-1'), ('user', 'login', 'bartik', '1', '0', 'sidebar_first', '', '-1'), ('system', 'navigation', 'bartik', '1', '0', 'sidebar_first', '', '-1'), ('system', 'management', 'bartik', '1', '1', 'sidebar_first', '', '-1'), ('system', 'help', 'bartik', '1', '0', 'help', '', '-1');
INSERT INTO d7_variable (name, value) VALUES ('user_register', 'i:2;');
INSERT INTO d7_role_permission (rid, permission, module) VALUES ('1', 'access content', 'node');
INSERT INTO d7_role_permission (rid, permission, module) VALUES ('2', 'access content', 'node');
INSERT INTO d7_watchdog (uid, type, message, variables, severity, link, location, referer, hostname, timestamp) VALUES ('0', 'system', '%module module installed.', 'a:1:{s:7:"%module";s:7:"minimal";}', '6', '', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&id=1&op=do', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&op=start&id=1', '127.0.0.1', '1311348899');
INSERT INTO d7_watchdog (uid, type, message, variables, severity, link, location, referer, hostname, timestamp) VALUES ('0', 'system', '%module module enabled.', 'a:1:{s:7:"%module";s:7:"minimal";}', '6', '', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&id=1&op=do', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&op=start&id=1', '127.0.0.1', '1311348899');
INSERT INTO d7_drupal_install_test (id) VALUES (1);
INSERT INTO d7_semaphore (name, value, expire) VALUES ('variable_init', '87318634e2998bb5199a1.76382437', '1311348924.3331');
INSERT INTO d7_variable (name, value) VALUES ('css_js_query_string', 's:6:"loqrav";');
INSERT INTO d7_semaphore (name, value, expire) VALUES ('menu_rebuild', '87318634e2998bb5199a1.76382437', '1311348965.5704');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('rss.xml', '', '', 'user_access', 'a:1:{i:0;s:14:"access content";}', 'node_feed', 'a:0:{}', '', '1', '1', '0', '', 'rss.xml', 'RSS feed', 't', '', '', 'a:0:{}', '0', '', '', '0', ''), ('node', '', '', 'user_access', 'a:1:{i:0;s:14:"access content";}', 'node_page_default', 'a:0:{}', '', '1', '1', '0', '', 'node', '', 't', '', '', 'a:0:{}', '0', '', '', '0', ''), ('batch', '', '', '1', 'a:0:{}', 'system_batch_page', 'a:0:{}', '', '1', '1', '0', '', 'batch', '', 't', '', '_system_batch_theme', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '1', '1', '0', '', 'admin', 'Administration', 't', '', '', 'a:0:{}', '6', '', '', '9', 'modules/system/system.admin.inc'), ('user', '', '', '1', 'a:0:{}', 'user_page', 'a:0:{}', '', '1', '1', '0', '', 'user', 'User account', 'user_menu_title', '', '', 'a:0:{}', '6', '', '', '-10', 'modules/user/user.pages.inc'), ('admin/tasks', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '3', '2', '1', 'admin', 'admin', 'Tasks', 't', '', '', 'a:0:{}', '140', '', '', '-20', 'modules/system/system.admin.inc'), ('user/login', '', '', 'user_is_anonymous', 'a:0:{}', 'user_page', 'a:0:{}', '', '3', '2', '1', 'user', 'user', 'Log in', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/user/user.pages.inc'), ('node/add', '', '', '_node_add_access', 'a:0:{}', 'node_add_page', 'a:0:{}', '', '3', '2', '0', '', 'node/add', 'Add content', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/node/node.pages.inc'), ('admin/compact', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_compact_page', 'a:0:{}', '', '3', '2', '0', '', 'admin/compact', 'Compact mode', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('filter/tips', '', '', '1', 'a:0:{}', 'filter_tips_long', 'a:0:{}', '', '3', '2', '0', '', 'filter/tips', 'Compose tips', 't', '', '', 'a:0:{}', '20', '', '', '0', 'modules/filter/filter.pages.inc'), ('user/register', '', '', 'user_register_access', 'a:0:{}', 'drupal_get_form', 'a:1:{i:0;s:18:"user_register_form";}', '', '3', '2', '1', 'user', 'user', 'Create new account', 't', '', '', 'a:0:{}', '132', '', '', '0', ''), ('system/files', '', '', '1', 'a:0:{}', 'file_download', 'a:1:{i:0;s:7:"private";}', '', '3', '2', '0', '', 'system/files', 'File download', 't', '', '', 'a:0:{}', '0', '', '', '0', ''), ('admin/index', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_index', 'a:0:{}', '', '3', '2', '1', 'admin', 'admin', 'Index', 't', '', '', 'a:0:{}', '132', '', '', '-18', 'modules/system/system.admin.inc'), ('system/temporary', '', '', '1', 'a:0:{}', 'file_download', 'a:1:{i:0;s:9:"temporary";}', '', '3', '2', '0', '', 'system/temporary', 'Temporary files', 't', '', '', 'a:0:{}', '0', '', '', '0', ''), ('system/timezone', '', '', '1', 'a:0:{}', 'system_timezone', 'a:0:{}', '', '3', '2', '0', '', 'system/timezone', 'Time zone', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_config_page', 'a:0:{}', '', '3', '2', '0', '', 'admin/config', 'Configuration', 't', '', '', 'a:0:{}', '6', 'Administer settings.', '', '0', 'modules/system/system.admin.inc'), ('user/logout', '', '', 'user_is_logged_in', 'a:0:{}', 'user_logout', 'a:0:{}', '', '3', '2', '0', '', 'user/logout', 'Log out', 't', '', '', 'a:0:{}', '6', '', '', '10', 'modules/user/user.pages.inc'), ('user/password', '', '', '1', 'a:0:{}', 'drupal_get_form', 'a:1:{i:0;s:9:"user_pass";}', '', '3', '2', '1', 'user', 'user', 'Request new password', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/user/user.pages.inc'), ('user/autocomplete', '', '', 'user_access', 'a:1:{i:0;s:20:"access user profiles";}', 'user_autocomplete', 'a:0:{}', '', '3', '2', '0', '', 'user/autocomplete', 'User autocomplete', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/user/user.pages.inc'), ('admin/appearance', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'system_themes_page', 'a:0:{}', '', '3', '2', '0', '', 'admin/appearance', 'Appearance', 't', '', '', 'a:0:{}', '6', 'Select and configure your themes.', 'left', '-6', 'modules/system/system.admin.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('admin/content', '', '', 'user_access', 'a:1:{i:0;s:23:"access content overview";}', 'drupal_get_form', 'a:1:{i:0;s:18:"node_admin_content";}', '', '3', '2', '0', '', 'admin/content', 'Content', 't', '', '', 'a:0:{}', '6', 'Find and manage content.', '', '-10', 'modules/node/node.admin.inc'), ('admin/modules', '', '', 'user_access', 'a:1:{i:0;s:18:"administer modules";}', 'drupal_get_form', 'a:1:{i:0;s:14:"system_modules";}', '', '3', '2', '0', '', 'admin/modules', 'Modules', 't', '', '', 'a:0:{}', '6', 'Enable or disable modules.', '', '-2', 'modules/system/system.admin.inc'), ('admin/reports', '', '', 'user_access', 'a:1:{i:0;s:19:"access site reports";}', 'system_admin_menu_block_page', 'a:0:{}', '', '3', '2', '0', '', 'admin/reports', 'Reports', 't', '', '', 'a:0:{}', '6', 'View reports, updates, and errors.', 'left', '5', 'modules/system/system.admin.inc'), ('admin/structure', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '3', '2', '0', '', 'admin/structure', 'Structure', 't', '', '', 'a:0:{}', '6', 'Administer blocks, content types, menus, etc.', 'right', '-8', 'modules/system/system.admin.inc'), ('system/ajax', '', '', '1', 'a:0:{}', 'ajax_form_callback', 'a:0:{}', 'ajax_deliver', '3', '2', '0', '', 'system/ajax', 'AHAH callback', 't', '', 'ajax_base_page_theme', 'a:0:{}', '0', '', '', '0', 'includes/form.inc'), ('node/%', 'a:1:{i:1;s:9:"node_load";}', '', 'node_access', 'a:2:{i:0;s:4:"view";i:1;i:1;}', 'node_page_view', 'a:1:{i:0;i:1;}', '', '2', '2', '0', '', 'node/%', '', 'node_page_title', 'a:1:{i:0;i:1;}', '', 'a:0:{}', '6', '', '', '0', ''), ('admin/people', '', '', 'user_access', 'a:1:{i:0;s:16:"administer users";}', 'user_admin', 'a:1:{i:0;s:4:"list";}', '', '3', '2', '0', '', 'admin/people', 'People', 't', '', '', 'a:0:{}', '6', 'Manage user accounts, roles, and permissions.', 'left', '-4', 'modules/user/user.admin.inc'), ('user/%', 'a:1:{i:1;s:9:"user_load";}', '', 'user_view_access', 'a:1:{i:0;i:1;}', 'user_view_page', 'a:1:{i:0;i:1;}', '', '2', '2', '0', '', 'user/%', 'My account', 'user_page_title', 'a:1:{i:0;i:1;}', '', 'a:0:{}', '6', '', '', '0', ''), ('admin/content/node', '', '', 'user_access', 'a:1:{i:0;s:23:"access content overview";}', 'drupal_get_form', 'a:1:{i:0;s:18:"node_admin_content";}', '', '7', '3', '1', 'admin/content', 'admin/content', 'Content', 't', '', '', 'a:0:{}', '140', '', '', '-10', 'modules/node/node.admin.inc'), ('admin/modules/list', '', '', 'user_access', 'a:1:{i:0;s:18:"administer modules";}', 'drupal_get_form', 'a:1:{i:0;s:14:"system_modules";}', '', '7', '3', '1', 'admin/modules', 'admin/modules', 'List', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/system/system.admin.inc'), ('node/%/view', 'a:1:{i:1;s:9:"node_load";}', '', 'node_access', 'a:2:{i:0;s:4:"view";i:1;i:1;}', 'node_page_view', 'a:1:{i:0;i:1;}', '', '5', '3', '1', 'node/%', 'node/%', 'View', 't', '', '', 'a:0:{}', '140', '', '', '-10', ''), ('user/%/view', 'a:1:{i:1;s:9:"user_load";}', '', 'user_view_access', 'a:1:{i:0;i:1;}', 'user_view_page', 'a:1:{i:0;i:1;}', '', '5', '3', '1', 'user/%', 'user/%', 'View', 't', '', '', 'a:0:{}', '140', '', '', '-10', ''), ('admin/people/create', '', '', 'user_access', 'a:1:{i:0;s:16:"administer users";}', 'user_admin', 'a:1:{i:0;s:6:"create";}', '', '7', '3', '1', 'admin/people', 'admin/people', 'Add user', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/user/user.admin.inc'), ('admin/appearance/list', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'system_themes_page', 'a:0:{}', '', '7', '3', '1', 'admin/appearance', 'admin/appearance', 'List', 't', '', '', 'a:0:{}', '140', 'Select and configure your theme', '', '-1', 'modules/system/system.admin.inc'), ('admin/appearance/disable', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'system_theme_disable', 'a:0:{}', '', '7', '3', '0', '', 'admin/appearance/disable', 'Disable theme', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/appearance/enable', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'system_theme_enable', 'a:0:{}', '', '7', '3', '0', '', 'admin/appearance/enable', 'Enable theme', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/people/people', '', '', 'user_access', 'a:1:{i:0;s:16:"administer users";}', 'user_admin', 'a:1:{i:0;s:4:"list";}', '', '7', '3', '1', 'admin/people', 'admin/people', 'List', 't', '', '', 'a:0:{}', '140', 'Find and manage people interacting with your site.', '', '-10', 'modules/user/user.admin.inc'), ('admin/appearance/default', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'system_theme_default', 'a:0:{}', '', '7', '3', '0', '', 'admin/appearance/default', 'Set default theme', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/modules/uninstall', '', '', 'user_access', 'a:1:{i:0;s:18:"administer modules";}', 'drupal_get_form', 'a:1:{i:0;s:24:"system_modules_uninstall";}', '', '7', '3', '1', 'admin/modules', 'admin/modules', 'Uninstall', 't', '', '', 'a:0:{}', '132', '', '', '20', 'modules/system/system.admin.inc'), ('admin/structure/types', '', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'node_overview_types', 'a:0:{}', '', '7', '3', '0', '', 'admin/structure/types', 'Content types', 't', '', '', 'a:0:{}', '6', 'Manage content types, including default status, front page promotion, comment settings, etc.', '', '0', 'modules/node/content_types.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('admin/reports/dblog', '', '', 'user_access', 'a:1:{i:0;s:19:"access site reports";}', 'dblog_overview', 'a:0:{}', '', '7', '3', '0', '', 'admin/reports/dblog', 'Recent log messages', 't', '', '', 'a:0:{}', '6', 'View events that have recently been logged.', '', '-1', 'modules/dblog/dblog.admin.inc'), ('admin/reports/status', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'system_status', 'a:0:{}', '', '7', '3', '0', '', 'admin/reports/status', 'Status report', 't', '', '', 'a:0:{}', '6', 'Get a status report about your site''s operation and any detected problems.', '', '-60', 'modules/system/system.admin.inc'), ('admin/structure/block', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'block_admin_display', 'a:1:{i:0;s:6:"bartik";}', '', '7', '3', '0', '', 'admin/structure/block', 'Blocks', 't', '', '', 'a:0:{}', '6', 'Configure what block content appears in your site''s sidebars and other regions.', '', '0', 'modules/block/block.admin.inc'), ('user/%/cancel', 'a:1:{i:1;s:9:"user_load";}', '', 'user_cancel_access', 'a:1:{i:0;i:1;}', 'drupal_get_form', 'a:2:{i:0;s:24:"user_cancel_confirm_form";i:1;i:1;}', '', '5', '3', '0', '', 'user/%/cancel', 'Cancel account', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/user/user.pages.inc'), ('admin/config/content', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/content', 'Content authoring', 't', '', '', 'a:0:{}', '6', 'Settings related to formatting and authoring content.', 'left', '-15', 'modules/system/system.admin.inc'), ('admin/config/development', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/development', 'Development', 't', '', '', 'a:0:{}', '6', 'Development tools.', 'right', '-10', 'modules/system/system.admin.inc'), ('user/%/edit', 'a:1:{i:1;s:9:"user_load";}', '', 'user_edit_access', 'a:1:{i:0;i:1;}', 'drupal_get_form', 'a:2:{i:0;s:17:"user_profile_form";i:1;i:1;}', '', '5', '3', '1', 'user/%', 'user/%', 'Edit', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/user/user.pages.inc'), ('admin/config/media', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/media', 'Media', 't', '', '', 'a:0:{}', '6', 'Media tools.', 'left', '-10', 'modules/system/system.admin.inc'), ('admin/people/permissions', '', '', 'user_access', 'a:1:{i:0;s:22:"administer permissions";}', 'drupal_get_form', 'a:1:{i:0;s:22:"user_admin_permissions";}', '', '7', '3', '1', 'admin/people', 'admin/people', 'Permissions', 't', '', '', 'a:0:{}', '132', 'Determine access to features by selecting permissions for roles.', '', '0', 'modules/user/user.admin.inc'), ('admin/config/regional', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/regional', 'Regional and language', 't', '', '', 'a:0:{}', '6', 'Regional settings, localization and translation.', 'left', '-5', 'modules/system/system.admin.inc'), ('node/%/revisions', 'a:1:{i:1;s:9:"node_load";}', '', '_node_revision_access', 'a:1:{i:0;i:1;}', 'node_revision_overview', 'a:1:{i:0;i:1;}', '', '5', '3', '1', 'node/%', 'node/%', 'Revisions', 't', '', '', 'a:0:{}', '132', '', '', '2', 'modules/node/node.pages.inc'), ('admin/config/search', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/search', 'Search and metadata', 't', '', '', 'a:0:{}', '6', 'Local site search, metadata and SEO.', 'left', '-10', 'modules/system/system.admin.inc'), ('admin/appearance/settings', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'drupal_get_form', 'a:1:{i:0;s:21:"system_theme_settings";}', '', '7', '3', '1', 'admin/appearance', 'admin/appearance', 'Settings', 't', '', '', 'a:0:{}', '132', 'Configure default and theme specific settings.', '', '20', 'modules/system/system.admin.inc'), ('admin/config/system', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/system', 'System', 't', '', '', 'a:0:{}', '6', 'General system related configuration.', 'right', '-20', 'modules/system/system.admin.inc'), ('admin/reports/access-denied', '', '', 'user_access', 'a:1:{i:0;s:19:"access site reports";}', 'dblog_top', 'a:1:{i:0;s:13:"access denied";}', '', '7', '3', '0', '', 'admin/reports/access-denied', 'Top ''access denied'' errors', 't', '', '', 'a:0:{}', '6', 'View ''access denied'' errors (403s).', '', '0', 'modules/dblog/dblog.admin.inc'), ('admin/reports/page-not-found', '', '', 'user_access', 'a:1:{i:0;s:19:"access site reports";}', 'dblog_top', 'a:1:{i:0;s:14:"page not found";}', '', '7', '3', '0', '', 'admin/reports/page-not-found', 'Top ''page not found'' errors', 't', '', '', 'a:0:{}', '6', 'View ''page not found'' errors (404s).', '', '0', 'modules/dblog/dblog.admin.inc'), ('admin/config/user-interface', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/user-interface', 'User interface', 't', '', '', 'a:0:{}', '6', 'Tools that enhance the user interface.', 'right', '-15', 'modules/system/system.admin.inc'), ('admin/config/services', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/services', 'Web services', 't', '', '', 'a:0:{}', '6', 'Tools related to web services.', 'right', '0', 'modules/system/system.admin.inc'), ('admin/config/workflow', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/workflow', 'Workflow', 't', '', '', 'a:0:{}', '6', 'Content workflow, editorial workflow tools.', 'right', '5', 'modules/system/system.admin.inc'), ('node/%/delete', 'a:1:{i:1;s:9:"node_load";}', '', 'node_access', 'a:2:{i:0;s:6:"delete";i:1;i:1;}', 'drupal_get_form', 'a:2:{i:0;s:19:"node_delete_confirm";i:1;i:1;}', '', '5', '3', '2', 'node/%', 'node/%', 'Delete', 't', '', '', 'a:0:{}', '132', '', '', '1', 'modules/node/node.pages.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('node/%/edit', 'a:1:{i:1;s:9:"node_load";}', '', 'node_access', 'a:2:{i:0;s:6:"update";i:1;i:1;}', 'node_page_edit', 'a:1:{i:0;i:1;}', '', '5', '3', '3', 'node/%', 'node/%', 'Edit', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/node/node.pages.inc'), ('admin/config/people', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'system_admin_menu_block_page', 'a:0:{}', '', '7', '3', '0', '', 'admin/config/people', 'People', 't', '', '', 'a:0:{}', '6', 'Configure user accounts.', 'left', '-20', 'modules/system/system.admin.inc'), ('admin/appearance/settings/global', '', '', 'user_access', 'a:1:{i:0;s:17:"administer themes";}', 'drupal_get_form', 'a:1:{i:0;s:21:"system_theme_settings";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Global settings', 't', '', '', 'a:0:{}', '140', '', '', '-1', 'modules/system/system.admin.inc'), ('admin/structure/types/list', '', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'node_overview_types', 'a:0:{}', '', '15', '4', '1', 'admin/structure/types', 'admin/structure/types', 'List', 't', '', '', 'a:0:{}', '140', '', '', '-10', 'modules/node/content_types.inc'), ('user/%/edit/account', 'a:1:{i:1;a:1:{s:18:"user_category_load";a:2:{i:0;s:4:"%map";i:1;s:6:"%index";}}}', '', 'user_edit_access', 'a:1:{i:0;i:1;}', 'drupal_get_form', 'a:2:{i:0;s:17:"user_profile_form";i:1;i:1;}', '', '11', '4', '1', 'user/%/edit', 'user/%', 'Account', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/user/user.pages.inc'), ('admin/modules/list/confirm', '', '', 'user_access', 'a:1:{i:0;s:18:"administer modules";}', 'drupal_get_form', 'a:1:{i:0;s:14:"system_modules";}', '', '15', '4', '0', '', 'admin/modules/list/confirm', 'List', 't', '', '', 'a:0:{}', '4', '', '', '0', 'modules/system/system.admin.inc'), ('admin/people/permissions/list', '', '', 'user_access', 'a:1:{i:0;s:22:"administer permissions";}', 'drupal_get_form', 'a:1:{i:0;s:22:"user_admin_permissions";}', '', '15', '4', '1', 'admin/people/permissions', 'admin/people', 'Permissions', 't', '', '', 'a:0:{}', '140', 'Determine access to features by selecting permissions for roles.', '', '-8', 'modules/user/user.admin.inc'), ('admin/modules/uninstall/confirm', '', '', 'user_access', 'a:1:{i:0;s:18:"administer modules";}', 'drupal_get_form', 'a:1:{i:0;s:24:"system_modules_uninstall";}', '', '15', '4', '0', '', 'admin/modules/uninstall/confirm', 'Uninstall', 't', '', '', 'a:0:{}', '4', '', '', '0', 'modules/system/system.admin.inc'), ('admin/reports/status/php', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'system_php', 'a:0:{}', '', '15', '4', '0', '', 'admin/reports/status/php', 'PHP', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/reports/status/run-cron', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'system_run_cron', 'a:0:{}', '', '15', '4', '0', '', 'admin/reports/status/run-cron', 'Run cron', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/system/actions', '', '', 'user_access', 'a:1:{i:0;s:18:"administer actions";}', 'system_actions_manage', 'a:0:{}', '', '15', '4', '0', '', 'admin/config/system/actions', 'Actions', 't', '', '', 'a:0:{}', '6', 'Manage the actions defined for your site.', '', '0', 'modules/system/system.admin.inc'), ('admin/structure/block/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '15', '4', '1', 'admin/structure/block', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/types/add', '', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'drupal_get_form', 'a:1:{i:0;s:14:"node_type_form";}', '', '15', '4', '1', 'admin/structure/types', 'admin/structure/types', 'Add content type', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/node/content_types.inc'), ('admin/appearance/settings/bartik', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:25:"themes/bartik/bartik.info";s:8:"filename";s:25:"themes/bartik/bartik.info";s:4:"name";s:6:"bartik";s:4:"info";a:16:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:6:"bartik";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Bartik', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/reports/event/%', 'a:1:{i:3;N;}', '', 'user_access', 'a:1:{i:0;s:19:"access site reports";}', 'dblog_event', 'a:1:{i:0;i:3;}', '', '14', '4', '0', '', 'admin/reports/event/%', 'Details', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/dblog/dblog.admin.inc'), ('admin/appearance/settings/garland', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:27:"themes/garland/garland.info";s:8:"filename";s:27:"themes/garland/garland.info";s:4:"name";s:7:"garland";s:4:"info";a:16:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:7:"garland";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Garland', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/people/ip-blocking', '', '', 'user_access', 'a:1:{i:0;s:18:"block IP addresses";}', 'system_ip_blocking', 'a:0:{}', '', '15', '4', '0', '', 'admin/config/people/ip-blocking', 'IP address blocking', 't', '', '', 'a:0:{}', '6', 'Manage blocked IP addresses.', '', '10', 'modules/system/system.admin.inc'), ('admin/reports/status/rebuild', '', '', 'user_access', 'a:1:{i:0;s:27:"access administration pages";}', 'drupal_get_form', 'a:1:{i:0;s:30:"node_configure_rebuild_confirm";}', '', '15', '4', '0', '', 'admin/reports/status/rebuild', 'Rebuild permissions', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/node/node.admin.inc'), ('admin/appearance/settings/seven', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/seven/seven.info";s:8:"filename";s:23:"themes/seven/seven.info";s:4:"name";s:5:"seven";s:4:"info";a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:3:{i:0;s:13:"sidebar_first";i:1;s:8:"page_top";i:2;s:11:"page_bottom";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:5:"seven";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Seven', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/appearance/settings/stark', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/stark/stark.info";s:8:"filename";s:23:"themes/stark/stark.info";s:4:"name";s:5:"stark";s:4:"info";a:15:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:5:"stark";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Stark', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('admin/appearance/settings/test_theme', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:39:"themes/tests/test_theme/test_theme.info";s:8:"filename";s:39:"themes/tests/test_theme/test_theme.info";s:4:"name";s:10:"test_theme";s:4:"info";a:15:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:10:"test_theme";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Test theme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/appearance/settings/update_test_basetheme', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:8:"filename";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:4:"name";s:21:"update_test_basetheme";s:4:"info";a:15:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:10:"sub_themes";a:1:{s:20:"update_test_subtheme";s:20:"Update test subtheme";}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:21:"update_test_basetheme";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Update test base theme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/appearance/settings/update_test_subtheme', '', '', '_system_themes_access', 'a:1:{i:0;O:8:"stdClass":11:{s:3:"uri";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:8:"filename";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:4:"name";s:20:"update_test_subtheme";s:4:"info";a:16:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"base_themes";a:1:{s:21:"update_test_basetheme";s:22:"Update test base theme";}s:6:"engine";s:11:"phptemplate";s:10:"base_theme";s:21:"update_test_basetheme";s:6:"status";i:0;}}', 'drupal_get_form', 'a:2:{i:0;s:21:"system_theme_settings";i:1;s:20:"update_test_subtheme";}', '', '15', '4', '1', 'admin/appearance/settings', 'admin/appearance', 'Update test subtheme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/people/accounts', '', '', 'user_access', 'a:1:{i:0;s:16:"administer users";}', 'drupal_get_form', 'a:1:{i:0;s:19:"user_admin_settings";}', '', '15', '4', '0', '', 'admin/config/people/accounts', 'Account settings', 't', '', '', 'a:0:{}', '6', 'Configure default behavior of users, including registration requirements, e-mails, fields, and user pictures.', '', '-10', 'modules/user/user.admin.inc'), ('admin/config/search/clean-urls', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:25:"system_clean_url_settings";}', '', '15', '4', '0', '', 'admin/config/search/clean-urls', 'Clean URLs', 't', '', '', 'a:0:{}', '6', 'Enable or disable clean URLs for your site.', '', '5', 'modules/system/system.admin.inc'), ('admin/config/system/cron', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:20:"system_cron_settings";}', '', '15', '4', '0', '', 'admin/config/system/cron', 'Cron', 't', '', '', 'a:0:{}', '6', 'Manage automatic site maintenance tasks.', '', '20', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:25:"system_date_time_settings";}', '', '15', '4', '0', '', 'admin/config/regional/date-time', 'Date and time', 't', '', '', 'a:0:{}', '6', 'Configure display formats for date and time.', '', '-15', 'modules/system/system.admin.inc'), ('admin/config/media/file-system', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:27:"system_file_system_settings";}', '', '15', '4', '0', '', 'admin/config/media/file-system', 'File system', 't', '', '', 'a:0:{}', '6', 'Tell Drupal where to store uploaded files and how they are accessed.', '', '-10', 'modules/system/system.admin.inc'), ('admin/config/media/image-toolkit', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:29:"system_image_toolkit_settings";}', '', '15', '4', '0', '', 'admin/config/media/image-toolkit', 'Image toolkit', 't', '', '', 'a:0:{}', '6', 'Choose which image toolkit to use if you have installed optional toolkits.', '', '20', 'modules/system/system.admin.inc'), ('admin/config/development/logging', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:23:"system_logging_settings";}', '', '15', '4', '0', '', 'admin/config/development/logging', 'Logging and errors', 't', '', '', 'a:0:{}', '6', 'Settings for logging and alerts modules. Various modules can route Drupal''s system events to different destinations, such as syslog, database, email, etc.', '', '-15', 'modules/system/system.admin.inc'), ('admin/config/development/maintenance', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:28:"system_site_maintenance_mode";}', '', '15', '4', '0', '', 'admin/config/development/maintenance', 'Maintenance mode', 't', '', '', 'a:0:{}', '6', 'Take the site offline for maintenance or bring it back online.', '', '-10', 'modules/system/system.admin.inc'), ('admin/config/development/performance', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:27:"system_performance_settings";}', '', '15', '4', '0', '', 'admin/config/development/performance', 'Performance', 't', '', '', 'a:0:{}', '6', 'Enable or disable page caching for anonymous users and set CSS and JS bandwidth optimization options.', '', '-20', 'modules/system/system.admin.inc'), ('admin/config/services/rss-publishing', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:25:"system_rss_feeds_settings";}', '', '15', '4', '0', '', 'admin/config/services/rss-publishing', 'RSS publishing', 't', '', '', 'a:0:{}', '6', 'Configure the site description, the number of items per feed and whether feeds should be titles/teasers/full-text.', '', '0', 'modules/system/system.admin.inc'), ('admin/config/regional/settings', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:24:"system_regional_settings";}', '', '15', '4', '0', '', 'admin/config/regional/settings', 'Regional settings', 't', '', '', 'a:0:{}', '6', 'Settings for the site''s default time zone and country.', '', '-20', 'modules/system/system.admin.inc'), ('admin/people/permissions/roles', '', '', 'user_access', 'a:1:{i:0;s:22:"administer permissions";}', 'drupal_get_form', 'a:1:{i:0;s:16:"user_admin_roles";}', '', '15', '4', '1', 'admin/people/permissions', 'admin/people', 'Roles', 't', '', '', 'a:0:{}', '132', 'List, edit, or add user roles.', '', '-5', 'modules/user/user.admin.inc'), ('admin/config/system/site-information', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:32:"system_site_information_settings";}', '', '15', '4', '0', '', 'admin/config/system/site-information', 'Site information', 't', '', '', 'a:0:{}', '6', 'Change site name, e-mail address, slogan, default front page, and number of posts per page, error pages.', '', '-20', 'modules/system/system.admin.inc'), ('admin/config/content/formats', '', '', 'user_access', 'a:1:{i:0;s:18:"administer filters";}', 'drupal_get_form', 'a:1:{i:0;s:21:"filter_admin_overview";}', '', '15', '4', '0', '', 'admin/config/content/formats', 'Text formats', 't', '', '', 'a:0:{}', '6', 'Configure how content input by users is filtered, including allowed HTML tags. Also allows enabling of module-provided filters.', '', '0', 'modules/filter/filter.admin.inc'), ('admin/config/content/formats/list', '', '', 'user_access', 'a:1:{i:0;s:18:"administer filters";}', 'drupal_get_form', 'a:1:{i:0;s:21:"filter_admin_overview";}', '', '31', '5', '1', 'admin/config/content/formats', 'admin/config/content/formats', 'List', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/filter/filter.admin.inc'), ('admin/config/people/accounts/settings', '', '', 'user_access', 'a:1:{i:0;s:16:"administer users";}', 'drupal_get_form', 'a:1:{i:0;s:19:"user_admin_settings";}', '', '31', '5', '1', 'admin/config/people/accounts', 'admin/config/people/accounts', 'Settings', 't', '', '', 'a:0:{}', '140', '', '', '-10', 'modules/user/user.admin.inc'), ('admin/config/content/formats/add', '', '', 'user_access', 'a:1:{i:0;s:18:"administer filters";}', 'filter_admin_format_page', 'a:0:{}', '', '31', '5', '1', 'admin/config/content/formats', 'admin/config/content/formats', 'Add text format', 't', '', '', 'a:0:{}', '388', '', '', '1', 'modules/filter/filter.admin.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('admin/config/system/actions/manage', '', '', 'user_access', 'a:1:{i:0;s:18:"administer actions";}', 'system_actions_manage', 'a:0:{}', '', '31', '5', '1', 'admin/config/system/actions', 'admin/config/system/actions', 'Manage actions', 't', '', '', 'a:0:{}', '140', 'Manage the actions defined for your site.', '', '-2', 'modules/system/system.admin.inc'), ('admin/config/system/actions/orphan', '', '', 'user_access', 'a:1:{i:0;s:18:"administer actions";}', 'system_actions_remove_orphans', 'a:0:{}', '', '31', '5', '0', '', 'admin/config/system/actions/orphan', 'Remove orphans', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/structure/block/list/bartik', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:25:"themes/bartik/bartik.info";s:8:"filename";s:25:"themes/bartik/bartik.info";s:4:"name";s:6:"bartik";s:4:"info";a:16:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:6:"bartik";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Bartik', 't', '', '', 'a:0:{}', '140', '', '', '-10', 'modules/block/block.admin.inc'), ('admin/config/search/clean-urls/check', '', '', '1', 'a:0:{}', 'drupal_json_output', 'a:1:{i:0;a:1:{s:6:"status";b:1;}}', '', '31', '5', '0', '', 'admin/config/search/clean-urls/check', 'Clean URL check', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/system/actions/configure', '', '', 'user_access', 'a:1:{i:0;s:18:"administer actions";}', 'drupal_get_form', 'a:1:{i:0;s:24:"system_actions_configure";}', '', '31', '5', '0', '', 'admin/config/system/actions/configure', 'Configure an advanced action', 't', '', '', 'a:0:{}', '4', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time/formats', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'system_date_time_formats', 'a:0:{}', '', '31', '5', '1', 'admin/config/regional/date-time', 'admin/config/regional/date-time', 'Formats', 't', '', '', 'a:0:{}', '132', 'Configure display format strings for date and time.', '', '-9', 'modules/system/system.admin.inc'), ('admin/structure/block/list/garland', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:27:"themes/garland/garland.info";s:8:"filename";s:27:"themes/garland/garland.info";s:4:"name";s:7:"garland";s:4:"info";a:16:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:7:"garland";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Garland', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('user/reset/%/%/%', 'a:3:{i:2;N;i:3;N;i:4;N;}', '', '1', 'a:0:{}', 'drupal_get_form', 'a:4:{i:0;s:15:"user_pass_reset";i:1;i:2;i:2;i:3;i:3;i:4;}', '', '24', '5', '0', '', 'user/reset/%/%/%', 'Reset password', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/user/user.pages.inc'), ('admin/structure/block/list/seven', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/seven/seven.info";s:8:"filename";s:23:"themes/seven/seven.info";s:4:"name";s:5:"seven";s:4:"info";a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:3:{i:0;s:13:"sidebar_first";i:1;s:8:"page_top";i:2;s:11:"page_bottom";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:5:"seven";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Seven', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/stark', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/stark/stark.info";s:8:"filename";s:23:"themes/stark/stark.info";s:4:"name";s:5:"stark";s:4:"info";a:15:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:5:"stark";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Stark', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/test_theme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:39:"themes/tests/test_theme/test_theme.info";s:8:"filename";s:39:"themes/tests/test_theme/test_theme.info";s:4:"name";s:10:"test_theme";s:4:"info";a:15:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:10:"test_theme";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Test theme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/update_test_basetheme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:8:"filename";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:4:"name";s:21:"update_test_basetheme";s:4:"info";a:15:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:10:"sub_themes";a:1:{s:20:"update_test_subtheme";s:20:"Update test subtheme";}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:21:"update_test_basetheme";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Update test base theme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/update_test_subtheme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":11:{s:3:"uri";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:8:"filename";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:4:"name";s:20:"update_test_subtheme";s:4:"info";a:16:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"base_themes";a:1:{s:21:"update_test_basetheme";s:22:"Update test base theme";}s:6:"engine";s:11:"phptemplate";s:10:"base_theme";s:21:"update_test_basetheme";s:6:"status";i:0;}}', 'block_admin_display', 'a:1:{i:0;s:20:"update_test_subtheme";}', '', '31', '5', '1', 'admin/structure/block', 'admin/structure/block', 'Update test subtheme', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('node/%/revisions/%/view', 'a:2:{i:1;a:1:{s:9:"node_load";a:1:{i:0;i:3;}}i:3;N;}', '', '_node_revision_access', 'a:1:{i:0;i:1;}', 'node_show', 'a:2:{i:0;i:1;i:1;b:1;}', '', '21', '5', '0', '', 'node/%/revisions/%/view', 'Revisions', 't', '', '', 'a:0:{}', '6', '', '', '0', ''), ('admin/config/regional/date-time/types', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:25:"system_date_time_settings";}', '', '31', '5', '1', 'admin/config/regional/date-time', 'admin/config/regional/date-time', 'Types', 't', '', '', 'a:0:{}', '140', 'Configure display formats for date and time.', '', '-10', 'modules/system/system.admin.inc'), ('node/%/revisions/%/delete', 'a:2:{i:1;a:1:{s:9:"node_load";a:1:{i:0;i:3;}}i:3;N;}', '', '_node_revision_access', 'a:2:{i:0;i:1;i:1;s:6:"delete";}', 'drupal_get_form', 'a:2:{i:0;s:28:"node_revision_delete_confirm";i:1;i:1;}', '', '21', '5', '0', '', 'node/%/revisions/%/delete', 'Delete earlier revision', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/node/node.pages.inc'), ('admin/structure/types/manage/%', 'a:1:{i:4;s:14:"node_type_load";}', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'drupal_get_form', 'a:2:{i:0;s:14:"node_type_form";i:1;i:4;}', '', '30', '5', '0', '', 'admin/structure/types/manage/%', 'Edit content type', 'node_type_page_title', 'a:1:{i:0;i:4;}', '', 'a:0:{}', '6', '', '', '0', 'modules/node/content_types.inc'), ('node/%/revisions/%/revert', 'a:2:{i:1;a:1:{s:9:"node_load";a:1:{i:0;i:3;}}i:3;N;}', '', '_node_revision_access', 'a:2:{i:0;i:1;i:1;s:6:"update";}', 'drupal_get_form', 'a:2:{i:0;s:28:"node_revision_revert_confirm";i:1;i:1;}', '', '21', '5', '0', '', 'node/%/revisions/%/revert', 'Revert to earlier revision', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/node/node.pages.inc'), ('admin/config/content/formats/%', 'a:1:{i:4;s:18:"filter_format_load";}', '', 'user_access', 'a:1:{i:0;s:18:"administer filters";}', 'filter_admin_format_page', 'a:1:{i:0;i:4;}', '', '30', '5', '0', '', 'admin/config/content/formats/%', '', 'filter_admin_format_title', 'a:1:{i:0;i:4;}', '', 'a:0:{}', '6', '', '', '0', 'modules/filter/filter.admin.inc'), ('admin/structure/block/demo/bartik', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:25:"themes/bartik/bartik.info";s:8:"filename";s:25:"themes/bartik/bartik.info";s:4:"name";s:6:"bartik";s:4:"info";a:16:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:6:"bartik";}', '', '31', '5', '0', '', 'admin/structure/block/demo/bartik', 'Bartik', 't', '', '_block_custom_theme', 'a:1:{i:0;s:6:"bartik";}', '0', '', '', '0', 'modules/block/block.admin.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('admin/structure/block/demo/garland', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:27:"themes/garland/garland.info";s:8:"filename";s:27:"themes/garland/garland.info";s:4:"name";s:7:"garland";s:4:"info";a:16:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:7:"garland";}', '', '31', '5', '0', '', 'admin/structure/block/demo/garland', 'Garland', 't', '', '_block_custom_theme', 'a:1:{i:0;s:7:"garland";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/demo/seven', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/seven/seven.info";s:8:"filename";s:23:"themes/seven/seven.info";s:4:"name";s:5:"seven";s:4:"info";a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:3:{i:0;s:13:"sidebar_first";i:1;s:8:"page_top";i:2;s:11:"page_bottom";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:5:"seven";}', '', '31', '5', '0', '', 'admin/structure/block/demo/seven', 'Seven', 't', '', '_block_custom_theme', 'a:1:{i:0;s:5:"seven";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/demo/stark', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:23:"themes/stark/stark.info";s:8:"filename";s:23:"themes/stark/stark.info";s:4:"name";s:5:"stark";s:4:"info";a:15:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:5:"stark";}', '', '31', '5', '0', '', 'admin/structure/block/demo/stark', 'Stark', 't', '', '_block_custom_theme', 'a:1:{i:0;s:5:"stark";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/demo/test_theme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:39:"themes/tests/test_theme/test_theme.info";s:8:"filename";s:39:"themes/tests/test_theme/test_theme.info";s:4:"name";s:10:"test_theme";s:4:"info";a:15:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:10:"test_theme";}', '', '31', '5', '0', '', 'admin/structure/block/demo/test_theme', 'Test theme', 't', '', '_block_custom_theme', 'a:1:{i:0;s:10:"test_theme";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/demo/update_test_basetheme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":10:{s:3:"uri";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:8:"filename";s:61:"themes/tests/update_test_basetheme/update_test_basetheme.info";s:4:"name";s:21:"update_test_basetheme";s:4:"info";a:15:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:10:"sub_themes";a:1:{s:20:"update_test_subtheme";s:20:"Update test subtheme";}s:6:"engine";s:11:"phptemplate";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:21:"update_test_basetheme";}', '', '31', '5', '0', '', 'admin/structure/block/demo/update_test_basetheme', 'Update test base theme', 't', '', '_block_custom_theme', 'a:1:{i:0;s:21:"update_test_basetheme";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/demo/update_test_subtheme', '', '', '_block_themes_access', 'a:1:{i:0;O:8:"stdClass":11:{s:3:"uri";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:8:"filename";s:59:"themes/tests/update_test_subtheme/update_test_subtheme.info";s:4:"name";s:20:"update_test_subtheme";s:4:"info";a:16:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}s:5:"owner";s:45:"themes/engines/phptemplate/phptemplate.engine";s:6:"prefix";s:11:"phptemplate";s:8:"template";b:1;s:11:"base_themes";a:1:{s:21:"update_test_basetheme";s:22:"Update test base theme";}s:6:"engine";s:11:"phptemplate";s:10:"base_theme";s:21:"update_test_basetheme";s:6:"status";i:0;}}', 'block_admin_demo', 'a:1:{i:0;s:20:"update_test_subtheme";}', '', '31', '5', '0', '', 'admin/structure/block/demo/update_test_subtheme', 'Update test subtheme', 't', '', '_block_custom_theme', 'a:1:{i:0;s:20:"update_test_subtheme";}', '0', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/types/manage/%/edit', 'a:1:{i:4;s:14:"node_type_load";}', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'drupal_get_form', 'a:2:{i:0;s:14:"node_type_form";i:1;i:4;}', '', '61', '6', '1', 'admin/structure/types/manage/%', 'admin/structure/types/manage/%', 'Edit', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/node/content_types.inc'), ('admin/config/regional/date-time/formats/lookup', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'system_date_time_lookup', 'a:0:{}', '', '63', '6', '0', '', 'admin/config/regional/date-time/formats/lookup', 'Date and time lookup', 't', '', '', 'a:0:{}', '0', '', '', '0', 'modules/system/system.admin.inc'), ('admin/structure/types/manage/%/delete', 'a:1:{i:4;s:14:"node_type_load";}', '', 'user_access', 'a:1:{i:0;s:24:"administer content types";}', 'drupal_get_form', 'a:2:{i:0;s:24:"node_type_delete_confirm";i:1;i:4;}', '', '61', '6', '0', '', 'admin/structure/types/manage/%/delete', 'Delete', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/node/content_types.inc'), ('admin/people/permissions/roles/edit/%', 'a:1:{i:5;s:14:"user_role_load";}', '', 'user_role_edit_access', 'a:1:{i:0;i:5;}', 'drupal_get_form', 'a:2:{i:0;s:15:"user_admin_role";i:1;i:5;}', '', '62', '6', '0', '', 'admin/people/permissions/roles/edit/%', 'Edit role', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/user/user.admin.inc'), ('admin/structure/block/list/garland/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/garland', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/seven/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/seven', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/stark/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/stark', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/test_theme/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/test_theme', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/update_test_basetheme/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/update_test_basetheme', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/list/update_test_subtheme/add', '', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:1:{i:0;s:20:"block_add_block_form";}', '', '63', '6', '1', 'admin/structure/block/list/update_test_subtheme', 'admin/structure/block', 'Add block', 't', '', '', 'a:0:{}', '388', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/manage/%/%', 'a:2:{i:4;N;i:5;N;}', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:3:{i:0;s:21:"block_admin_configure";i:1;i:4;i:2;i:5;}', '', '60', '6', '0', '', 'admin/structure/block/manage/%/%', 'Configure block', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/block/block.admin.inc'), ('admin/config/people/ip-blocking/delete/%', 'a:1:{i:5;s:15:"blocked_ip_load";}', '', 'user_access', 'a:1:{i:0;s:18:"block IP addresses";}', 'drupal_get_form', 'a:2:{i:0;s:25:"system_ip_blocking_delete";i:1;i:5;}', '', '62', '6', '0', '', 'admin/config/people/ip-blocking/delete/%', 'Delete IP address', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time/types/add', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:32:"system_add_date_format_type_form";}', '', '63', '6', '1', 'admin/config/regional/date-time/types', 'admin/config/regional/date-time', 'Add date type', 't', '', '', 'a:0:{}', '388', 'Add new date type.', '', '-10', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time/formats/add', '', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:1:{i:0;s:34:"system_configure_date_formats_form";}', '', '63', '6', '1', 'admin/config/regional/date-time/formats', 'admin/config/regional/date-time', 'Add format', 't', '', '', 'a:0:{}', '388', 'Allow users to add additional date formats.', '', '-10', 'modules/system/system.admin.inc');
INSERT INTO d7_menu_router (path, load_functions, to_arg_functions, access_callback, access_arguments, page_callback, page_arguments, delivery_callback, fit, number_parts, context, tab_parent, tab_root, title, title_callback, title_arguments, theme_callback, theme_arguments, type, description, position, weight, include_file) VALUES ('user/%/cancel/confirm/%/%', 'a:3:{i:1;s:9:"user_load";i:4;N;i:5;N;}', '', 'user_cancel_access', 'a:1:{i:0;i:1;}', 'user_cancel_confirm', 'a:3:{i:0;i:1;i:1;i:4;i:2;i:5;}', '', '44', '6', '0', '', 'user/%/cancel/confirm/%/%', 'Confirm account cancellation', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/user/user.pages.inc'), ('admin/config/system/actions/delete/%', 'a:1:{i:5;s:12:"actions_load";}', '', 'user_access', 'a:1:{i:0;s:18:"administer actions";}', 'drupal_get_form', 'a:2:{i:0;s:26:"system_actions_delete_form";i:1;i:5;}', '', '62', '6', '0', '', 'admin/config/system/actions/delete/%', 'Delete action', 't', '', '', 'a:0:{}', '6', 'Delete an action.', '', '0', 'modules/system/system.admin.inc'), ('admin/people/permissions/roles/delete/%', 'a:1:{i:5;s:14:"user_role_load";}', '', 'user_role_edit_access', 'a:1:{i:0;i:5;}', 'drupal_get_form', 'a:2:{i:0;s:30:"user_admin_role_delete_confirm";i:1;i:5;}', '', '62', '6', '0', '', 'admin/people/permissions/roles/delete/%', 'Delete role', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/user/user.admin.inc'), ('admin/config/content/formats/%/disable', 'a:1:{i:4;s:18:"filter_format_load";}', '', '_filter_disable_format_access', 'a:1:{i:0;i:4;}', 'drupal_get_form', 'a:2:{i:0;s:20:"filter_admin_disable";i:1;i:4;}', '', '61', '6', '0', '', 'admin/config/content/formats/%/disable', 'Disable text format', 't', '', '', 'a:0:{}', '6', '', '', '0', 'modules/filter/filter.admin.inc'), ('admin/structure/block/manage/%/%/configure', 'a:2:{i:4;N;i:5;N;}', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:3:{i:0;s:21:"block_admin_configure";i:1;i:4;i:2;i:5;}', '', '121', '7', '2', 'admin/structure/block/manage/%/%', 'admin/structure/block/manage/%/%', 'Configure block', 't', '', '', 'a:0:{}', '140', '', '', '0', 'modules/block/block.admin.inc'), ('admin/structure/block/manage/%/%/delete', 'a:2:{i:4;N;i:5;N;}', '', 'user_access', 'a:1:{i:0;s:17:"administer blocks";}', 'drupal_get_form', 'a:3:{i:0;s:25:"block_custom_block_delete";i:1;i:4;i:2;i:5;}', '', '121', '7', '0', 'admin/structure/block/manage/%/%', 'admin/structure/block/manage/%/%', 'Delete block', 't', '', '', 'a:0:{}', '132', '', '', '0', 'modules/block/block.admin.inc'), ('admin/config/regional/date-time/formats/%/delete', 'a:1:{i:5;N;}', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:2:{i:0;s:30:"system_date_delete_format_form";i:1;i:5;}', '', '125', '7', '0', '', 'admin/config/regional/date-time/formats/%/delete', 'Delete date format', 't', '', '', 'a:0:{}', '6', 'Allow users to delete a configured date format.', '', '0', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time/types/%/delete', 'a:1:{i:5;N;}', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:2:{i:0;s:35:"system_delete_date_format_type_form";i:1;i:5;}', '', '125', '7', '0', '', 'admin/config/regional/date-time/types/%/delete', 'Delete date type', 't', '', '', 'a:0:{}', '6', 'Allow users to delete a configured date type.', '', '0', 'modules/system/system.admin.inc'), ('admin/config/regional/date-time/formats/%/edit', 'a:1:{i:5;N;}', '', 'user_access', 'a:1:{i:0;s:29:"administer site configuration";}', 'drupal_get_form', 'a:2:{i:0;s:34:"system_configure_date_formats_form";i:1;i:5;}', '', '125', '7', '0', '', 'admin/config/regional/date-time/formats/%/edit', 'Edit date format', 't', '', '', 'a:0:{}', '6', 'Allow users to edit a configured date format.', '', '0', 'modules/system/system.admin.inc');
INSERT INTO d7_variable (name, value) VALUES ('menu_masks', 'a:19:{i:0;i:125;i:1;i:121;i:2;i:63;i:3;i:62;i:4;i:61;i:5;i:60;i:6;i:44;i:7;i:31;i:8;i:30;i:9;i:24;i:10;i:21;i:11;i:15;i:12;i:14;i:13;i:11;i:14;i:7;i:15;i:5;i:16;i:3;i:17;i:2;i:18;i:1;}');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin', '0', '0', '0', '0', '9', 'system', 'Administration', 'a:0:{}', '0', '0');
INSERT INTO d7_variable (name, value) VALUES ('menu_expanded', 'a:0:{}');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('user-menu', '0', 'user', '0', '0', '0', '0', '-10', 'system', 'User account', 'a:1:{s:5:"alter";b:1;}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '0', 'filter/tips', '1', '0', '0', '0', '0', 'system', 'Compose tips', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '0', 'node/%', '0', '0', '0', '0', '0', 'system', '', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '0', 'node/add', '0', '0', '0', '0', '0', 'system', 'Add content', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/appearance', '0', '0', '0', '0', '-6', 'system', 'Appearance', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:33:"Select and configure your themes.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/config', '0', '0', '0', '0', '0', 'system', 'Configuration', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:20:"Administer settings.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/content', '0', '0', '0', '0', '-10', 'system', 'Content', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:24:"Find and manage content.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('user-menu', '2', 'user/register', '-1', '0', '0', '0', '0', 'system', 'Create new account', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/index', '-1', '0', '0', '0', '-18', 'system', 'Index', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('user-menu', '2', 'user/login', '-1', '0', '0', '0', '0', 'system', 'Log in', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('user-menu', '0', 'user/logout', '0', '0', '0', '0', '10', 'system', 'Log out', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/modules', '0', '0', '0', '0', '-2', 'system', 'Modules', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:26:"Enable or disable modules.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '0', 'user/%', '0', '0', '0', '0', '0', 'system', 'My account', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/people', '0', '0', '0', '0', '-4', 'system', 'People', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Manage user accounts, roles, and permissions.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/reports', '0', '0', '0', '0', '5', 'system', 'Reports', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:34:"View reports, updates, and errors.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('user-menu', '2', 'user/password', '-1', '0', '0', '0', '0', 'system', 'Request new password', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/structure', '0', '0', '0', '0', '-8', 'system', 'Structure', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Administer blocks, content types, menus, etc.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '0', 'admin/tasks', '-1', '0', '0', '0', '-20', 'system', 'Tasks', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '15', 'admin/people/create', '-1', '0', '0', '0', '0', 'system', 'Add user', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '18', 'admin/structure/block', '0', '0', '0', '0', '0', 'system', 'Blocks', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:79:"Configure what block content appears in your site''s sidebars and other regions.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '14', 'user/%/cancel', '0', '0', '0', '0', '0', 'system', 'Cancel account', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '8', 'admin/content/node', '-1', '0', '0', '0', '-10', 'system', 'Content', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/content', '0', '0', '0', '0', '-15', 'system', 'Content authoring', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:53:"Settings related to formatting and authoring content.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '18', 'admin/structure/types', '0', '0', '0', '0', '0', 'system', 'Content types', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:92:"Manage content types, including default status, front page promotion, comment settings, etc.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '4', 'node/%/delete', '-1', '0', '0', '0', '1', 'system', 'Delete', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/development', '0', '0', '0', '0', '-10', 'system', 'Development', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:18:"Development tools.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '14', 'user/%/edit', '-1', '0', '0', '0', '0', 'system', 'Edit', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '4', 'node/%/edit', '-1', '0', '0', '0', '0', 'system', 'Edit', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '6', 'admin/appearance/list', '-1', '0', '0', '0', '-1', 'system', 'List', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:31:"Select and configure your theme";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '13', 'admin/modules/list', '-1', '0', '0', '0', '0', 'system', 'List', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '15', 'admin/people/people', '-1', '0', '0', '0', '-10', 'system', 'List', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:50:"Find and manage people interacting with your site.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/media', '0', '0', '0', '0', '-10', 'system', 'Media', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:12:"Media tools.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/people', '0', '0', '0', '0', '-20', 'system', 'People', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:24:"Configure user accounts.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '15', 'admin/people/permissions', '-1', '0', '0', '0', '0', 'system', 'Permissions', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:64:"Determine access to features by selecting permissions for roles.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '16', 'admin/reports/dblog', '0', '0', '0', '0', '-1', 'system', 'Recent log messages', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"View events that have recently been logged.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/regional', '0', '0', '0', '0', '-5', 'system', 'Regional and language', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:48:"Regional settings, localization and translation.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '4', 'node/%/revisions', '-1', '0', '0', '0', '2', 'system', 'Revisions', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/search', '0', '0', '0', '0', '-10', 'system', 'Search and metadata', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:36:"Local site search, metadata and SEO.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '6', 'admin/appearance/settings', '-1', '0', '0', '0', '20', 'system', 'Settings', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:46:"Configure default and theme specific settings.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '16', 'admin/reports/status', '0', '0', '0', '0', '-60', 'system', 'Status report', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:74:"Get a status report about your site''s operation and any detected problems.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/system', '0', '0', '0', '0', '-20', 'system', 'System', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:37:"General system related configuration.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '16', 'admin/reports/access-denied', '0', '0', '0', '0', '0', 'system', 'Top ''access denied'' errors', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:35:"View ''access denied'' errors (403s).";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '16', 'admin/reports/page-not-found', '0', '0', '0', '0', '0', 'system', 'Top ''page not found'' errors', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:36:"View ''page not found'' errors (404s).";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '13', 'admin/modules/uninstall', '-1', '0', '0', '0', '20', 'system', 'Uninstall', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/user-interface', '0', '0', '0', '0', '-15', 'system', 'User interface', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:38:"Tools that enhance the user interface.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '4', 'node/%/view', '-1', '0', '0', '0', '-10', 'system', 'View', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '14', 'user/%/view', '-1', '0', '0', '0', '-10', 'system', 'View', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/services', '0', '0', '0', '0', '0', 'system', 'Web services', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:30:"Tools related to web services.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '7', 'admin/config/workflow', '0', '0', '0', '0', '5', 'system', 'Workflow', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Content workflow, editorial workflow tools.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '34', 'admin/config/people/accounts', '0', '0', '0', '0', '-10', 'system', 'Account settings', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:109:"Configure default behavior of users, including registration requirements, e-mails, fields, and user pictures.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '42', 'admin/config/system/actions', '0', '0', '0', '0', '0', 'system', 'Actions', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:41:"Manage the actions defined for your site.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '25', 'admin/structure/types/add', '-1', '0', '0', '0', '0', 'system', 'Add content type', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/bartik', '-1', '0', '0', '0', '0', 'system', 'Bartik', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '39', 'admin/config/search/clean-urls', '0', '0', '0', '0', '5', 'system', 'Clean URLs', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Enable or disable clean URLs for your site.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '42', 'admin/config/system/cron', '0', '0', '0', '0', '20', 'system', 'Cron', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:40:"Manage automatic site maintenance tasks.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '37', 'admin/config/regional/date-time', '0', '0', '0', '0', '-15', 'system', 'Date and time', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:44:"Configure display formats for date and time.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '16', 'admin/reports/event/%', '0', '0', '0', '0', '0', 'system', 'Details', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '33', 'admin/config/media/file-system', '0', '0', '0', '0', '-10', 'system', 'File system', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:68:"Tell Drupal where to store uploaded files and how they are accessed.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/garland', '-1', '0', '0', '0', '0', 'system', 'Garland', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/global', '-1', '0', '0', '0', '-1', 'system', 'Global settings', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '34', 'admin/config/people/ip-blocking', '0', '0', '0', '0', '10', 'system', 'IP address blocking', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:28:"Manage blocked IP addresses.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '33', 'admin/config/media/image-toolkit', '0', '0', '0', '0', '20', 'system', 'Image toolkit', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:74:"Choose which image toolkit to use if you have installed optional toolkits.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '31', 'admin/modules/list/confirm', '-1', '0', '0', '0', '0', 'system', 'List', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '25', 'admin/structure/types/list', '-1', '0', '0', '0', '-10', 'system', 'List', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '27', 'admin/config/development/logging', '0', '0', '0', '0', '-15', 'system', 'Logging and errors', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:154:"Settings for logging and alerts modules. Various modules can route Drupal''s system events to different destinations, such as syslog, database, email, etc.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '27', 'admin/config/development/maintenance', '0', '0', '0', '0', '-10', 'system', 'Maintenance mode', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:62:"Take the site offline for maintenance or bring it back online.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '27', 'admin/config/development/performance', '0', '0', '0', '0', '-20', 'system', 'Performance', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:101:"Enable or disable page caching for anonymous users and set CSS and JS bandwidth optimization options.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '35', 'admin/people/permissions/list', '-1', '0', '0', '0', '-8', 'system', 'Permissions', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:64:"Determine access to features by selecting permissions for roles.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '49', 'admin/config/services/rss-publishing', '0', '0', '0', '0', '0', 'system', 'RSS publishing', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:114:"Configure the site description, the number of items per feed and whether feeds should be titles/teasers/full-text.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '37', 'admin/config/regional/settings', '0', '0', '0', '0', '-20', 'system', 'Regional settings', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:54:"Settings for the site''s default time zone and country.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '35', 'admin/people/permissions/roles', '-1', '0', '0', '0', '-5', 'system', 'Roles', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:30:"List, edit, or add user roles.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/seven', '-1', '0', '0', '0', '0', 'system', 'Seven', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '42', 'admin/config/system/site-information', '0', '0', '0', '0', '-20', 'system', 'Site information', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:104:"Change site name, e-mail address, slogan, default front page, and number of posts per page, error pages.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/stark', '-1', '0', '0', '0', '0', 'system', 'Stark', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/test_theme', '-1', '0', '0', '0', '0', 'system', 'Test theme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '24', 'admin/config/content/formats', '0', '0', '0', '0', '0', 'system', 'Text formats', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:127:"Configure how content input by users is filtered, including allowed HTML tags. Also allows enabling of module-provided filters.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '45', 'admin/modules/uninstall/confirm', '-1', '0', '0', '0', '0', 'system', 'Uninstall', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/update_test_basetheme', '-1', '0', '0', '0', '0', 'system', 'Update test base theme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '40', 'admin/appearance/settings/update_test_subtheme', '-1', '0', '0', '0', '0', 'system', 'Update test subtheme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '28', 'user/%/edit/account', '-1', '0', '0', '0', '0', 'system', 'Account', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '78', 'admin/config/content/formats/%', '0', '0', '0', '0', '0', 'system', '', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '78', 'admin/config/content/formats/add', '-1', '0', '0', '0', '1', 'system', 'Add text format', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/bartik', '-1', '0', '0', '0', '-10', 'system', 'Bartik', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '52', 'admin/config/system/actions/configure', '-1', '0', '0', '0', '0', 'system', 'Configure an advanced action', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '25', 'admin/structure/types/manage/%', '0', '0', '0', '0', '0', 'system', 'Edit content type', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '58', 'admin/config/regional/date-time/formats', '-1', '0', '0', '0', '-9', 'system', 'Formats', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:51:"Configure display format strings for date and time.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/garland', '-1', '0', '0', '0', '0', 'system', 'Garland', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '78', 'admin/config/content/formats/list', '-1', '0', '0', '0', '0', 'system', 'List', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '52', 'admin/config/system/actions/manage', '-1', '0', '0', '0', '-2', 'system', 'Manage actions', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:41:"Manage the actions defined for your site.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '51', 'admin/config/people/accounts/settings', '-1', '0', '0', '0', '-10', 'system', 'Settings', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/seven', '-1', '0', '0', '0', '0', 'system', 'Seven', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/stark', '-1', '0', '0', '0', '0', 'system', 'Stark', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/test_theme', '-1', '0', '0', '0', '0', 'system', 'Test theme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '58', 'admin/config/regional/date-time/types', '-1', '0', '0', '0', '-10', 'system', 'Types', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:44:"Configure display formats for date and time.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/update_test_basetheme', '-1', '0', '0', '0', '0', 'system', 'Update test base theme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/list/update_test_subtheme', '-1', '0', '0', '0', '0', 'system', 'Update test subtheme', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '38', 'node/%/revisions/%/delete', '0', '0', '0', '0', '0', 'system', 'Delete earlier revision', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '38', 'node/%/revisions/%/revert', '0', '0', '0', '0', '0', 'system', 'Revert to earlier revision', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '38', 'node/%/revisions/%/view', '0', '0', '0', '0', '0', 'system', 'Revisions', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '89', 'admin/structure/block/list/garland/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '93', 'admin/structure/block/list/seven/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '94', 'admin/structure/block/list/stark/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '95', 'admin/structure/block/list/test_theme/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '97', 'admin/structure/block/list/update_test_basetheme/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '98', 'admin/structure/block/list/update_test_subtheme/add', '-1', '0', '0', '0', '0', 'system', 'Add block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '96', 'admin/config/regional/date-time/types/add', '-1', '0', '0', '0', '-10', 'system', 'Add date type', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:18:"Add new date type.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '88', 'admin/config/regional/date-time/formats/add', '-1', '0', '0', '0', '-10', 'system', 'Add format', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Allow users to add additional date formats.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '21', 'admin/structure/block/manage/%/%', '0', '0', '0', '0', '0', 'system', 'Configure block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('navigation', '22', 'user/%/cancel/confirm/%/%', '0', '0', '0', '0', '0', 'system', 'Confirm account cancellation', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '87', 'admin/structure/types/manage/%/delete', '0', '0', '0', '0', '0', 'system', 'Delete', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '63', 'admin/config/people/ip-blocking/delete/%', '0', '0', '0', '0', '0', 'system', 'Delete IP address', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '52', 'admin/config/system/actions/delete/%', '0', '0', '0', '0', '0', 'system', 'Delete action', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:17:"Delete an action.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '73', 'admin/people/permissions/roles/delete/%', '0', '0', '0', '0', '0', 'system', 'Delete role', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '83', 'admin/config/content/formats/%/disable', '0', '0', '0', '0', '0', 'system', 'Disable text format', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '87', 'admin/structure/types/manage/%/edit', '-1', '0', '0', '0', '0', 'system', 'Edit', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '73', 'admin/people/permissions/roles/edit/%', '0', '0', '0', '0', '0', 'system', 'Edit role', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '110', 'admin/structure/block/manage/%/%/configure', '-1', '0', '0', '0', '0', 'system', 'Configure block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '110', 'admin/structure/block/manage/%/%/delete', '-1', '0', '0', '0', '0', 'system', 'Delete block', 'a:0:{}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '88', 'admin/config/regional/date-time/formats/%/delete', '0', '0', '0', '0', '0', 'system', 'Delete date format', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:47:"Allow users to delete a configured date format.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '96', 'admin/config/regional/date-time/types/%/delete', '0', '0', '0', '0', '0', 'system', 'Delete date type', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Allow users to delete a configured date type.";}}', '0', '0');
INSERT INTO d7_menu_links (menu_name, plid, link_path, hidden, external, has_children, expanded, weight, module, link_title, options, customized, updated) VALUES ('management', '88', 'admin/config/regional/date-time/formats/%/edit', '0', '0', '0', '0', '0', 'system', 'Edit date format', 'a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Allow users to edit a configured date format.";}}', '0', '0');
INSERT INTO d7_watchdog (uid, type, message, variables, severity, link, location, referer, hostname, timestamp) VALUES ('0', 'menu', '%type: !message in %function (line %line of %file).', 'a:6:{s:5:"%type";s:12:"PDOException";s:8:"!message";s:508:"SQLSTATE[42000]: Syntax error or access violation: 1 Error occured while executing query &#039;UPDATE d_variable set NAME=&#039;hi&#039; WHERE 0=1;&#039;: The table &#039;d_variable&#039; does not exist.
 ([1] at /home/doug/dev/parelastic/pdo_parelastic/parelastic_statement.c:93): SELECT ml.*, m.*, ml.weight AS link_weight
FROM 
{menu_links} ml
LEFT OUTER JOIN {menu_router} m ON m.path = ml.router_path
WHERE  (ml.mlid = :db_condition_placeholder_0) ; Array
(
    [:db_condition_placeholder_0] =&gt; 25
)
";s:9:"%function";s:16:"menu_link_load()";s:5:"%file";s:37:"/var/www/drupal-7.4/includes/menu.inc";s:5:"%line";i:2515;s:14:"severity_level";i:3;}', '3', '', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&id=1&op=finished', 'http://localhost/drupal-7.4/install.php?profile=minimal&locale=en&op=start&id=1', '127.0.0.1', '1311348919');
#SELECT * from system LIMIT 1;
#SELECT * from foobar order by value;
#SELECT * from foobar order by value;
#SELECT * from foobar order by value;
#SELECT * from foobar order by value;
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT 1 FROM d7_variable LIMIT 0, 1;
SELECT 1 FROM d7_actions LIMIT 0, 1;
SELECT 1 FROM d7_batch LIMIT 0, 1;
SELECT 1 FROM d7_blocked_ips LIMIT 0, 1;
SELECT 1 FROM d7_cache LIMIT 0, 1;
SELECT 1 FROM d7_cache_bootstrap LIMIT 0, 1;
SELECT 1 FROM d7_cache_form LIMIT 0, 1;
SELECT 1 FROM d7_cache_page LIMIT 0, 1;
SELECT 1 FROM d7_cache_menu LIMIT 0, 1;
SELECT 1 FROM d7_cache_path LIMIT 0, 1;
SELECT 1 FROM d7_date_format_type LIMIT 0, 1;
SELECT 1 FROM d7_date_formats LIMIT 0, 1;
SELECT 1 FROM d7_date_format_locale LIMIT 0, 1;
SELECT 1 FROM d7_file_managed LIMIT 0, 1;
SELECT 1 FROM d7_file_usage LIMIT 0, 1;
SELECT 1 FROM d7_flood LIMIT 0, 1;
SELECT 1 FROM d7_menu_router LIMIT 0, 1;
SELECT 1 FROM d7_menu_links LIMIT 0, 1;
SELECT 1 FROM d7_queue LIMIT 0, 1;
SELECT 1 FROM d7_registry LIMIT 0, 1;
SELECT 1 FROM d7_registry_file LIMIT 0, 1;
SELECT 1 FROM d7_semaphore LIMIT 0, 1;
SELECT 1 FROM d7_sequences LIMIT 0, 1;
SELECT 1 FROM d7_sessions LIMIT 0, 1;
SELECT 1 FROM d7_system LIMIT 0, 1;
SELECT 1 FROM d7_url_alias LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'theme';
SELECT * FROM d7_system WHERE type = 'theme';
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'theme_default') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT cid, data, created, expire, serialized FROM d7_cache_bootstrap WHERE cid IN ('variables');
SELECT expire, value FROM d7_semaphore WHERE name = 'variable_init';
SELECT cid, data, created, expire, serialized FROM d7_cache_bootstrap WHERE cid IN ('module_implements');
SELECT filename FROM d7_system WHERE name = 'bartik' AND type = 'theme';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT 1 FROM d7_variable LIMIT 0, 1;
SELECT 1 FROM d7_actions LIMIT 0, 1;
SELECT 1 FROM d7_batch LIMIT 0, 1;
SELECT 1 FROM d7_blocked_ips LIMIT 0, 1;
SELECT 1 FROM d7_cache LIMIT 0, 1;
SELECT 1 FROM d7_cache_bootstrap LIMIT 0, 1;
SELECT 1 FROM d7_cache_form LIMIT 0, 1;
SELECT 1 FROM d7_cache_page LIMIT 0, 1;
SELECT 1 FROM d7_cache_menu LIMIT 0, 1;
SELECT 1 FROM d7_cache_path LIMIT 0, 1;
SELECT 1 FROM d7_date_format_type LIMIT 0, 1;
SELECT 1 FROM d7_date_formats LIMIT 0, 1;
SELECT 1 FROM d7_date_format_locale LIMIT 0, 1;
SELECT 1 FROM d7_file_managed LIMIT 0, 1;
SELECT 1 FROM d7_file_usage LIMIT 0, 1;
SELECT 1 FROM d7_flood LIMIT 0, 1;
SELECT 1 FROM d7_menu_router LIMIT 0, 1;
SELECT 1 FROM d7_menu_links LIMIT 0, 1;
SELECT 1 FROM d7_queue LIMIT 0, 1;
SELECT 1 FROM d7_registry LIMIT 0, 1;
SELECT 1 FROM d7_registry_file LIMIT 0, 1;
SELECT 1 FROM d7_semaphore LIMIT 0, 1;
SELECT 1 FROM d7_sequences LIMIT 0, 1;
SELECT 1 FROM d7_sessions LIMIT 0, 1;
SELECT 1 FROM d7_system LIMIT 0, 1;
SELECT 1 FROM d7_url_alias LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'theme';
SELECT * FROM d7_system WHERE type = 'theme';
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'theme_default') );
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'cron_key') );
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'module';
SELECT status FROM d7_system WHERE type = 'module' AND name = 'user';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.archiver.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.mail.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.queue.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.tar.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.updater.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/system/system.test') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/user/user.module') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/user/user.test') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/filetransfer/filetransfer.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/filetransfer/ssh.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/filetransfer/ftp.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/filetransfer/local.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/mysql/query.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/mysql/database.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/mysql/schema.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/mysql/install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/pgsql/select.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/pgsql/query.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/pgsql/database.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/pgsql/schema.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/pgsql/install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/parelastic/select.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/parelastic/query.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/parelastic/database.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/parelastic/schema.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/parelastic/install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/sqlite/select.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/sqlite/query.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/sqlite/database.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/sqlite/schema.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/sqlite/install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/prefetch.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/select.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/log.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/query.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/database.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/database/schema.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/language.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/file.mimetypes.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/xmlrpcs.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/mail.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/path.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/locale.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/xmlrpc.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/theme.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/menu.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/graph.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/form.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/batch.queue.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/archiver.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/tablesort.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/session.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/update.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/stream_wrappers.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/password.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/ajax.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/token.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/entity.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/authorize.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/iso.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/cache-install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/bootstrap.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/unicode.entities.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/module.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/install.core.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/unicode.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/batch.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/common.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/theme.maintenance.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/actions.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/pager.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/date.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/errors.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/utility.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/cache.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/registry.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/install.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/image.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/file.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/lock.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'includes/updater.inc') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_authmap LIMIT 0, 1;
SELECT 1 FROM d7_role_permission LIMIT 0, 1;
SELECT 1 FROM d7_role LIMIT 0, 1;
SELECT 1 FROM d7_users LIMIT 0, 1;
SELECT 1 FROM d7_users_roles LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'install_profile_modules') );
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'install_task') );
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT DISTINCT SUBSTRING_INDEX(source, '/', 1) AS path FROM d7_url_alias;
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'path_alias_whitelist') );
#SELECT 1 FROM d7_profile_field LIMIT 0, 1;
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'module';
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'install_current_batch') );
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'drupal_private_key') );
SELECT 1 AS expression
FROM 
d7_sessions sessions
WHERE ( (sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw') AND (ssid = '') );
SELECT MAX(value) FROM d7_sequences;
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT rid, permission FROM d7_role_permission WHERE rid IN ('1');
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'filter';
SELECT filename FROM d7_system WHERE name = 'filter' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/filter/filter.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_filter LIMIT 0, 1;
SELECT 1 FROM d7_filter_format LIMIT 0, 1;
SELECT 1 FROM d7_cache_filter LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 AS expression
FROM 
d7_filter_format filter_format
WHERE ( (format = 'plain_text') );
SELECT 1 AS expression
FROM 
d7_filter filter
WHERE ( (format = 'plain_text') AND (name = 'filter_url') );
SELECT 1 AS expression
FROM 
d7_filter filter
WHERE ( (format = 'plain_text') AND (name = 'filter_autop') );
SELECT 1 AS expression
FROM 
d7_filter filter
WHERE ( (format = 'plain_text') AND (name = 'filter_htmlcorrector') );
SELECT 1 AS expression
FROM 
d7_filter filter
WHERE ( (format = 'plain_text') AND (name = 'filter_html_escape') );
SELECT 1 AS expression
FROM 
d7_filter filter
WHERE ( (format = 'plain_text') AND (name = 'filter_html') );
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'filter_fallback_format') );
SELECT 1 AS expression
FROM 
d7_sessions sessions
WHERE ( (sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw') AND (ssid = '') );
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'field_sql_storage';
SELECT filename FROM d7_system WHERE name = 'field_sql_storage' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/field/modules/field_sql_storage/field_sql_storage.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'user';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'node';
SELECT filename FROM d7_system WHERE name = 'node' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/node/node.module') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/node/node.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_node LIMIT 0, 1;
SELECT 1 FROM d7_node_access LIMIT 0, 1;
SELECT 1 FROM d7_node_revision LIMIT 0, 1;
SELECT 1 FROM d7_node_type LIMIT 0, 1;
SELECT 1 FROM d7_block_node_type LIMIT 0, 1;
SELECT 1 FROM d7_history LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'field';
SELECT filename FROM d7_system WHERE name = 'field' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/field/field.module') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/field/field.attach.inc') );
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/field/tests/field.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT 1 FROM d7_field_config_instance LIMIT 0, 1;
SELECT 1 FROM d7_cache_field LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'text';
SELECT filename FROM d7_system WHERE name = 'text' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/field/modules/text/text.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT fc.*
FROM 
d7_field_config fc;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'block';
SELECT filename FROM d7_system WHERE name = 'block' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/block/block.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT fc.*
FROM 
d7_field_config fc;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_block LIMIT 0, 1;
SELECT 1 FROM d7_block_role LIMIT 0, 1;
SELECT 1 FROM d7_block_custom LIMIT 0, 1;
SELECT 1 FROM d7_cache_block LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'dblog';
SELECT filename FROM d7_system WHERE name = 'dblog' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'modules/dblog/dblog.test') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT fc.*
FROM 
d7_field_config fc;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 FROM d7_watchdog LIMIT 0, 1;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data, item_id FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC LIMIT 0, 1;
SELECT status FROM d7_system WHERE type = 'module' AND name = 'minimal';
SELECT filename FROM d7_system WHERE name = 'minimal' AND type = 'module';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT 1 AS expression
FROM 
d7_registry_file registry_file
WHERE ( (filename = 'profiles/minimal/minimal.profile') );
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_field_config LIMIT 0, 1;
SELECT fc.*
FROM 
d7_field_config fc;
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT name, schema_version FROM d7_system WHERE type = 'module';
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'user_register') );
SELECT ff.*
FROM 
d7_filter_format ff
WHERE  (status = '1') 
ORDER BY weight ASC;
SELECT nt.*
FROM 
d7_node_type nt
WHERE  (disabled = '0') 
ORDER BY nt.type ASC;
SELECT 1 AS expression
FROM 
d7_role_permission role_permission
WHERE ( (rid = '1') AND (permission = 'access content') );
SELECT 1 AS expression
FROM 
d7_role_permission role_permission
WHERE ( (rid = '2') AND (permission = 'access content') );
SELECT value FROM d7_variable WHERE name = 'install_task';
SELECT name, value FROM d7_variable;
SELECT name, filename FROM d7_system WHERE status = 1 AND bootstrap = 1 AND type = 'module' ORDER BY weight ASC, name ASC;
SELECT 1 FROM d7_blocked_ips WHERE ip = '127.0.0.1';
SELECT u.*, s.* FROM d7_users u INNER JOIN d7_sessions s ON u.uid = s.uid WHERE s.sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw';
SELECT filename FROM d7_registry WHERE name = 'SelectQueryExtender' AND type = 'class';
SELECT * FROM d7_system WHERE type = 'theme' OR (type = 'module' AND status = 1) ORDER BY weight ASC, name ASC;
SELECT batch FROM d7_batch WHERE bid = '1' AND token = 'TP9sNghho7cha_mn97e0gimhiCKOQaa2ukeFpxzKfSo';
SELECT data FROM d7_queue q WHERE name = 'drupal_batch:1:0' ORDER BY item_id ASC;
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'css_js_query_string') );
#SELECT 1 FROM d7_profile_field LIMIT 0, 1;
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_system WHERE type = 'module';
SELECT * FROM d7_registry_file;
SELECT filename, name, type, status, schema_version, weight FROM d7_system WHERE type = 'theme';
SELECT * FROM d7_system WHERE type = 'theme';
SELECT nt.*
FROM 
d7_node_type nt
ORDER BY nt.type ASC;
SELECT nt.*
FROM 
d7_node_type nt
WHERE  (disabled = '0') 
ORDER BY nt.type ASC;
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'menu_masks') );
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_name FROM d7_menu_links WHERE expanded <> 0 GROUP BY menu_name;
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'menu_expanded') );
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'filter/tips') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'filter') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/content') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/register') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/index') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/login') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/logout') AND (module = 'system') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/modules') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/password') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'user-menu') AND (link_path = 'user') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/tasks') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = '') ;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/create') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '15') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '18') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%/cancel') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '14') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/content/node') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/content') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/content') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '8') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '18') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '4') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/development') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%/edit') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '14') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/edit') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '4') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/list') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '6') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/modules/list') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '13') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/people') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '15') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/media') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/people') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/permissions') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '15') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports/dblog') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '16') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/revisions') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '4') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/search') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '6') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports/status') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '16') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports/access-denied') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '16') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports/page-not-found') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '16') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/modules/uninstall') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '13') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/user-interface') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/view') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '4') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%/view') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '14') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/services') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/workflow') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '7') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/people/accounts') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '34') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/actions') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '42') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '25') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/bartik') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/search/clean-urls') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/search') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/search') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '39') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/cron') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '42') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '37') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/reports/event/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports/event') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/reports') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '16') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/media/file-system') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/media') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/media') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '33') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/garland') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/global') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/people/ip-blocking') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '34') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/media/image-toolkit') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/media') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/media') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '33') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/modules/list/confirm') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules/list') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules/list') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '31') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types/list') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '25') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/development/logging') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '27') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/development/maintenance') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '27') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/development/performance') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/development') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '27') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/permissions/list') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '35') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/services/rss-publishing') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/services') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/services') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '49') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/settings') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '37') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/permissions/roles') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '35') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/seven') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/site-information') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '42') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/stark') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/test_theme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content/formats') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '24') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/modules/uninstall/confirm') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules/uninstall') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/modules/uninstall') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '45') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/update_test_basetheme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/appearance/settings/update_test_subtheme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/appearance/settings') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '40') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%/edit/account') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/edit') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/edit') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '28') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content/formats/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '78') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content/formats/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '78') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/bartik') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/actions/configure') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '52') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types/manage/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types/manage') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '25') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/formats') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '58') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/garland') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content/formats/list') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '78') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/actions/manage') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '52') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/people/accounts/settings') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people/accounts') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people/accounts') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '51') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/seven') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/stark') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/test_theme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/types') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '58') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/update_test_basetheme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/update_test_subtheme') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/revisions/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '38') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/revisions/%/revert') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '38') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'node/%/revisions/%/view') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'node/%/revisions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '38') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/garland/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/garland') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/garland') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '89') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/seven/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/seven') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/seven') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '93') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/stark/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/stark') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/stark') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '94') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/test_theme/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/test_theme') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/test_theme') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '95') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/update_test_basetheme/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/update_test_basetheme') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/update_test_basetheme') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '97') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/list/update_test_subtheme/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/update_test_subtheme') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/list/update_test_subtheme') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '98') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/types/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/types') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/types') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '96') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/formats/add') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '88') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/manage/%/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '21') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'user/%/cancel/confirm/%/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/cancel/confirm/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/cancel/confirm') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/cancel') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'navigation') AND (link_path = 'user/%/cancel') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'navigation') AND (hidden = '0') AND (plid = '22') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types/manage/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types/manage/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types/manage/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '87') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/people/ip-blocking/delete/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people/ip-blocking/delete') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people/ip-blocking') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/people/ip-blocking') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '63') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/system/actions/delete/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions/delete') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/system/actions') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '52') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/permissions/roles/delete/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles/delete') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '73') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/content/formats/%/disable') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/content/formats/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '83') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/types/manage/%/edit') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types/manage/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/types/manage/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '87') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/people/permissions/roles/edit/%') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles/edit') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/people/permissions/roles') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '73') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/manage/%/%/configure') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage/%/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage/%/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '110') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/structure/block/manage/%/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage/%/%') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/structure/block/manage/%/%') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '110') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/formats/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '88') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/types/%/delete') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/types/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/types') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/types') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '96') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid, menu_links.menu_name AS menu_name, menu_links.plid AS plid, menu_links.customized AS customized, menu_links.has_children AS has_children, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE  (link_path = 'admin/config/regional/date-time/formats/%/edit') AND (module = 'system') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats/%') ;
SELECT COUNT(*) AS expression
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (module = 'system') AND (menu_name = 'management') AND (link_path = 'admin/config/regional/date-time/formats') ;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '88') 
LIMIT 1 OFFSET 0;
SELECT menu_links.link_path AS link_path, menu_links.mlid AS mlid, menu_links.router_path AS router_path, menu_links.updated AS updated
FROM 
d7_menu_links menu_links
WHERE ( (updated = '1') OR( (router_path NOT IN  ('rss.xml', 'node', 'batch', 'admin', 'user', 'admin/tasks', 'user/login', 'node/add', 'admin/compact', 'filter/tips', 'user/register', 'system/files', 'admin/index', 'system/temporary', 'system/timezone', 'admin/config', 'user/logout', 'user/password', 'user/autocomplete', 'admin/appearance', 'admin/content', 'admin/modules', 'admin/reports', 'admin/structure', 'system/ajax', 'node/%', 'admin/people', 'user/%', 'admin/content/node', 'admin/modules/list', 'node/%/view', 'user/%/view', 'admin/people/create', 'admin/appearance/list', 'admin/appearance/disable', 'admin/appearance/enable', 'admin/people/people', 'admin/appearance/default', 'admin/modules/uninstall', 'admin/structure/types', 'admin/reports/dblog', 'admin/reports/status', 'admin/structure/block', 'user/%/cancel', 'admin/config/content', 'admin/config/development', 'user/%/edit', 'admin/config/media', 'admin/people/permissions', 'admin/config/regional', 'node/%/revisions', 'admin/config/search', 'admin/appearance/settings', 'admin/config/system', 'admin/reports/access-denied', 'admin/reports/page-not-found', 'admin/config/user-interface', 'admin/config/services', 'admin/config/workflow', 'node/%/delete', 'node/%/edit', 'admin/config/people', 'admin/appearance/settings/global', 'admin/structure/types/list', 'user/%/edit/account', 'admin/modules/list/confirm', 'admin/people/permissions/list', 'admin/modules/uninstall/confirm', 'admin/reports/status/php', 'admin/reports/status/run-cron', 'admin/config/system/actions', 'admin/structure/block/add', 'admin/structure/types/add', 'admin/appearance/settings/bartik', 'admin/reports/event/%', 'admin/appearance/settings/garland', 'admin/config/people/ip-blocking', 'admin/reports/status/rebuild', 'admin/appearance/settings/seven', 'admin/appearance/settings/stark', 'admin/appearance/settings/test_theme', 'admin/appearance/settings/update_test_basetheme', 'admin/appearance/settings/update_test_subtheme', 'admin/config/people/accounts', 'admin/config/search/clean-urls', 'admin/config/system/cron', 'admin/config/regional/date-time', 'admin/config/media/file-system', 'admin/config/media/image-toolkit', 'admin/config/development/logging', 'admin/config/development/maintenance', 'admin/config/development/performance', 'admin/config/services/rss-publishing', 'admin/config/regional/settings', 'admin/people/permissions/roles', 'admin/config/system/site-information', 'admin/config/content/formats', 'admin/config/content/formats/list', 'admin/config/people/accounts/settings', 'admin/config/content/formats/add', 'admin/config/system/actions/manage', 'admin/config/system/actions/orphan', 'admin/structure/block/list/bartik', 'admin/config/search/clean-urls/check', 'admin/config/system/actions/configure', 'admin/config/regional/date-time/formats', 'admin/structure/block/list/garland', 'user/reset/%/%/%', 'admin/structure/block/list/seven', 'admin/structure/block/list/stark', 'admin/structure/block/list/test_theme', 'admin/structure/block/list/update_test_basetheme', 'admin/structure/block/list/update_test_subtheme', 'node/%/revisions/%/view', 'admin/config/regional/date-time/types', 'node/%/revisions/%/delete', 'admin/structure/types/manage/%', 'node/%/revisions/%/revert', 'admin/config/content/formats/%', 'admin/structure/block/demo/bartik', 'admin/structure/block/demo/garland', 'admin/structure/block/demo/seven', 'admin/structure/block/demo/stark', 'admin/structure/block/demo/test_theme', 'admin/structure/block/demo/update_test_basetheme', 'admin/structure/block/demo/update_test_subtheme', 'admin/structure/types/manage/%/edit', 'admin/config/regional/date-time/formats/lookup', 'admin/structure/types/manage/%/delete', 'admin/people/permissions/roles/edit/%', 'admin/structure/block/list/garland/add', 'admin/structure/block/list/seven/add', 'admin/structure/block/list/stark/add', 'admin/structure/block/list/test_theme/add', 'admin/structure/block/list/update_test_basetheme/add', 'admin/structure/block/list/update_test_subtheme/add', 'admin/structure/block/manage/%/%', 'admin/config/people/ip-blocking/delete/%', 'admin/config/regional/date-time/types/add', 'admin/config/regional/date-time/formats/add', 'user/%/cancel/confirm/%/%', 'admin/config/system/actions/delete/%', 'admin/people/permissions/roles/delete/%', 'admin/config/content/formats/%/disable', 'admin/structure/block/manage/%/%/configure', 'admin/structure/block/manage/%/%/delete', 'admin/config/regional/date-time/formats/%/delete', 'admin/config/regional/date-time/types/%/delete', 'admin/config/regional/date-time/formats/%/edit')) AND (external = '0') AND (customized = '1') ));
SELECT menu_links.*
FROM 
d7_menu_links menu_links
WHERE  (router_path NOT IN  ('rss.xml', 'node', 'batch', 'admin', 'user', 'admin/tasks', 'user/login', 'node/add', 'admin/compact', 'filter/tips', 'user/register', 'system/files', 'admin/index', 'system/temporary', 'system/timezone', 'admin/config', 'user/logout', 'user/password', 'user/autocomplete', 'admin/appearance', 'admin/content', 'admin/modules', 'admin/reports', 'admin/structure', 'system/ajax', 'node/%', 'admin/people', 'user/%', 'admin/content/node', 'admin/modules/list', 'node/%/view', 'user/%/view', 'admin/people/create', 'admin/appearance/list', 'admin/appearance/disable', 'admin/appearance/enable', 'admin/people/people', 'admin/appearance/default', 'admin/modules/uninstall', 'admin/structure/types', 'admin/reports/dblog', 'admin/reports/status', 'admin/structure/block', 'user/%/cancel', 'admin/config/content', 'admin/config/development', 'user/%/edit', 'admin/config/media', 'admin/people/permissions', 'admin/config/regional', 'node/%/revisions', 'admin/config/search', 'admin/appearance/settings', 'admin/config/system', 'admin/reports/access-denied', 'admin/reports/page-not-found', 'admin/config/user-interface', 'admin/config/services', 'admin/config/workflow', 'node/%/delete', 'node/%/edit', 'admin/config/people', 'admin/appearance/settings/global', 'admin/structure/types/list', 'user/%/edit/account', 'admin/modules/list/confirm', 'admin/people/permissions/list', 'admin/modules/uninstall/confirm', 'admin/reports/status/php', 'admin/reports/status/run-cron', 'admin/config/system/actions', 'admin/structure/block/add', 'admin/structure/types/add', 'admin/appearance/settings/bartik', 'admin/reports/event/%', 'admin/appearance/settings/garland', 'admin/config/people/ip-blocking', 'admin/reports/status/rebuild', 'admin/appearance/settings/seven', 'admin/appearance/settings/stark', 'admin/appearance/settings/test_theme', 'admin/appearance/settings/update_test_basetheme', 'admin/appearance/settings/update_test_subtheme', 'admin/config/people/accounts', 'admin/config/search/clean-urls', 'admin/config/system/cron', 'admin/config/regional/date-time', 'admin/config/media/file-system', 'admin/config/media/image-toolkit', 'admin/config/development/logging', 'admin/config/development/maintenance', 'admin/config/development/performance', 'admin/config/services/rss-publishing', 'admin/config/regional/settings', 'admin/people/permissions/roles', 'admin/config/system/site-information', 'admin/config/content/formats', 'admin/config/content/formats/list', 'admin/config/people/accounts/settings', 'admin/config/content/formats/add', 'admin/config/system/actions/manage', 'admin/config/system/actions/orphan', 'admin/structure/block/list/bartik', 'admin/config/search/clean-urls/check', 'admin/config/system/actions/configure', 'admin/config/regional/date-time/formats', 'admin/structure/block/list/garland', 'user/reset/%/%/%', 'admin/structure/block/list/seven', 'admin/structure/block/list/stark', 'admin/structure/block/list/test_theme', 'admin/structure/block/list/update_test_basetheme', 'admin/structure/block/list/update_test_subtheme', 'node/%/revisions/%/view', 'admin/config/regional/date-time/types', 'node/%/revisions/%/delete', 'admin/structure/types/manage/%', 'node/%/revisions/%/revert', 'admin/config/content/formats/%', 'admin/structure/block/demo/bartik', 'admin/structure/block/demo/garland', 'admin/structure/block/demo/seven', 'admin/structure/block/demo/stark', 'admin/structure/block/demo/test_theme', 'admin/structure/block/demo/update_test_basetheme', 'admin/structure/block/demo/update_test_subtheme', 'admin/structure/types/manage/%/edit', 'admin/config/regional/date-time/formats/lookup', 'admin/structure/types/manage/%/delete', 'admin/people/permissions/roles/edit/%', 'admin/structure/block/list/garland/add', 'admin/structure/block/list/seven/add', 'admin/structure/block/list/stark/add', 'admin/structure/block/list/test_theme/add', 'admin/structure/block/list/update_test_basetheme/add', 'admin/structure/block/list/update_test_subtheme/add', 'admin/structure/block/manage/%/%', 'admin/config/people/ip-blocking/delete/%', 'admin/config/regional/date-time/types/add', 'admin/config/regional/date-time/formats/add', 'user/%/cancel/confirm/%/%', 'admin/config/system/actions/delete/%', 'admin/people/permissions/roles/delete/%', 'admin/config/content/formats/%/disable', 'admin/structure/block/manage/%/%/configure', 'admin/structure/block/manage/%/%/delete', 'admin/config/regional/date-time/formats/%/delete', 'admin/config/regional/date-time/types/%/delete', 'admin/config/regional/date-time/formats/%/edit')) AND (external = '0') AND (updated = '0') AND (customized = '0') 
ORDER BY depth DESC;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'user-menu') AND (hidden = '0') AND (plid = '2') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '15') 
LIMIT 1 OFFSET 0;
SELECT menu_links.mlid AS mlid
FROM 
d7_menu_links menu_links
WHERE  (menu_name = 'management') AND (hidden = '0') AND (plid = '8') 
LIMIT 1 OFFSET 0;
SELECT mlid FROM d7_menu_links WHERE plid = '18';
SELECT ml.*, m.*, ml.weight AS link_weight
FROM 
d7_menu_links ml
LEFT OUTER JOIN d7_menu_router m ON m.path = ml.router_path
WHERE  (ml.mlid = '25') ;
SELECT rid, permission FROM d7_role_permission WHERE rid IN ('1');
SELECT * FROM d7_menu_router WHERE path IN ('node') ORDER BY fit DESC LIMIT 0, 1;
SELECT ml.*, m.*, ml.weight AS link_weight
FROM 
d7_menu_links ml
LEFT OUTER JOIN d7_menu_router m ON m.path = ml.router_path
WHERE  (ml.menu_name IN  ('navigation', 'management', 'user-menu', 'main-menu')) AND (ml.link_path IN  ('node')) ;
SELECT menu_name FROM d7_menu_links WHERE expanded <> 0 GROUP BY menu_name;
SELECT 1 AS expression
FROM 
d7_variable variable
WHERE ( (name = 'menu_expanded') );
# from simple query test
select * from d7_menu_links;
select * from d7_menu_links where 0=1;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET schema_version='7071'
WHERE  (name = 'system') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET schema_version='7071'
WHERE  (name = 'system') ;
UPDATE d7_system SET status='1'
WHERE  (type = 'theme') AND (name = 'bartik') ;
UPDATE d7_system SET info='a:13:{s:4:"name";s:6:"System";s:11:"description";s:54:"Handles general site configuration for administrators.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:5:"files";a:6:{i:0;s:19:"system.archiver.inc";i:1;s:15:"system.mail.inc";i:2;s:16:"system.queue.inc";i:3;s:14:"system.tar.inc";i:4;s:18:"system.updater.inc";i:5;s:11:"system.test";}s:8:"required";b:1;s:9:"configure";s:19:"admin/config/system";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:12:"dependencies";a:0:{}s:3:"php";s:5:"5.2.4";s:9:"bootstrap";i:0;}'
WHERE  (filename = 'modules/system/system.module') ;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'user') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7017'
WHERE  (name = 'user') ;
UPDATE d7_role SET rid='1'
WHERE  (rid = '0') ;
UPDATE d7_role SET rid='2'
WHERE  (rid = '0') ;
UPDATE d7_system SET weight='1000'
WHERE  (filename = 'profiles/minimal/minimal.profile') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:0:{}s:7:"success";b:0;s:5:"start";i:0;s:7:"elapsed";i:0;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:9;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'filter') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7010'
WHERE  (name = 'filter') ;
UPDATE d7_sessions SET uid='0', cache='0', hostname='127.0.0.1', session='batches|a:1:{i:1;b:1;}', timestamp='1311348752'
WHERE ( (sid = 'nzxMXNSqjFy84eC4uZb1FI4Fk4dNwJ6ZxuUwO9cpeEw') AND (ssid = '') );
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:1:{i:0;s:6:"filter";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:15609.95000000000072759576141834259033203125;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:8;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'field_sql_storage') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7002'
WHERE  (name = 'field_sql_storage') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:2:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:30690.02000000000043655745685100555419921875;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:7;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'node') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7011'
WHERE  (name = 'node') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:4:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:56540.0100000000020372681319713592529296875;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:5;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'field') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7001'
WHERE  (name = 'field') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:5:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:78590.009999999994761310517787933349609375;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:4;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'text') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7000'
WHERE  (name = 'text') ;
UPDATE d7_field_config SET module='text', active='1'
WHERE  (type = 'text') ;
UPDATE d7_field_config SET module='text', active='1'
WHERE  (type = 'text_long') ;
UPDATE d7_field_config SET module='text', active='1'
WHERE  (type = 'text_with_summary') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:6:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";i:5;s:4:"text";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:95200.080000000001746229827404022216796875;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:3;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'block') ;
UPDATE d7_system SET bootstrap='0';
UPDATE d7_system SET schema_version='7008'
WHERE  (name = 'block') ;
UPDATE d7_system SET weight='-5'
WHERE  (name = 'block') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:7:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";i:5;s:4:"text";i:6;s:5:"block";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:117829.979999999995925463736057281494140625;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:2;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'dblog') ;
UPDATE d7_system SET bootstrap='1'
WHERE  (name IN  ('dblog')) ;
UPDATE d7_system SET bootstrap='0'
WHERE  (name NOT IN  ('dblog')) ;
UPDATE d7_system SET schema_version='7001'
WHERE  (name = 'dblog') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:8:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";i:5;s:4:"text";i:6;s:5:"block";i:7;s:5:"dblog";}s:7:"success";b:0;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:138859.97000000000116415321826934814453125;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:1;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET status='1'
WHERE  (type = 'module') AND (name = 'minimal') ;
UPDATE d7_system SET bootstrap='1'
WHERE  (name IN  ('dblog')) ;
UPDATE d7_system SET bootstrap='0'
WHERE  (name NOT IN  ('dblog')) ;
UPDATE d7_system SET schema_version='0'
WHERE  (name = 'minimal') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:9:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";i:5;s:4:"text";i:6;s:5:"block";i:7;s:5:"dblog";i:8;s:7:"minimal";}s:7:"success";b:1;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:158759.98000000001047737896442413330078125;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:0;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_drupal_install_test SET id = 2;
UPDATE d7_system SET info='a:16:{s:4:"name";s:7:"Garland";s:11:"description";s:111:"A multi-column theme which can be configured to modify colors and switch between fixed and fluid width layouts.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:1:{s:9:"style.css";s:24:"themes/garland/style.css";}s:5:"print";a:1:{s:9:"print.css";s:24:"themes/garland/print.css";}}s:8:"settings";a:1:{s:13:"garland_width";s:5:"fluid";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:29:"themes/garland/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/garland/garland.info') ;
UPDATE d7_system SET info='a:16:{s:4:"name";s:5:"Seven";s:11:"description";s:65:"A simple one-column, tableless, fluid width administration theme.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:6:"screen";a:2:{s:9:"reset.css";s:22:"themes/seven/reset.css";s:9:"style.css";s:22:"themes/seven/style.css";}}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"1";}s:7:"regions";a:5:{s:7:"content";s:7:"Content";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:13:"sidebar_first";s:13:"First sidebar";}s:14:"regions_hidden";a:3:{i:0;s:13:"sidebar_first";i:1;s:8:"page_top";i:2;s:11:"page_bottom";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/seven/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}}'
WHERE  (filename = 'themes/seven/seven.info') ;
UPDATE d7_system SET info='a:15:{s:4:"name";s:5:"Stark";s:11:"description";s:208:"This theme demonstrates Drupal''s default HTML markup and CSS styles. To learn how to build your own theme and override Drupal''s default code, see the <a href="http://drupal.org/theme-guide">Theming Guide</a>.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:10:"layout.css";s:23:"themes/stark/layout.css";}}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:27:"themes/stark/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/stark/stark.info') ;
UPDATE d7_system SET info='a:15:{s:4:"name";s:10:"Test theme";s:11:"description";s:34:"Theme for testing the theme system";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:11:"stylesheets";a:1:{s:3:"all";a:1:{s:15:"system.base.css";s:39:"themes/tests/test_theme/system.base.css";}}s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:38:"themes/tests/test_theme/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/tests/test_theme/test_theme.info') ;
UPDATE d7_system SET info='a:15:{s:4:"name";s:22:"Update test base theme";s:11:"description";s:63:"Test theme which acts as a base theme for other test subthemes.";s:4:"core";s:3:"7.x";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:49:"themes/tests/update_test_basetheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/tests/update_test_basetheme/update_test_basetheme.info') ;
UPDATE d7_system SET info='a:16:{s:4:"name";s:20:"Update test subtheme";s:11:"description";s:62:"Test theme which uses update_test_basetheme as the base theme.";s:4:"core";s:3:"7.x";s:10:"base theme";s:21:"update_test_basetheme";s:6:"hidden";b:1;s:7:"version";s:3:"7.4";s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:7:"regions";a:9:{s:13:"sidebar_first";s:12:"Left sidebar";s:14:"sidebar_second";s:13:"Right sidebar";s:7:"content";s:7:"Content";s:6:"header";s:6:"Header";s:6:"footer";s:6:"Footer";s:11:"highlighted";s:11:"Highlighted";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";}s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:48:"themes/tests/update_test_subtheme/screenshot.png";s:3:"php";s:5:"5.2.4";s:11:"stylesheets";a:0:{}s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/tests/update_test_subtheme/update_test_subtheme.info') ;
UPDATE d7_system SET info='a:16:{s:4:"name";s:6:"Bartik";s:11:"description";s:48:"A flexible, recolorable theme with many regions.";s:7:"package";s:4:"Core";s:7:"version";s:3:"7.4";s:4:"core";s:3:"7.x";s:11:"stylesheets";a:2:{s:3:"all";a:3:{s:14:"css/layout.css";s:28:"themes/bartik/css/layout.css";s:13:"css/style.css";s:27:"themes/bartik/css/style.css";s:14:"css/colors.css";s:28:"themes/bartik/css/colors.css";}s:5:"print";a:1:{s:13:"css/print.css";s:27:"themes/bartik/css/print.css";}}s:7:"regions";a:17:{s:6:"header";s:6:"Header";s:4:"help";s:4:"Help";s:8:"page_top";s:8:"Page top";s:11:"page_bottom";s:11:"Page bottom";s:11:"highlighted";s:11:"Highlighted";s:8:"featured";s:8:"Featured";s:7:"content";s:7:"Content";s:13:"sidebar_first";s:13:"Sidebar first";s:14:"sidebar_second";s:14:"Sidebar second";s:14:"triptych_first";s:14:"Triptych first";s:15:"triptych_middle";s:15:"Triptych middle";s:13:"triptych_last";s:13:"Triptych last";s:18:"footer_firstcolumn";s:19:"Footer first column";s:19:"footer_secondcolumn";s:20:"Footer second column";s:18:"footer_thirdcolumn";s:19:"Footer third column";s:19:"footer_fourthcolumn";s:20:"Footer fourth column";s:6:"footer";s:6:"Footer";}s:8:"settings";a:1:{s:20:"shortcut_module_link";s:1:"0";}s:7:"project";s:6:"drupal";s:9:"datestamp";s:10:"1309397516";s:6:"engine";s:11:"phptemplate";s:8:"features";a:9:{i:0;s:4:"logo";i:1;s:7:"favicon";i:2;s:4:"name";i:3;s:6:"slogan";i:4;s:17:"node_user_picture";i:5;s:20:"comment_user_picture";i:6;s:25:"comment_user_verification";i:7;s:9:"main_menu";i:8;s:14:"secondary_menu";}s:10:"screenshot";s:28:"themes/bartik/screenshot.png";s:3:"php";s:5:"5.2.4";s:7:"scripts";a:0:{}s:14:"regions_hidden";a:2:{i:0;s:8:"page_top";i:1;s:11:"page_bottom";}}'
WHERE  (filename = 'themes/bartik/bartik.info') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin', router_path='admin', hidden='0', external='0', has_children='0', expanded='0', weight='9', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Administration', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='user-menu', plid='0', link_path='user', router_path='user', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='User account', options='a:1:{s:5:"alter";b:1;}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='0', link_path='filter/tips', router_path='filter/tips', hidden='1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Compose tips', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='0', link_path='node/%', router_path='node/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='0', link_path='node/add', router_path='node/add', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add content', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/appearance', router_path='admin/appearance', hidden='0', external='0', has_children='0', expanded='0', weight='-6', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Appearance', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:33:"Select and configure your themes.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/config', router_path='admin/config', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Configuration', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:20:"Administer settings.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/content', router_path='admin/content', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Content', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:24:"Find and manage content.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='user-menu', plid='2', link_path='user/register', router_path='user/register', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Create new account', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/index', router_path='admin/index', hidden='-1', external='0', has_children='0', expanded='0', weight='-18', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Index', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='user-menu', plid='2', link_path='user/login', router_path='user/login', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Log in', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET menu_name='user-menu', plid='0', link_path='user/logout', router_path='user/logout', hidden='0', external='0', has_children='0', expanded='0', weight='10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Log out', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/modules', router_path='admin/modules', hidden='0', external='0', has_children='0', expanded='0', weight='-2', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Modules', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:26:"Enable or disable modules.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='0', link_path='user/%', router_path='user/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='My account', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/people', router_path='admin/people', hidden='0', external='0', has_children='0', expanded='0', weight='-4', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='People', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Manage user accounts, roles, and permissions.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/reports', router_path='admin/reports', hidden='0', external='0', has_children='0', expanded='0', weight='5', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Reports', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:34:"View reports, updates, and errors.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='user-menu', plid='2', link_path='user/password', router_path='user/password', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Request new password', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/structure', router_path='admin/structure', hidden='0', external='0', has_children='0', expanded='0', weight='-8', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Structure', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Administer blocks, content types, menus, etc.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='0', link_path='admin/tasks', router_path='admin/tasks', hidden='-1', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Tasks', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET menu_name='management', plid='15', link_path='admin/people/create', router_path='admin/people/create', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add user', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '15') ;
UPDATE d7_menu_links SET menu_name='management', plid='18', link_path='admin/structure/block', router_path='admin/structure/block', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Blocks', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:79:"Configure what block content appears in your site''s sidebars and other regions.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '18') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='14', link_path='user/%/cancel', router_path='user/%/cancel', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Cancel account', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '14') ;
UPDATE d7_menu_links SET menu_name='management', plid='8', link_path='admin/content/node', router_path='admin/content/node', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Content', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '8') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/content', router_path='admin/config/content', hidden='0', external='0', has_children='0', expanded='0', weight='-15', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Content authoring', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:53:"Settings related to formatting and authoring content.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='18', link_path='admin/structure/types', router_path='admin/structure/types', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Content types', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:92:"Manage content types, including default status, front page promotion, comment settings, etc.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '18') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='4', link_path='node/%/delete', router_path='node/%/delete', hidden='-1', external='0', has_children='0', expanded='0', weight='1', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '4') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/development', router_path='admin/config/development', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Development', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:18:"Development tools.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='14', link_path='user/%/edit', router_path='user/%/edit', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '14') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='4', link_path='node/%/edit', router_path='node/%/edit', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '4') ;
UPDATE d7_menu_links SET menu_name='management', plid='6', link_path='admin/appearance/list', router_path='admin/appearance/list', hidden='-1', external='0', has_children='0', expanded='0', weight='-1', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:31:"Select and configure your theme";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '6') ;
UPDATE d7_menu_links SET menu_name='management', plid='13', link_path='admin/modules/list', router_path='admin/modules/list', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '13') ;
UPDATE d7_menu_links SET menu_name='management', plid='15', link_path='admin/people/people', router_path='admin/people/people', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:50:"Find and manage people interacting with your site.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '15') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/media', router_path='admin/config/media', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Media', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:12:"Media tools.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/people', router_path='admin/config/people', hidden='0', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='People', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:24:"Configure user accounts.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='15', link_path='admin/people/permissions', router_path='admin/people/permissions', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Permissions', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:64:"Determine access to features by selecting permissions for roles.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '15') ;
UPDATE d7_menu_links SET menu_name='management', plid='16', link_path='admin/reports/dblog', router_path='admin/reports/dblog', hidden='0', external='0', has_children='0', expanded='0', weight='-1', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Recent log messages', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"View events that have recently been logged.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '16') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/regional', router_path='admin/config/regional', hidden='0', external='0', has_children='0', expanded='0', weight='-5', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Regional and language', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:48:"Regional settings, localization and translation.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='4', link_path='node/%/revisions', router_path='node/%/revisions', hidden='-1', external='0', has_children='0', expanded='0', weight='2', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Revisions', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '4') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/search', router_path='admin/config/search', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Search and metadata', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:36:"Local site search, metadata and SEO.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='6', link_path='admin/appearance/settings', router_path='admin/appearance/settings', hidden='-1', external='0', has_children='0', expanded='0', weight='20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Settings', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:46:"Configure default and theme specific settings.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '6') ;
UPDATE d7_menu_links SET menu_name='management', plid='16', link_path='admin/reports/status', router_path='admin/reports/status', hidden='0', external='0', has_children='0', expanded='0', weight='-60', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Status report', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:74:"Get a status report about your site''s operation and any detected problems.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '16') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/system', router_path='admin/config/system', hidden='0', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='System', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:37:"General system related configuration.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='16', link_path='admin/reports/access-denied', router_path='admin/reports/access-denied', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Top ''access denied'' errors', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:35:"View ''access denied'' errors (403s).";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '16') ;
UPDATE d7_menu_links SET menu_name='management', plid='16', link_path='admin/reports/page-not-found', router_path='admin/reports/page-not-found', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Top ''page not found'' errors', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:36:"View ''page not found'' errors (404s).";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '16') ;
UPDATE d7_menu_links SET menu_name='management', plid='13', link_path='admin/modules/uninstall', router_path='admin/modules/uninstall', hidden='-1', external='0', has_children='0', expanded='0', weight='20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Uninstall', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '13') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/user-interface', router_path='admin/config/user-interface', hidden='0', external='0', has_children='0', expanded='0', weight='-15', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='User interface', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:38:"Tools that enhance the user interface.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='4', link_path='node/%/view', router_path='node/%/view', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='View', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '4') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='14', link_path='user/%/view', router_path='user/%/view', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='View', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '14') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/services', router_path='admin/config/services', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Web services', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:30:"Tools related to web services.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='7', link_path='admin/config/workflow', router_path='admin/config/workflow', hidden='0', external='0', has_children='0', expanded='0', weight='5', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Workflow', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Content workflow, editorial workflow tools.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '7') ;
UPDATE d7_menu_links SET menu_name='management', plid='34', link_path='admin/config/people/accounts', router_path='admin/config/people/accounts', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Account settings', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:109:"Configure default behavior of users, including registration requirements, e-mails, fields, and user pictures.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '34') ;
UPDATE d7_menu_links SET menu_name='management', plid='42', link_path='admin/config/system/actions', router_path='admin/config/system/actions', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Actions', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:41:"Manage the actions defined for your site.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '42') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/add', router_path='admin/structure/block/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='25', link_path='admin/structure/types/add', router_path='admin/structure/types/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add content type', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '25') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/bartik', router_path='admin/appearance/settings/bartik', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Bartik', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='39', link_path='admin/config/search/clean-urls', router_path='admin/config/search/clean-urls', hidden='0', external='0', has_children='0', expanded='0', weight='5', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Clean URLs', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Enable or disable clean URLs for your site.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '39') ;
UPDATE d7_menu_links SET menu_name='management', plid='42', link_path='admin/config/system/cron', router_path='admin/config/system/cron', hidden='0', external='0', has_children='0', expanded='0', weight='20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Cron', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:40:"Manage automatic site maintenance tasks.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '42') ;
UPDATE d7_menu_links SET menu_name='management', plid='37', link_path='admin/config/regional/date-time', router_path='admin/config/regional/date-time', hidden='0', external='0', has_children='0', expanded='0', weight='-15', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Date and time', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:44:"Configure display formats for date and time.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '37') ;
UPDATE d7_menu_links SET menu_name='management', plid='16', link_path='admin/reports/event/%', router_path='admin/reports/event/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Details', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '16') ;
UPDATE d7_menu_links SET menu_name='management', plid='33', link_path='admin/config/media/file-system', router_path='admin/config/media/file-system', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='File system', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:68:"Tell Drupal where to store uploaded files and how they are accessed.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '33') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/garland', router_path='admin/appearance/settings/garland', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Garland', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/global', router_path='admin/appearance/settings/global', hidden='-1', external='0', has_children='0', expanded='0', weight='-1', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Global settings', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='34', link_path='admin/config/people/ip-blocking', router_path='admin/config/people/ip-blocking', hidden='0', external='0', has_children='0', expanded='0', weight='10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='IP address blocking', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:28:"Manage blocked IP addresses.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '34') ;
UPDATE d7_menu_links SET menu_name='management', plid='33', link_path='admin/config/media/image-toolkit', router_path='admin/config/media/image-toolkit', hidden='0', external='0', has_children='0', expanded='0', weight='20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Image toolkit', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:74:"Choose which image toolkit to use if you have installed optional toolkits.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '33') ;
UPDATE d7_menu_links SET menu_name='management', plid='31', link_path='admin/modules/list/confirm', router_path='admin/modules/list/confirm', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '31') ;
UPDATE d7_menu_links SET menu_name='management', plid='25', link_path='admin/structure/types/list', router_path='admin/structure/types/list', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '25') ;
UPDATE d7_menu_links SET menu_name='management', plid='27', link_path='admin/config/development/logging', router_path='admin/config/development/logging', hidden='0', external='0', has_children='0', expanded='0', weight='-15', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Logging and errors', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:154:"Settings for logging and alerts modules. Various modules can route Drupal''s system events to different destinations, such as syslog, database, email, etc.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '27') ;
UPDATE d7_menu_links SET menu_name='management', plid='27', link_path='admin/config/development/maintenance', router_path='admin/config/development/maintenance', hidden='0', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Maintenance mode', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:62:"Take the site offline for maintenance or bring it back online.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '27') ;
UPDATE d7_menu_links SET menu_name='management', plid='27', link_path='admin/config/development/performance', router_path='admin/config/development/performance', hidden='0', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Performance', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:101:"Enable or disable page caching for anonymous users and set CSS and JS bandwidth optimization options.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '27') ;
UPDATE d7_menu_links SET menu_name='management', plid='35', link_path='admin/people/permissions/list', router_path='admin/people/permissions/list', hidden='-1', external='0', has_children='0', expanded='0', weight='-8', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Permissions', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:64:"Determine access to features by selecting permissions for roles.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '35') ;
UPDATE d7_menu_links SET menu_name='management', plid='49', link_path='admin/config/services/rss-publishing', router_path='admin/config/services/rss-publishing', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='RSS publishing', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:114:"Configure the site description, the number of items per feed and whether feeds should be titles/teasers/full-text.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '49') ;
UPDATE d7_menu_links SET menu_name='management', plid='37', link_path='admin/config/regional/settings', router_path='admin/config/regional/settings', hidden='0', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Regional settings', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:54:"Settings for the site''s default time zone and country.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '37') ;
UPDATE d7_menu_links SET menu_name='management', plid='35', link_path='admin/people/permissions/roles', router_path='admin/people/permissions/roles', hidden='-1', external='0', has_children='0', expanded='0', weight='-5', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Roles', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:30:"List, edit, or add user roles.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '35') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/seven', router_path='admin/appearance/settings/seven', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Seven', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='42', link_path='admin/config/system/site-information', router_path='admin/config/system/site-information', hidden='0', external='0', has_children='0', expanded='0', weight='-20', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Site information', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:104:"Change site name, e-mail address, slogan, default front page, and number of posts per page, error pages.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '42') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/stark', router_path='admin/appearance/settings/stark', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Stark', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/test_theme', router_path='admin/appearance/settings/test_theme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Test theme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='24', link_path='admin/config/content/formats', router_path='admin/config/content/formats', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Text formats', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:127:"Configure how content input by users is filtered, including allowed HTML tags. Also allows enabling of module-provided filters.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '24') ;
UPDATE d7_menu_links SET menu_name='management', plid='45', link_path='admin/modules/uninstall/confirm', router_path='admin/modules/uninstall/confirm', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Uninstall', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '45') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/update_test_basetheme', router_path='admin/appearance/settings/update_test_basetheme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Update test base theme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='management', plid='40', link_path='admin/appearance/settings/update_test_subtheme', router_path='admin/appearance/settings/update_test_subtheme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Update test subtheme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '40') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='28', link_path='user/%/edit/account', router_path='user/%/edit/account', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Account', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '28') ;
UPDATE d7_menu_links SET menu_name='management', plid='78', link_path='admin/config/content/formats/%', router_path='admin/config/content/formats/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '78') ;
UPDATE d7_menu_links SET menu_name='management', plid='78', link_path='admin/config/content/formats/add', router_path='admin/config/content/formats/add', hidden='-1', external='0', has_children='0', expanded='0', weight='1', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add text format', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '78') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/bartik', router_path='admin/structure/block/list/bartik', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Bartik', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='52', link_path='admin/config/system/actions/configure', router_path='admin/config/system/actions/configure', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Configure an advanced action', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '52') ;
UPDATE d7_menu_links SET menu_name='management', plid='25', link_path='admin/structure/types/manage/%', router_path='admin/structure/types/manage/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit content type', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '25') ;
UPDATE d7_menu_links SET menu_name='management', plid='58', link_path='admin/config/regional/date-time/formats', router_path='admin/config/regional/date-time/formats', hidden='-1', external='0', has_children='0', expanded='0', weight='-9', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Formats', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:51:"Configure display format strings for date and time.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '58') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/garland', router_path='admin/structure/block/list/garland', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Garland', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='78', link_path='admin/config/content/formats/list', router_path='admin/config/content/formats/list', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='List', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '78') ;
UPDATE d7_menu_links SET menu_name='management', plid='52', link_path='admin/config/system/actions/manage', router_path='admin/config/system/actions/manage', hidden='-1', external='0', has_children='0', expanded='0', weight='-2', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Manage actions', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:41:"Manage the actions defined for your site.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '52') ;
UPDATE d7_menu_links SET menu_name='management', plid='51', link_path='admin/config/people/accounts/settings', router_path='admin/config/people/accounts/settings', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Settings', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '51') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/seven', router_path='admin/structure/block/list/seven', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Seven', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/stark', router_path='admin/structure/block/list/stark', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Stark', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/test_theme', router_path='admin/structure/block/list/test_theme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Test theme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='58', link_path='admin/config/regional/date-time/types', router_path='admin/config/regional/date-time/types', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Types', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:44:"Configure display formats for date and time.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '58') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/update_test_basetheme', router_path='admin/structure/block/list/update_test_basetheme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Update test base theme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/list/update_test_subtheme', router_path='admin/structure/block/list/update_test_subtheme', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Update test subtheme', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='38', link_path='node/%/revisions/%/delete', router_path='node/%/revisions/%/delete', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete earlier revision', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '38') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='38', link_path='node/%/revisions/%/revert', router_path='node/%/revisions/%/revert', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Revert to earlier revision', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '38') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='38', link_path='node/%/revisions/%/view', router_path='node/%/revisions/%/view', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Revisions', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '38') ;
UPDATE d7_menu_links SET menu_name='management', plid='89', link_path='admin/structure/block/list/garland/add', router_path='admin/structure/block/list/garland/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '89') ;
UPDATE d7_menu_links SET menu_name='management', plid='93', link_path='admin/structure/block/list/seven/add', router_path='admin/structure/block/list/seven/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '93') ;
UPDATE d7_menu_links SET menu_name='management', plid='94', link_path='admin/structure/block/list/stark/add', router_path='admin/structure/block/list/stark/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '94') ;
UPDATE d7_menu_links SET menu_name='management', plid='95', link_path='admin/structure/block/list/test_theme/add', router_path='admin/structure/block/list/test_theme/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '95') ;
UPDATE d7_menu_links SET menu_name='management', plid='97', link_path='admin/structure/block/list/update_test_basetheme/add', router_path='admin/structure/block/list/update_test_basetheme/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '97') ;
UPDATE d7_menu_links SET menu_name='management', plid='98', link_path='admin/structure/block/list/update_test_subtheme/add', router_path='admin/structure/block/list/update_test_subtheme/add', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '98') ;
UPDATE d7_menu_links SET menu_name='management', plid='96', link_path='admin/config/regional/date-time/types/add', router_path='admin/config/regional/date-time/types/add', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add date type', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:18:"Add new date type.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '96') ;
UPDATE d7_menu_links SET menu_name='management', plid='88', link_path='admin/config/regional/date-time/formats/add', router_path='admin/config/regional/date-time/formats/add', hidden='-1', external='0', has_children='0', expanded='0', weight='-10', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Add format', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:43:"Allow users to add additional date formats.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '88') ;
UPDATE d7_menu_links SET menu_name='management', plid='21', link_path='admin/structure/block/manage/%/%', router_path='admin/structure/block/manage/%/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Configure block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '21') ;
UPDATE d7_menu_links SET menu_name='navigation', plid='22', link_path='user/%/cancel/confirm/%/%', router_path='user/%/cancel/confirm/%/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Confirm account cancellation', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '22') ;
UPDATE d7_menu_links SET menu_name='management', plid='87', link_path='admin/structure/types/manage/%/delete', router_path='admin/structure/types/manage/%/delete', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '87') ;
UPDATE d7_menu_links SET menu_name='management', plid='63', link_path='admin/config/people/ip-blocking/delete/%', router_path='admin/config/people/ip-blocking/delete/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete IP address', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '63') ;
UPDATE d7_menu_links SET menu_name='management', plid='52', link_path='admin/config/system/actions/delete/%', router_path='admin/config/system/actions/delete/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete action', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:17:"Delete an action.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '52') ;
UPDATE d7_menu_links SET menu_name='management', plid='73', link_path='admin/people/permissions/roles/delete/%', router_path='admin/people/permissions/roles/delete/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete role', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '73') ;
UPDATE d7_menu_links SET menu_name='management', plid='83', link_path='admin/config/content/formats/%/disable', router_path='admin/config/content/formats/%/disable', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Disable text format', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '83') ;
UPDATE d7_menu_links SET menu_name='management', plid='87', link_path='admin/structure/types/manage/%/edit', router_path='admin/structure/types/manage/%/edit', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '87') ;
UPDATE d7_menu_links SET menu_name='management', plid='73', link_path='admin/people/permissions/roles/edit/%', router_path='admin/people/permissions/roles/edit/%', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit role', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '73') ;
UPDATE d7_menu_links SET menu_name='management', plid='110', link_path='admin/structure/block/manage/%/%/configure', router_path='admin/structure/block/manage/%/%/configure', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Configure block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '110') ;
UPDATE d7_menu_links SET menu_name='management', plid='110', link_path='admin/structure/block/manage/%/%/delete', router_path='admin/structure/block/manage/%/%/delete', hidden='-1', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete block', options='a:0:{}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '110') ;
UPDATE d7_menu_links SET menu_name='management', plid='88', link_path='admin/config/regional/date-time/formats/%/delete', router_path='admin/config/regional/date-time/formats/%/delete', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete date format', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:47:"Allow users to delete a configured date format.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '88') ;
UPDATE d7_menu_links SET menu_name='management', plid='96', link_path='admin/config/regional/date-time/types/%/delete', router_path='admin/config/regional/date-time/types/%/delete', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Delete date type', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Allow users to delete a configured date type.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '96') ;
UPDATE d7_menu_links SET menu_name='management', plid='88', link_path='admin/config/regional/date-time/formats/%/edit', router_path='admin/config/regional/date-time/formats/%/edit', hidden='0', external='0', has_children='0', expanded='0', weight='0', depth='1', p1='1', p2='0', p3='0', p4='0', p5='0', p6='0', p7='0', p8='0', p9='0', module='system', link_title='Edit date format', options='a:1:{s:10:"attributes";a:1:{s:5:"title";s:45:"Allow users to edit a configured date format.";}}', customized='0'
WHERE  (mlid = '1') ;
UPDATE d7_menu_links SET has_children='1'
WHERE  (mlid = '88') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '2') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '15') ;
UPDATE d7_menu_links SET has_children='0'
WHERE  (mlid = '8') ;
UPDATE d7_batch SET batch='a:12:{s:4:"sets";a:1:{i:0;a:14:{s:7:"sandbox";a:0:{}s:7:"results";a:9:{i:0;s:6:"filter";i:1;s:17:"field_sql_storage";i:2;s:4:"user";i:3;s:4:"node";i:4;s:5:"field";i:5;s:4:"text";i:6;s:5:"block";i:7;s:5:"dblog";i:8;s:7:"minimal";}s:7:"success";b:1;s:5:"start";d:1311348758.975739955902099609375;s:7:"elapsed";d:158759.98000000001047737896442413330078125;s:5:"title";s:17:"Installing Drupal";s:13:"error_message";s:42:"The installation has encountered an error.";s:8:"finished";s:33:"_install_profile_modules_finished";s:12:"init_message";s:24:"Initializing.<br/>&nbsp;";s:16:"progress_message";s:29:"Completed @current of @total.";s:3:"css";a:0:{}s:5:"total";i:9;s:5:"count";i:0;s:5:"queue";a:2:{s:4:"name";s:16:"drupal_batch:1:0";s:5:"class";s:10:"BatchQueue";}}}s:16:"has_form_submits";b:0;s:11:"current_set";i:0;s:11:"progressive";b:1;s:3:"url";s:65:"http://localhost/drupal-7.4/install.php?profile=minimal&locale=en";s:11:"url_options";a:0:{}s:10:"source_url";s:4:"node";s:8:"redirect";s:37:"install.php?profile=minimal&locale=en";s:5:"theme";s:5:"seven";s:17:"redirect_callback";s:11:"drupal_goto";s:2:"id";s:1:"1";s:13:"error_message";s:142:"Please continue to <a href="http://localhost/drupal-7.4/install.php?profile=minimal&amp;locale=en&amp;id=1&amp;op=finished">the error page</a>";}'
WHERE  (bid = '1') ;
UPDATE d7_variable SET value='a:0:{}'
WHERE ( (name = 'menu_expanded') );
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_semaphore 
WHERE  (value = '13228538164e299701e4fb29.21264293') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.archiver.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.mail.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.queue.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.tar.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.updater.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/system/system.test') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/user/user.module') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/user/user.test') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/filetransfer/filetransfer.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/filetransfer/ssh.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/filetransfer/ftp.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/filetransfer/local.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/mysql/query.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/mysql/database.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/mysql/schema.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/mysql/install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/pgsql/select.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/pgsql/query.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/pgsql/database.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/pgsql/schema.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/pgsql/install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/parelastic/select.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/parelastic/query.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/parelastic/database.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/parelastic/schema.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/parelastic/install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/sqlite/select.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/sqlite/query.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/sqlite/database.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/sqlite/schema.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/sqlite/install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/prefetch.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/select.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/log.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/query.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/database.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/database/schema.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/language.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/file.mimetypes.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/xmlrpcs.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/mail.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/path.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/locale.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/xmlrpc.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/theme.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/menu.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/graph.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/form.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/batch.queue.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/archiver.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/tablesort.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/session.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/update.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/stream_wrappers.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/password.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/ajax.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/token.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/entity.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/authorize.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/iso.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/cache-install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/bootstrap.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/unicode.entities.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/module.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/install.core.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/unicode.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/batch.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/common.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/theme.maintenance.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/actions.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/pager.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/date.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/errors.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/utility.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/cache.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/registry.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/install.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/image.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/file.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/lock.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'includes/updater.inc') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '17282909654e2997f839bc89.47473471') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_variable 
WHERE  (name = 'install_profile_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_semaphore 
WHERE  (value = '17282909654e2997f839bc89.47473471') ;
DELETE FROM d7_sequences WHERE value < '1';
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '7511433144e29980c267b02.15736042') ;
DELETE FROM d7_semaphore 
WHERE  (value = '7511433144e29980c267b02.15736042') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '3348278214e2998139d94c6.24180592') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/filter/filter.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'filter\_formats%' ESCAPE '\\') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'filter\_list\_format%' ESCAPE '\\') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_queue 
WHERE  (item_id = '1') ;
DELETE FROM d7_semaphore 
WHERE  (value = '3348278214e2998139d94c6.24180592') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '13528603224e29982b2fdbf9.30951523') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/field/modules/field_sql_storage/field_sql_storage.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_queue 
WHERE  (item_id = '2') ;
DELETE FROM d7_semaphore 
WHERE  (value = '13528603224e29982b2fdbf9.30951523') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '13948105504e299838c4b607.67968046') ;
DELETE FROM d7_queue 
WHERE  (item_id = '3') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/node/node.module') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/node/node.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_queue 
WHERE  (item_id = '4') ;
DELETE FROM d7_semaphore 
WHERE  (value = '13948105504e299838c4b607.67968046') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '589947794e29985462fca9.97850267') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/field/field.module') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/field/field.attach.inc') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/field/tests/field.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid LIKE 'field\_info\_types:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid = 'field_info_fields') ;
DELETE FROM d7_queue 
WHERE  (item_id = '5') ;
DELETE FROM d7_semaphore 
WHERE  (value = '589947794e29985462fca9.97850267') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '8129432104e29986a7695f5.12489159') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/field/modules/text/text.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid LIKE 'field\_info\_types:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid = 'field_info_fields') ;
DELETE FROM d7_queue 
WHERE  (item_id = '6') ;
DELETE FROM d7_semaphore 
WHERE  (value = '8129432104e29986a7695f5.12489159') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '14294613174e29987bae7188.98654661') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/block/block.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid LIKE 'field\_info\_types:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid = 'field_info_fields') ;
DELETE FROM d7_queue 
WHERE  (item_id = '7') ;
DELETE FROM d7_semaphore 
WHERE  (value = '14294613174e29987bae7188.98654661') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '11340947494e2998919cb7e1.30424089') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'modules/dblog/dblog.test') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid LIKE 'field\_info\_types:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid = 'field_info_fields') ;
DELETE FROM d7_queue 
WHERE  (item_id = '8') ;
DELETE FROM d7_semaphore 
WHERE  (value = '11340947494e2998919cb7e1.30424089') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '8037725394e2998a584f1b3.96730055') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_registry 
WHERE  (filename = 'profiles/minimal/minimal.profile') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_field;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'entity\_info:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid LIKE 'field\_info\_types:%' ESCAPE '\\') ;
DELETE FROM d7_cache_field 
WHERE  (cid = 'field_info_fields') ;
DELETE FROM d7_queue 
WHERE  (item_id = '9') ;
DELETE FROM d7_semaphore 
WHERE  (value = '8037725394e2998a584f1b3.96730055') ;
DELETE FROM d7_drupal_install_test;
DELETE FROM d7_semaphore 
WHERE  (name = 'variable_init') AND (value = '87318634e2998bb5199a1.76382437') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'hook_info') ;
DELETE FROM d7_variable 
WHERE  (name = 'drupal_css_cache_files') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_variable 
WHERE  (name = 'javascript_parsed') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_variable 
WHERE  (name = 'drupal_js_cache_files') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'bootstrap_modules') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'system_list') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'theme\_registry%' ESCAPE '\\') ;
DELETE FROM d7_cache 
WHERE  (cid LIKE 'node\_types:%' ESCAPE '\\') ;
DELETE FROM d7_menu_router;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:management:%' ESCAPE '\\') ;
DELETE FROM d7_cache_block 
WHERE  (expire <> '0') AND (expire < '1311348919') ;
DELETE FROM d7_cache_page 
WHERE  (expire <> '0') AND (expire < '1311348919') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:user-menu:%' ESCAPE '\\') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:navigation:%' ESCAPE '\\') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '3') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '5') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '9') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '10') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '11') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '12') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '17') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '2') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '19') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '20') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '23') ;
DELETE FROM d7_menu_links 
WHERE  (mlid = '8') ;
DELETE FROM d7_semaphore 
WHERE  (value = '87318634e2998bb5199a1.76382437') ;
DELETE FROM d7_cache_block 
WHERE  (expire <> '0') AND (expire < '1311348919') ;
DELETE FROM d7_cache_page 
WHERE  (expire <> '0') AND (expire < '1311348919') ;
DELETE FROM d7_cache_bootstrap 
WHERE  (cid = 'variables') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:navigation:%' ESCAPE '\\') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:management:%' ESCAPE '\\') ;
DELETE FROM d7_cache_menu 
WHERE  (cid LIKE 'links:user-menu:%' ESCAPE '\\') ;
