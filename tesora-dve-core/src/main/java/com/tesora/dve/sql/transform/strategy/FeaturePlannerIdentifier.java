package com.tesora.dve.sql.transform.strategy;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */



public enum FeaturePlannerIdentifier {

	DEGENERATE,
	DISTRIBUTION_KEY_EXECUTE,
	GROUP_BY,
	ORDERBY_LIMIT,
	SESSION,
	ORDER_BY,
	JOIN,
	GENERIC_AGG,
	MYSQL_GROUPBY_EXT,
	SINGLE_SITE,
	INSERT_INTO_SELECT,
	REPLACE_INTO,
	UPDATE_DIST_VECT,
	NESTED_QUERY,
	NESTED_QUERY_BROADCAST,
	PROJ_CORSUB,
	WC_CORSUB,
	HAVING,
	UNION,
	DELETE,
	INFO_SCHEMA,
	TIMESTAMP_VARIABLE,
	CONTAINER_BASE_TABLE,
	NULL_LITERAL,
	NATURAL_JOIN_REWRITE,
	JOIN_SIMPLIFICATION,
	VIEW,
	TRUNCATE_MT_TABLE,
	ADHOC;
}
