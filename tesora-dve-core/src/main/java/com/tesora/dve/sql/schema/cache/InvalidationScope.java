// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

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

// when invalidating the cache for a particular key, what other keys should we also invalidate
public enum InvalidationScope {

	// a global invalidation tosses the whole cache
	GLOBAL,
	// a local invalidation tosses just the key
	LOCAL,
	// a cascading invalidation tosses the key plus anything enclosed by it
	// so for a database it would toss the tables, for a tenant it tosses the tables, etc.
	CASCADE
}
