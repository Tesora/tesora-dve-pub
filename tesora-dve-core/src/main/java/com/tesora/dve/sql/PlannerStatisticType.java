// OS_STATUS: public
package com.tesora.dve.sql;

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

public enum PlannerStatisticType {

	// bump this when the candidate parser fails
	UNCACHEABLE_CANDIDATE_PARSE,

	// bump this when the candidate parser works but the full parser comes
	// up with a different number of literals
	UNCACHEABLE_INVALID_CANDIDATE,

    //bump this when the stmt is not cacheable because the number of literals exceeded the
    // "max_cached_plan_literals" setting.
    UNCACHEABLE_EXCEEDED_LITERAL_LIMIT,

    //bump this when the stmt is not cacheable because it contains a dynamic function
    //that must be interpreted at execution time, such as UUID() and LAST_INSERT_ID()
    UNCACHEABLE_DYNAMIC_FUNCTION,

	// bump this when the stmt is not cacheable for some other reason
	// note that this doesn't include ddl, for which it would not match the cache and
	// also would not try to cache it
	UNCACHEABLE_PLAN,


	
}
