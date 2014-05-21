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

import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class CandidateCachedPlan {

	private final ExecutionPlan ep;
	private final boolean cacheIfNotFound;
	private final CandidateParser shrunk;
	
	public CandidateCachedPlan(CandidateParser cp, ExecutionPlan ep, boolean encache) {
		this.ep = ep;
		this.shrunk = cp;
		this.cacheIfNotFound = encache;
	}
	
	public ExecutionPlan getPlan() { return ep; }
	public boolean tryCaching() { return cacheIfNotFound; }
	public CandidateParser getShrunk() { return shrunk; }
	
}
