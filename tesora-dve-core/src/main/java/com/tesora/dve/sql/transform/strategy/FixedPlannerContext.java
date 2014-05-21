// OS_STATUS: public
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

import com.tesora.dve.sql.schema.SchemaContext;

public class FixedPlannerContext {

	private final SchemaContext sc;
	private final TempGroupManager tempGroupManager;
	private final TempTableSanity sanity;

	public FixedPlannerContext(SchemaContext sc) {
		this.sc = sc;
		this.tempGroupManager = new TempGroupManager();
		this.sanity = new TempTableSanity();
	}
	
	public SchemaContext getContext() {
		return sc;
	}
	
	public TempGroupManager getTempGroupManager() {
		return tempGroupManager;
	}
	
	public TempTableSanity getTempTableSanity() {
		return sanity;
	}
}
