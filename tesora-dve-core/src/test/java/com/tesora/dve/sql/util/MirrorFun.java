package com.tesora.dve.sql.util;

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

import com.tesora.dve.sql.SchemaTest;

public abstract class MirrorFun extends MirrorTest {
	
	protected boolean ignoreOrder = false;
	protected boolean explainOnFailure = true;
	protected boolean omitMetadataChecks = false;
	
	public MirrorFun(boolean expl) {
		this(false,expl);
	}

	public MirrorFun(boolean unordered, boolean ignoreMD, boolean explain) {
		ignoreOrder = unordered;
		omitMetadataChecks = ignoreMD;
		explainOnFailure = explain;
	}
	
	public MirrorFun(boolean unordered, boolean explain) {
		this(unordered,false,explain);
	}
	
	public String getContext() {
		return "mirrorfun";
	}
	
	public abstract ResourceResponse execute(TestResource mr) throws Throwable;

	@Override
	public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
		ResourceResponse cr = execute(checkdb);
		ResourceResponse sr = execute(sysdb);
		if (sysdb == null) return;
		if (cr != null && sr != null) {
			try {
				cr.assertEqualResponse(getContext(), sr);
				cr.assertEqualResults(getContext(), ignoreOrder, omitMetadataChecks, sr);
			} catch (AssertionError ae) {
				// annotate if we actually can get the underlying statement, otherwise don't bother
				if (explainOnFailure)
					throw SchemaTest.annotateFailureWithPlan(ae,getContext(),checkdb,sysdb,null);
				throw ae;
			}
		}
	}
}