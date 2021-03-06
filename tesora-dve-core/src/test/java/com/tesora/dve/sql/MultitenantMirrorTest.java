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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class MultitenantMirrorTest extends SchemaMirrorTest {

	private static final String TENANT_NAME = "highfive";
	
	private static final int SITES = 3;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema").withMTMode(MultitenantMode.ADAPTIVE);
	private static final NativeDDL nativeDDL =
		new NativeDDL(TENANT_NAME);
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,initialize());
	}
	
	// our initialization is to create a tenant; we'll do the other stuff in the tests
	protected static List<MirrorTest> initialize() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				if (nativeDDL.equals(mr.getDDL())) return null;
				mr.getConnection().execute("create tenant " + TENANT_NAME + " '" + TENANT_NAME + "'");
				return null;
			}
			
		});
		return out;
	}
	

	@Test
	public void testPE1473() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("use " + TENANT_NAME));
		tests.add(new StatementMirrorProc("create table pe1473 (id int auto_increment, payload varchar(32), primary key (id))"));
		tests.add(new StatementMirrorProc("insert into pe1473 (payload) values ('one'),('two'),('three')"));
		tests.add(new StatementMirrorFun("select * from pe1473 order by id"));
		tests.add(new StatementMirrorProc("drop table pe1473"));
		runTest(tests);
	}
	
}
