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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

public abstract class SchemaMirrorTest extends SchemaTest {

	protected TestResource sysResource;
	protected TestResource checkResource;
	protected TestResource nativeResource;

	protected boolean useUTF8 = true;

	public void setUseUTF8(boolean useUTF8) {
		this.useUTF8 = useUTF8;
	}

	protected ProjectDDL getMultiDDL() {
		return null;
	}
	
	protected ProjectDDL getSingleDDL() {
		return null;
	}
	
	protected ProjectDDL getNativeDDL() {
		return null;
	}
	
	protected static void setup(ProjectDDL multiDDL, ProjectDDL singleDDL, ProjectDDL nativeDDL, List<MirrorTest> populate) throws Throwable {
		ArrayList<ProjectDDL> exists = new ArrayList<ProjectDDL>();
		if (multiDDL != null) exists.add(multiDDL);
		if (singleDDL != null) exists.add(singleDDL);
		if (nativeDDL != null) exists.add(nativeDDL);
		PETest.projectSetup(exists.toArray(new ProjectDDL[0]));
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		// putting the load into the per class portion
		// then we can have real test cases (almost)
		PortalDBHelperConnectionResource sysconn = (multiDDL == null ? null : new PortalDBHelperConnectionResource());
		PortalDBHelperConnectionResource checkconn = (singleDDL == null ? null : new PortalDBHelperConnectionResource());
		DBHelperConnectionResource nativeConn = (nativeDDL == null ? null : new DBHelperConnectionResource());
		try {
			TestResource smr = (multiDDL == null ? null : new TestResource(sysconn, multiDDL));
			TestResource cmr = (singleDDL == null ? null : new TestResource(checkconn, singleDDL));
			TestResource nmr = (nativeDDL == null ? null : new TestResource(nativeConn, nativeDDL));
			ArrayList<TestResource> trs = new ArrayList<TestResource>();
			if (smr != null)
				trs.add(smr);
			if (cmr != null)
				trs.add(cmr);
			if (nmr != null)
				trs.add(nmr);
			for (TestResource tr : trs)
				tr.getDDL().create(tr);
			for (TestResource tr : trs) {
				for (MirrorTest mt : populate)
					mt.execute(tr, null);
			}
		} finally {
			if (sysconn != null)
				sysconn.disconnect();
			if (checkconn != null)
				checkconn.disconnect();
			if (nativeConn != null)
				nativeConn.disconnect();
		}
	}
	
	@Before
	public void connect() throws Throwable {
		if (getMultiDDL() != null)
			sysResource = new TestResource(createConnection(getMultiDDL()), getMultiDDL());
		if (getSingleDDL() != null)
			checkResource = new TestResource(createConnection(getSingleDDL()), getSingleDDL());
		if (getNativeDDL() != null)
			nativeResource = new TestResource(createConnection(getNativeDDL()), getNativeDDL());
		TestResource[] trs = new TestResource[] { sysResource, checkResource, nativeResource };
		for(TestResource tr : trs)
			if (tr != null)
				onConnect(tr);
	}

	public ConnectionResource getMultiConnection() {
		if (sysResource == null) return null;
		return sysResource.getConnection();
	}
	
	public ConnectionResource getSingleConnection() {
		if (checkResource == null) return null;
		return checkResource.getConnection();
	}
	
	public ConnectionResource getNativeConnection() {
		if (nativeResource == null) return null;
		return nativeResource.getConnection();
	}
	
	protected ConnectionResource createConnection(ProjectDDL p) throws Throwable {
		if (p == getNativeDDL()) 
			return new DBHelperConnectionResource(useUTF8);
		else if (p == getMultiDDL() || p == getSingleDDL()) 
			return new PortalDBHelperConnectionResource(useUTF8);
		
		throw new PEException("ProjectDDL of unknown type " + p.getClass());
	}
	
	protected void onConnect(TestResource tr) throws Throwable {
		tr.getConnection().execute("use " + tr.getDDL().getDatabaseName());
	}
	
	@After
	public void disconnect() throws Throwable {
		if (sysResource != null)
			sysResource.getConnection().disconnect();
		if (checkResource != null)
			checkResource.getConnection().disconnect();
		if (nativeResource != null)
			nativeResource.getConnection().disconnect();
		sysResource = null;
		checkResource = null;
		nativeResource = null;
	}
	
	protected void runTest(MirrorTest single) throws Throwable {
		runTest(Collections.singletonList(single));
	}
	
	protected void runTest(List<MirrorTest> tests) throws Throwable {
		ListOfPairs<TestResource, TestResource> c = new ListOfPairs<TestResource,TestResource>();
		c.add(nativeResource, checkResource);
		c.add(nativeResource, sysResource);
		runTest(tests, c, true);
	}

	protected ListOfPairs<TestResource,TestResource> getTestConfig() {
		ListOfPairs<TestResource, TestResource> c = new ListOfPairs<TestResource,TestResource>();
		c.add(nativeResource, checkResource);
		c.add(nativeResource, sysResource);
		return c;
	}
	
	protected void runTest(List<MirrorTest> tests, ListOfPairs<TestResource,TestResource> configs, boolean omitMissingRHS) throws Throwable {
		for(Pair<TestResource, TestResource> p : configs) {
			if (p.getSecond() == null && omitMissingRHS) continue;
			for(MirrorTest mt : tests) {
				mt.execute(p.getFirst(), p.getSecond());
			}
		}
	}

	protected void runSerialNoMirror(List<MirrorTest> tests) throws Throwable {
		ListOfPairs<TestResource, TestResource> c = new ListOfPairs<TestResource, TestResource>();
		if (nativeResource != null) c.add(nativeResource,null);
		if (checkResource != null) c.add(checkResource,null);
		if (sysResource != null) c.add(sysResource,null);
		runTest(tests, c, false);
	}
	
	/**
	 * Facilitates extended packet mirror testing by handling necessary variable
	 * and setting updates.
	 */
	protected class ExtendedPacketTester {

		private static final long VARIABLE_REFRESH_WAIT_TIME_SEC = 10;
		private static final long DEFAULT_MAX_ALLOWED_PACKET_SIZE = 16777216;

		private final long maxAllowedPacketSize;
		private boolean useFormatedOutput = false;

		private final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		/**
		 * @param maxAllowedPacketSize
		 *            Value of 'max_allowed_packet' global variable used in the
		 *            test.
		 */
		public ExtendedPacketTester(final long maxAllowedPacketSize) {
			this.maxAllowedPacketSize = maxAllowedPacketSize;
		}

		/**
		 * Execute the mirror tests.
		 */
		public void runTests() throws Throwable {
			try {
				resetMaxAllowedPacketVariable(this.maxAllowedPacketSize);
				ResourceResponse.BLOB_COLUMN.useFormatedOutput(this.useFormatedOutput);

				// Refresh the 'max_allowed_packet' variable.
				disconnect();
				TimeUnit.SECONDS.sleep(VARIABLE_REFRESH_WAIT_TIME_SEC); //TODO: hack to deal with race condition where fast disconnect/reconnect after a response still picks up old value. -sgossard
				connect();

				test();
			} finally {
				ResourceResponse.BLOB_COLUMN.useFormatedOutput(true);
				resetMaxAllowedPacketVariable(DEFAULT_MAX_ALLOWED_PACKET_SIZE);
			}
		}

		/**
		 * Run mirror tests.
		 */
		protected void test() throws Throwable {
			SchemaMirrorTest.this.runTest(this.tests);
		}

		/**
		 * Add MirrorTest to the suite.
		 */
		public void add(final MirrorTest test) {
			this.tests.add(test);
		}

		/**
		 * Add MirrorTests to the suite.
		 */
		public void addAll(final Collection<MirrorTest> tests) {
			this.tests.addAll(tests);
		}

		/**
		 * Remove all MirrorTest currently in the suite.
		 */
		public void clear() {
			this.tests.clear();
		}

		/**
		 * Formatted output on large packets may lead to heap exhaustion in
		 * StringBuilder.
		 */
		public void enableFormatedOutput() {
			this.useFormatedOutput = true;
		}

		private void resetMaxAllowedPacketVariable(final long value) throws Throwable {
			SchemaMirrorTest.this.runTest(new StatementMirrorProc("SET GLOBAL max_allowed_packet = " + String.valueOf(value)));
		}

	}

}
