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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.siteprovider.SiteProviderPlugin;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderFactory;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variable.ScopedVariableHandler;
import com.tesora.dve.variable.VariableConfig;
import com.tesora.dve.variable.VariableInfo;
import com.tesora.dve.worker.SiteManagerCommand;

public class GroupProviderDDLTest extends SchemaTest {

	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),"schema");
	
	static GroupProviderDDLTest thisTest;
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}


	private static final String testProviderName = "floyd";

	SiteManagerCommand lastPrepare = null;
	SiteManagerCommand lastUpdate = null;
	SiteManagerCommand lastShow = null;
	SiteManagerCommand lastRollback = null;
	String lastSetVariableName = null;
	String lastSetVariableValue = null;
	private ProxyConnectionResource conn = null;
	
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		thisTest = this;
	}
	
	@After
	public void after() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		thisTest = null;

		SiteProviderFactory.deregister(testProviderName);
	}
	
	@Test
	public void testGroupProviderDDL() throws Throwable {
		update("CREATE DYNAMIC SITE PROVIDER " + testProviderName + " USING plugin='" + GroupProviderDDLTestProvider.class.getName() + "' other=1 other.another='ballyfoyle' crop=true",
				Command.CREATE, testProviderName, 
				new Object[] {"other", new Long(1), "other.another", "ballyfoyle", "crop", Boolean.TRUE });
		conn.assertResults("show dynamic site providers", 
				br(nr,"floyd",GroupProviderDDLTestProvider.class.getName(),"YES",
				   nr,OnPremiseSiteProvider.DEFAULT_NAME, OnPremiseSiteProvider.class.getCanonicalName(), "YES"));
		update("ALTER DYNAMIC SITE PROVIDER " + testProviderName + " first='ballyfoyle' second='tinahely' third='paddy'",
				Command.ALTER, testProviderName,
				new Object[] { "first", "ballyfoyle", "second", "tinahely", "third", "paddy" });
		Object[] rollBackOpts = new Object[] { "rollback", Boolean.FALSE, "rollback", Boolean.TRUE };
		try {
			update("ALTER DYNAMIC SITE PROVIDER " + testProviderName + " rollback=false rollback=true",
					Command.ALTER, testProviderName, rollBackOpts);
			fail("rollback exception not propagated");
		} catch (PEException pe) {
			assertPEException(pe, "Test rollback");
			assertNotNull(lastRollback);
			assertCommandBlock(Command.ALTER, testProviderName, rollBackOpts, lastRollback);
		}
		show("SHOW DYNAMIC SITE PROVIDER " + testProviderName + " cmd='sites'",
				Command.SHOW, testProviderName, new Object[] { "cmd", "sites" });

		// variable tests
		conn.execute("alter dynamic site provider floyd set tic = 'ticklish'");
		assertSetVariable("tic","ticklish");
		conn.execute("alter dynamic site provider floyd set tac = 'spackling'");
		assertSetVariable("tac","spackling");
		conn.assertResults("show dynamic site provider variables floyd",
				br(nr,"tac","spackling",
				   nr,"tic","ticklish"));
		conn.assertResults("show dynamic site provider variables",
				br(nr,"floyd","tac","spackling",
				   nr,"floyd","tic","ticklish"));
		conn.assertResults("show dynamic site provider variables like '%ac'",
				br(nr,"floyd","tac","spackling"));
		
		try {
			conn.execute("alter dynamic site provider floyd set dne = 'unknowable'");
		} catch (PENotFoundException re) {
			assertException(re,PENotFoundException.class,"Variable \"dne\" not found");
		}

		// make sure we can handle attributes with funny names
		conn.execute("alter dynamic site provider floyd  cmd='set status' site='inst1' pool='abc' count='15'");
		
		conn.execute("alter dynamic site provider floyd  cmd='set status' pool=null");
		
		update("DROP DYNAMIC SITE PROVIDER " + testProviderName,
				Command.DROP, testProviderName,
				new Object[] {});
		conn.assertResults("show dynamic site providers", 
				br( nr,OnPremiseSiteProvider.DEFAULT_NAME, OnPremiseSiteProvider.class.getCanonicalName(), "YES"));
		String sl = TestCatalogHelper.getInstance().getCatalogUrl();
		String ops = "OnPremise";
		String local = "LOCAL";
		Integer negone = new Integer(-1);
		Integer zero = new Integer(0);
		String n = "ONLINE";
//		System.out.println(conn.printResults("show dynamic site providers sites"));
		conn.assertResults("show dynamic site providers sites",
				 br(
						 nr, ops+"_"+local+"_dyn1", ops, local, "dyn1", sl, negone, zero, zero, n, SchemaTest.getIgnore(),
						 nr, ops+"_"+local+"_dyn2", ops, local, "dyn2", sl, negone, zero, zero, n, SchemaTest.getIgnore(),
						 nr, ops+"_"+local+"_dyn3", ops, local, "dyn3", sl, negone, zero, zero, n, SchemaTest.getIgnore(),
						 nr, ops+"_"+local+"_dyn4", ops, local, "dyn4", sl, negone, zero, zero, n, SchemaTest.getIgnore(),
						 nr, ops+"_"+local+"_dyn5", ops, local, "dyn5", sl, negone, zero, zero, n, SchemaTest.getIgnore() 
						 ));
		
	}

	private void clearLast() {
		lastPrepare = null;
		lastUpdate = null;
		lastShow = null;
		lastRollback = null;
	}
	
	protected void update(String sql, Command action, String providerName,
			Object[] opts) throws Throwable {
		clearLast();
		SchemaTest.echo(conn.printResults(sql));
		assertNotNull(lastUpdate);
		SchemaTest.echo(" => " + lastUpdate);
		assertCommandBlock(action, providerName, opts, lastUpdate);
		assertNotNull(lastPrepare);
		assertCommandBlock(action, providerName, opts, lastPrepare);
		// only clearLast at the beginning - lets us further examine the called state
	}

	protected void show(String sql, Command action, String providerName, Object[] opts) throws Throwable {
		clearLast();
		SchemaTest.echo(conn.printResults(sql));
		assertNotNull(lastShow);
		SchemaTest.echo(" => " + lastShow);
		assertCommandBlock(action, providerName, opts, lastShow);
	}
	
	
	// this is our test provider
	public static class GroupProviderDDLTestProvider implements SiteProviderPlugin {

		// we hold two variables, tic and tac
		String tic;
		String tac;

		boolean isEnabled = true;

		public GroupProviderDDLTestProvider() {
			
		}
		
		public String getProviderType()
		{
			return "GroupProviderDDLTestProvider";
		}
		
		@Override
		public void initialize(SiteProviderContext ctxt, String name, boolean isEnabled1, String config) {
			// TODO Auto-generated method stub
		}
		
		@Override
		public void close() {
			// TODO Auto-generated method stub
		}
		
		@Override
		public void provisionWorkerRequest(SiteProviderContext ctxt, GetWorkerRequest getWorkerRequest) throws PEException {
			// TODO Auto-generated method stub
		}

		@Override
		public void returnSitesByClass(SiteProviderContext ctxt, String siteClass, Collection<? extends StorageSite> sites) {
			// TODO Auto-generated method stub
		}

		@Override
		public List<CatalogEntity> show(SiteManagerCommand smc)
				throws PEException {
			thisTest.lastShow = smc;
			return Collections.emptyList();
		}

		@Override
		public SiteManagerCommand prepareUpdate(SiteManagerCommand smc)
				throws PEException {
			thisTest.lastPrepare = smc;
			return smc;
		}

		@Override
		public int update(SiteManagerCommand smc) throws PEException {
			thisTest.lastUpdate = smc;
			for(Pair<String,Object> kv : smc.getOptions()) {
				if ("rollback".equals(kv.getFirst()) && Boolean.TRUE.equals(kv.getSecond()))
					throw new PEException("Test rollback");
			}
			return 1;
		}

		@Override
		public void rollback(SiteManagerCommand smc) throws PEException {
			thisTest.lastRollback = smc;
		}

		@Override
		public boolean isEnabled() {
			return this.isEnabled;
		}

		@Override
		public void setEnabled(boolean isEnabled) throws PEException {
			this.isEnabled = isEnabled;
		}

		@Override
		public String getProviderName() {
			return this.getClass().getSimpleName();
		}

		@Override
		public VariableConfig<ScopedVariableHandler> getVariableConfiguration() {
			VariableConfig<ScopedVariableHandler> out = new VariableConfig<ScopedVariableHandler>();
			out.add(new GroupProviderDDLTestVariableInfo(this,"tic","tickleme"));
			out.add(new GroupProviderDDLTestVariableInfo(this,"tac","tackleme"));
			return out;
		}
		
		private static class GroupProviderDDLTestVariableInfo extends VariableInfo<ScopedVariableHandler> {
		
			private String theName;
			private String theDefaultValue;
			private GroupProviderDDLTestVariableHandler theHandler;
			
			public GroupProviderDDLTestVariableInfo(GroupProviderDDLTestProvider prov, String varName, String varDef) {
				theName = varName;
				theDefaultValue = varDef;
				theHandler = new GroupProviderDDLTestVariableHandler(prov);
			}
			
			@Override
			public String getName() {
				return theName;
			}

			@Override
			public String getDefaultValue() {
				return theDefaultValue;
			}

			@Override
			public ScopedVariableHandler getHandler() {
				return theHandler;
			}

		}
		
		public static class GroupProviderDDLTestVariableHandler extends ScopedVariableHandler {
			
			private GroupProviderDDLTestProvider me;
			
			public GroupProviderDDLTestVariableHandler(GroupProviderDDLTestProvider enc) {
				me = enc;
			}
			
			@Override
			public void setValue(String scopeName, String name, String value)
					throws PEException {
				if ("tic".equals(name.trim().toLowerCase())) {
					me.tic = value;
				} else if ("tac".equals(name.trim().toLowerCase())) {
					me.tac = value;
				} else {
					throw new IllegalArgumentException("No such variable: " + name);
				}
				thisTest.lastSetVariableName = name;
				thisTest.lastSetVariableValue = value;
			}

			@Override
			public String getValue(String scopeName, String name)
					throws PEException {
				if ("tic".equals(name.trim().toLowerCase()))
					return me.tic;
				else if ("tac".equals(name.trim().toLowerCase()))
					return me.tac;
				else
					throw new IllegalArgumentException("No such variable: " + name);
			}
			
		}

		@Override
		public Collection<? extends StorageSite> getAllSites() {
			// TODO Auto-generated method stub
			return null;
		}
		

		
	}
	
	private static void assertCommandBlock(Command expectedAction, String expectedName,
			Object[] expectedOptions, SiteManagerCommand on) throws Throwable {
		assertEquals("expected action",expectedAction,on.getAction());
		assertEquals("expected provider name",expectedName,on.getTarget().getName());
		assertOptions(expectedOptions, on);
	}
	
	private static void assertOptions(Object[] in, SiteManagerCommand on) throws Throwable {
		List<Pair<String,Object>> opts = new ArrayList<Pair<String,Object>>();
		int i = 0;
		while(i < in.length) {
			String key = (String)in[i];
			if ((++i) >= in.length) 
				throw new Throwable("Missing value for key " + key);
			Object value = in[i];
			opts.add(new Pair<String,Object>(key,value));
			i++;
		}
		assertEquals("command block should have matching opt size",opts.size(), on.getOptions().size());
		for(i = 0; i < opts.size(); i++) {
			Pair<String,Object> expected = opts.get(i);
			Pair<String,Object> actual = on.getOptions().get(i);
			assertEquals("same option should have same key",expected.getFirst(),actual.getFirst());
			assertEquals("same option should have same value",expected.getSecond(),actual.getSecond());
		}
	}
	
	private void assertSetVariable(String varName, String varValue) {
		assertEquals("last set variable name should match",varName, lastSetVariableName);
		assertEquals("last set variable value should match", varValue, lastSetVariableValue);
	}
}
