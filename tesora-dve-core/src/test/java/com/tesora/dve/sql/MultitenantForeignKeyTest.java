// OS_STATUS: public
package com.tesora.dve.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.StrictForeignKeyTest.TestConnections;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;


public class MultitenantForeignKeyTest extends MultitenantTest {

	@Test
	public void testSimpleNonForwardDeclTID() throws Throwable {
		setContext("testSimpleNonForwardDeclTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`))");
		assertValidBackingStructure();
	}

	@Test
	public void test2TableForwardDeclTID() throws Throwable {
		setContext("test2TableForwardDeclTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`))");
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		assertValidBackingStructure();
	}

	@Test
	public void test3TableForwardDeclTID() throws Throwable {
		setContext("test3TableForwardDeclTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table grandparent (`id` int, `tid` int, primary key (`id`), foreign key (`tid`) references `parent` (`id`))");
		tenantConnection.execute("create table parent (`id` int, `tid` int, primary key (`id`), foreign key (`tid`) references `child` (`id`))");
		tenantConnection.execute("create table child (`id` int, `tid` int, primary key (`id`))");
		assertValidBackingStructure();
	}
	
	@Test
	public void test3TableDanglingForwardDeclTID() throws Throwable {
		setContext("test3TableDanglingForwardDeclTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table grandparent (`id` int, `tid` int, `fid` int, primary key (`id`), constraint `doofus` foreign key (`tid`) references `parent` (`id`), constraint `missing` foreign key (`fid`) references `ancestor` (`id`))");
		tenantConnection.execute("create table parent (`id` int, `tid` int, primary key (`id`), constraint `dingbat` foreign key (`tid`) references `child` (`id`))");
		tenantConnection.execute("create table child (`id` int, `tid` int, primary key (`id`))");
		assertValidBackingStructure("grandparent","ancestor");
	}
	
	@Test
	public void test2TableDropTID() throws Throwable {
		setContext("test2TableDropTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`))");
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		assertValidBackingStructure();
		tenantConnection.execute("drop table fktarg");
		assertValidBackingStructure("fkref","fktarg");
	}
	
	@Test
	public void test2TableDanglingDropTID() throws Throwable {
		setContext("test2TableDanglingDropTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `fid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`), constraint `idiot` foreign key (`fid`) references `missing` (`id`))");
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		assertValidBackingStructure("fkref","missing");
		tenantConnection.execute("drop table fktarg");
		assertValidBackingStructure("fkref","fktarg","fkref","missing");
	}
	
	@Test
	public void test2TableAlterTID() throws Throwable {
		setContext("test2TableAlterTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`))");
		tenantConnection.execute("alter table fktarg add `fid` int");
		assertValidBackingStructure();		
	}
	
	@Test
	public void test3TableAlterTID() throws Throwable {
		setContext("test3TableAlterTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`))");
		tenantConnection.execute("create table ancestor (`id` int, `tid` int, primary key (`id`), foreign key (`tid`) references `fkref` (`id`))");
		tenantConnection.execute("alter table fktarg add `fid` int");
		assertValidBackingStructure();			
	}
	
	@Test
	public void test2TableAlterDanglingTID() throws Throwable {
		setContext("test2TableAlterDanglingTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `fid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`), constraint `idiot` foreign key (`fid`) references `missing` (`id`))");
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		assertValidBackingStructure("fkref","missing");
		tenantConnection.execute("alter table fktarg add `fid` int");
		assertValidBackingStructure("fkref","missing");
		
	}
	
	@Test
	public void testCreateSetNullTID() throws Throwable {
		setContext("testSetNullTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`) on delete set null)");
		assertValidBackingStructure();		
	}

	@Test
	public void testAlterSetNullTID() throws Throwable {
		setContext("testSetNullTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`))");
		tenantConnection.execute("alter table fkref add foreign key (`tid`) references `fktarg` (`id`) on delete set null");
		assertValidBackingStructure();		
	}
	
	@Test
	public void testForwardSetNullTID() throws Throwable {
		setContext("testSetNullTID");
		rootConnection.execute(testDDL.withMTMode(MultitenantMode.ADAPTIVE).getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		tenantConnection.execute("set foreign_key_checks=0");
		tenantConnection.execute("create table fkref (`id` int, `tid` int, `ref` varchar(32), primary key (`id`), foreign key (`tid`) references `fktarg` (`id`) on delete set null)");
//		tenantConnection.execute("set foreign_key_checks=1");
//		System.out.println(tenantConnection.printResults("show create table fkref"));
		tenantConnection.execute("create table fktarg (`id` int, `value` varchar(32), primary key (id))");
//		System.out.println(tenantConnection.printResults("show create table fktarg"));
		assertValidBackingStructure();		
	}

	

	
	
	// valid backing structure is:
	// if the backing table is shared, then all the fk targets must be mangled
	// if the backing table is fixed, then they don't have to be mangled.
	// in cases where the table is fixed, we pass in an array of names; 
	// the first of each pair is the enclosing table name, the second is the allowable unmangled target name.
	private void assertValidBackingStructure(String...allowedForwards) throws Throwable {
//		System.out.println(tenantConnection.printResults("select * from information_schema.key_column_usage"));
		becomeL();
		rootConnection.execute("set foreign_key_checks=0");
		MultiMap<String,String> allowed = new MultiMap<String,String>();
		for(int i = 0; i < allowedForwards.length; i++) {
			allowed.put(allowedForwards[i], allowedForwards[++i]);
		}
		
		List<ResultRow> rows =
				rootConnection.fetch("select table_name, scope_name, table_state from information_schema.scopes where scope_name is not null").getResults();
		HashMap<String,String> names = new HashMap<String,String>();
		LinkedHashSet<String> shared = new LinkedHashSet<String>();
		LinkedHashSet<String> fixed = new LinkedHashSet<String>();

		for(ResultRow rr : rows) {
			String tn = (String) rr.getResultColumn(1).getColumnValue();
			String sn = (String) rr.getResultColumn(2).getColumnValue();
			String ts = (String) rr.getResultColumn(3).getColumnValue();
			if ("SHARED".equals(ts)) {
				shared.add(tn);
			} else {
				fixed.add(tn);
			}
			names.put(tn,sn);
		}
		
		String refSQL =
				"select distinct referenced_table_name from information_schema.key_column_usage where table_name = '%s' and referenced_table_name is not null";
		
		for(String s : shared) {
			rows = rootConnection.fetch(String.format(refSQL,s)).getResults();
			for(ResultRow rr : rows) {
				String n = (String) rr.getResultColumn(1).getColumnValue();
				// shared tables never have forward refs
				String ln = names.get(n);
				assertNotNull("shared table " + s + " must not have undecorated names",ln);
			}
		}
		for(String s : fixed) {
			rows = rootConnection.fetch(String.format(refSQL,s)).getResults();
			for(ResultRow rr : rows) {
				String n = (String) rr.getResultColumn(1).getColumnValue();
				String ln = names.get(n);
				if (ln == null) {
					String ven = names.get(s);
					Collection<String> possibilities = allowed.get(ven);
					if (possibilities != null && possibilities.contains(n)) {
						// ok
					} else {
						fail("Found forward ref '" + n + "' but shouldn't be forward");
					}
				}
			}
		}		
	}
	
	// multitenant mode only does strict tables - so we just need to test the same thing the strict test does
	// well, not really - tenants can't create ranges and such.
	
	@Test
	public void testStrictColocatedND() throws Throwable {
		setContext("testStrictColocatedND");
		StrictForeignKeyTest.testColocated(buildStrictConnections());
	}

	@Test
	public void testStrictNonColocatedND() throws Throwable {
		setContext("testStrictNonCoocatedND");
		StrictForeignKeyTest.testNonColocated(buildStrictConnections());
	}

	// some of the nd tests are ignored right now pending info schema query rewrite changes
	
	@Ignore
	@Test
	public void testStrictDropND() throws Throwable {
		setContext("testStrictNonCoocatedND");
		StrictForeignKeyTest.testDropTargetTable(buildStrictConnections());
	}

	@Ignore
	@Test
	public void testStrictAlterND() throws Throwable {
		setContext("testStrictNonCoocatedND");
		StrictForeignKeyTest.testAlterFKs(buildStrictConnections());
	}

	// it's not clear under what circumstances set null can work - looks like only tenant id dist really supports
	// set null (because the appropriate rows are all on the same site).
	@Ignore
	@Test
	public void testStrictFKActionsND() throws Throwable {
		setContext("testStrictNonCoocatedND");
		StrictForeignKeyTest.testFKActions(buildStrictConnections());
	}

	private MultitenantTestConnections buildStrictConnections() throws Throwable {
		PEDDL myDDL = new PEDDL(testDDL);
		myDDL.withTemplate(null, false);
		rootConnection.execute(myDDL.getCreateDatabaseStatement());
		createTenant(0);
		tenantConnection.execute("use " + tenantNames[0]);
		return new MultitenantTestConnections(myDDL,rootConnection,tenantConnection, tenantNames[0]);
	}
	
	private static class MultitenantTestConnections implements TestConnections {

		private ConnectionResource rootConnection;
		private ConnectionResource tenantConnection;
		private PEDDL ddl;
		private String tenantName;
		
		public MultitenantTestConnections(PEDDL theDDL, ConnectionResource rootConn, ConnectionResource tenantConn, String tenName) {
			this.ddl = theDDL;
			this.rootConnection = rootConn;
			this.tenantConnection = tenantConn;
			this.tenantName = tenName;
		}
		
		@Override
		public ConnectionResource getRootConnection() {
			return rootConnection;
		}

		@Override
		public ConnectionResource getTestConnection() {
			return tenantConnection;
		}

		@Override
		public ProjectDDL getDDL() {
			return ddl;
		}

		@Override
		public Class<?> getExceptionClass() {
			return SQLException.class;
		}

		@Override
		public void assertSchemaException(Throwable t, String message)
				throws Throwable {
			String tm = t.getMessage();
			if (tm.indexOf(message) > -1)
				return;
			throw t;
		}

		@Override
		public String getDBName() {
			return tenantName;
		}
		
	}
	
}
