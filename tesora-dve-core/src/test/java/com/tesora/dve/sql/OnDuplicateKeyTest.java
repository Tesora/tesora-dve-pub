// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

public class OnDuplicateKeyTest extends SchemaTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),"schema");
	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(sysDDL,checkDDL,nativeDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	private static Properties getUrlOptions() {
		Properties props = new Properties();
		props.setProperty("useAffectedRows","true");
		return props;
	}
	
	@Test
	public void testOnDupKeyInserts() throws Throwable {
		ProxyConnectionResource sysconn = new ProxyConnectionResource();
		ProxyConnectionResource checkconn = new ProxyConnectionResource();
		DBHelperConnectionResource nativeConn = new DBHelperConnectionResource(getUrlOptions());
		TestResource smr = new TestResource(sysconn,sysDDL);
		TestResource cmr = new TestResource(checkconn,checkDDL);
		TestResource nmr = new TestResource(nativeConn,nativeDDL);
		TestResource[] trs = new TestResource[] { smr, cmr, nmr };
		try {
			for(TestResource tr : trs)
				tr.getDDL().create(tr);
			List<MirrorTest> tests = buildTests();
			// we're going to run these tests twice - once against check and once against sys
			for(MirrorTest mt : tests)
				mt.execute(nmr,cmr);
			// now recreate the native database
			nmr.getDDL().destroy(nmr);
			nmr.getDDL().create(nmr);
			for(MirrorTest mt : tests)
				mt.execute(nmr,smr);
		} finally {
			for(TestResource tr : trs) try {
				tr.getConnection().disconnect();
			} catch (Throwable t) {
				// ignore
			}
		}
	}
	
	private static final String[] tabNames = new String[] { "B", "S", "A", "R" };
	private static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final boolean[] enabled = new boolean[] {
		true,
		true,
		false,
		true
	};
	
	
	private List<MirrorTest> buildTests() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;
				if (ext) 
					// declare the range
					mr.getConnection().execute("create range open" + mr.getDDL().getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				for(int i = 0; i < tabNames.length; i++) {
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tabNames[i]).append("` (`id` int unsigned not null auto_increment, primary key (`id`)) ");

					if (ext) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());

				}
				return rr;
			}
		});

		// the insert order is 
		// insert into tab () values ()
		// insert into tab (id) values ('1') on duplicate key update value = value
		// insert into tab () values ()
		// select 1 from tab limit 0, 1
		for(int i = 0; i < tabNames.length; i++) {
			if (!enabled[i]) continue;
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " () values ()"));
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " () values ()"));			
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " (id) values ('1') on duplicate key update id = id"));
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " () values ()"));
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + " (id) values ('100') on duplicate key update id = id"));
			out.add(new StatementMirrorFun("select 1 from " + tabNames[i] + " limit 0,1"));
			out.add(new StatementMirrorFun("select id from " + tabNames[i] + " order by id"));
		}

		return out;
	}

	@Ignore // TOPERF - testOnDupKeyInserts passes by setting useAffectedRows on the connection, but this 
	// test still doesn't.  The test will pass if we set CLIENT_ROWS_FOUND on the connection to mysql,
	// but that is not the correct behaviour for us
	@Test
	public void testOnMultiDupKeyInserts() throws Throwable {
		ProxyConnectionResource sysconn = new ProxyConnectionResource();
		ProxyConnectionResource checkconn = new ProxyConnectionResource();
		DBHelperConnectionResource nativeConn = new DBHelperConnectionResource();
		TestResource smr = new TestResource(sysconn,sysDDL);
		TestResource cmr = new TestResource(checkconn,checkDDL);
		TestResource nmr = new TestResource(nativeConn,nativeDDL);
		TestResource[] trs = new TestResource[] { smr, cmr, nmr };
		try {
			for(TestResource tr : trs)
				tr.getDDL().create(tr);
			List<MirrorTest> tests = buildMultiTests();
			// we're going to run these tests twice - once against check and once against sys
			for(MirrorTest mt : tests)
				mt.execute(nmr,cmr);
			// now recreate the native database
			nmr.getDDL().destroy(nmr);
			nmr.getDDL().create(nmr);
			for(MirrorTest mt : tests)
				mt.execute(nmr,smr);
		} finally {
			for(TestResource tr : trs) try {
				tr.getConnection().disconnect();
			} catch (Throwable t) {
				// ignore
			}
			
			if(nativeConn != null)
				nativeConn.disconnect();
		}
	}
	
	private List<MirrorTest> buildMultiTests() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();

		buildMultiOnDupKeyValues(out);
		
		return out;
	}

	// adding test for multiple dup key values
	private void buildMultiOnDupKeyValues(List<MirrorTest> out) {
		out.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;

				String[] distVects = new String[] { 
					"broadcast distribute",
					"static distribute on (`blog_id`)",
					"random distribute",
					"range distribute on (`blog_id`) using "
				};

				for(int i = 0; i < distVects.length; i++) {
					if (ext) 
						// declare the range
						mr.getConnection().execute("create range open" + mr.getDDL().getDatabaseName() + i + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
					
					StringBuilder buf = new StringBuilder();
					buf.append("CREATE TABLE wpo"+i+" (");
					buf.append("opi bigint(20) unsigned NOT NULL auto_increment,");
					buf.append("opn varchar(64) NOT NULL default '',");
					buf.append("opv longtext NOT NULL,");
					buf.append("al varchar(20) NOT NULL default 'yes',");
					buf.append("PRIMARY KEY  (opi),");
					buf.append("UNIQUE KEY opn (opn)");
					buf.append(") DEFAULT CHARACTER SET utf8 ");

					if (ext) {
						buf.append(distVects[i]);
						if (i==3) {
							buf.append(" open" + mr.getDDL().getDatabaseName() + i);
						}
					}
					rr = mr.getConnection().execute(buf.toString());

				}
				return rr;
			}
		});
		
		for(int i = 0; i < distVects.length; i++) {
			if (!enabled[i]) continue;
			StringBuilder buf = new StringBuilder();
			buf.append("INSERT INTO `wpo"+i+"` (");
			buf.append("`opn`, `opv`, `al`) ");
			buf.append("VALUES ");
			buf.append("('_site_transient_timeout_theme_roots', '1325536233', 'yes')");
			buf.append(", ('gobbledygook','12344567','no') ");
			buf.append(",('_site_transient_timeout_theme_roots', '1325536233', 'yes') ");
			buf.append("ON DUPLICATE KEY UPDATE ");
			buf.append("`opn` = VALUES(`opn`), ");
			buf.append("`opv` = VALUES(`opv`), ");
			buf.append("`al` = VALUES(`al`);");
			out.add(new StatementMirrorProc(buf.toString()));
		}
	}
	
	@Test
	public void testOnDupInsertIntoSelect() throws Throwable {
		try (ProxyConnectionResource sysconn = new ProxyConnectionResource();
				DBHelperConnectionResource nativeConn = new DBHelperConnectionResource()) {
			TestResource smr = new TestResource(sysconn,sysDDL);
			TestResource nmr = new TestResource(nativeConn,nativeDDL);
			TestResource[] trs = new TestResource[] { smr, nmr };

			for(TestResource tr : trs)
				tr.getDDL().create(tr);
			List<MirrorTest> tests = buildIISTests();
			// we're going to run these tests twice - once against check and once against sys
			for(MirrorTest mt : tests)
				mt.execute(nmr,smr);
			for(TestResource tr : trs)
				tr.getDDL().destroy(tr);
		}
	}

	private List<MirrorTest> buildIISTests() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean pe = !mr.getDDL().isNative();
				if (pe) 
					mr.getConnection().execute("create range openrange (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				mr.getConnection().execute("create table isdkt (`id` int auto_increment, `fid` int, `sid` int, primary key(`id`)) engine=innodb /*#dve range distribute on (`id`) using openrange */");
				mr.getConnection().execute("create table tisdkt (`id` int auto_increment, `fid` int, `sid` int, primary key(`id`)) engine=innodb /*#dve range distribute on (`id`) using openrange */");
				return mr.getConnection().execute("insert into isdkt (`fid`, `sid`) values (1,1),(2,2),(3,3),(4,4),(5,5)");
			}
			
		});
		out.add(new StatementMirrorProc("insert into tisdkt select * from isdkt"));
		out.add(new StatementMirrorFun("select * from tisdkt order by id"));
		boolean ignoreUpdateCount = true;
		if (ignoreUpdateCount) {
			int a[] = new int[] { 11, 22, 33, 44, 55 };
			for(int o : a) {
				out.add(new StatementMirrorProcIgnore("insert into tisdkt select id, id + " + o + " fid, 2 * (id + " + o + ") sid from isdkt on duplicate key update fid = values(fid), sid = values(sid)"));				
			}
		} else {
			out.add(new StatementMirrorProc("insert into tisdkt select id, 5*id fid, 3*id as sid from isdkt on duplicate key update `id` = values(`id`), `fid` = values(`fid`), `sid` = values(`sid`)"));
		}
		out.add(new StatementMirrorFun("select * from tisdkt order by id"));
		return out;
	}
	
}
