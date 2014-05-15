// OS_STATUS: public
package com.tesora.dve.sql;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class ViewDDLTest extends SchemaTest {

	private static final ProjectDDL checkDDL =
			new PEDDL("adb",
					new StorageGroupDDL("check",1,"checkg"),
					"database");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}


	private PortalDBHelperConnectionResource conn;

	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
	}

	@After
	public void after() throws Throwable {
		if(conn != null) {
			conn.disconnect();
		}

		conn = null;
	}

	private static final String vq = 
			"select table_schema, table_name, check_option, is_updatable, definer, security_type, mode from information_schema.views where table_schema = '%s' and table_name = '%s'";
	private static final String vdq =
			"select view_definition from information_schema.views where table_schema = '%s' and table_name = '%s'";
	private static final String tq =		
			"select table_type from information_schema.tables where table_schema = '%s' and table_name = '%s'";
    @Test
    public void testBasicCreate() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, primary key (id)) broadcast distribute");
    		conn.execute("create table B (id int, stuff varchar(32), primary key (id)) random distribute");
    		conn.execute("create view AB as select a.id as lid, b.id as rid, b.stuff as stuffed from A a inner join B b on a.id = b.id");
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(),"AB"),
    				br(nr,"adb","AB","NONE","NO","root@%","DEFINER","passthrough"));
    		conn.assertResults("show fields from AB",
    				br(nr,"lid","int(11)","YES","",null,"",
    				   nr,"rid","int(11)","YES","",null,"",
    				   nr,"stuffed","varchar(32)","NO","",null,""));
    		conn.execute("create view BB as select a.id as lid, b.id as rid, b.stuff as stuffed from B a inner join B b on a.id = b.id");
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(),"BB"),
    				br(nr,"adb","BB","NONE","NO","root@%","DEFINER","emulate"));
    		conn.assertResults(String.format(tq, checkDDL.getDatabaseName(),"AB"),
    				br(nr,"VIEW"));
     		conn.assertResults(String.format(tq, checkDDL.getDatabaseName(),"A"),
    				br(nr,"BASE TABLE"));    		
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }
    
    @Test
    public void testDrop() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, fid int, primary key (id)) broadcast distribute");
    		conn.execute("create view AA as select id, max(fid) from A group by id");
    		conn.assertResults("show tables",br(nr,"A",nr,"AA"));
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(),"AA"),
    				br(nr,"adb","AA","NONE","NO","root@%","DEFINER","passthrough"));
    		conn.execute("DROP VIEW AA");
    		conn.assertResults("show tables", br(nr,"A"));
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(),"AA"), br());
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }
    
    @Test
    public void testCreateOrReplace() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, fid int, primary key (id)) broadcast distribute");
    		conn.execute("create table B (id int, fid int, primary key (id)) random distribute");
    		conn.execute("create view AA as select id, max(fid) from A group by id");
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(), "AA"),
    				br(nr,"adb","AA","NONE","NO","root@%","DEFINER","passthrough"));
    		conn.assertResults(String.format(vdq,checkDDL.getDatabaseName(),"AA"),
    				br(nr,"SELECT `A`.`id`,max( `A`.`fid` )  FROM `A` GROUP BY `A`.`id` ASC"));
    		conn.execute("create or replace view AA as select id, fid from B order by fid");
    		conn.assertResults(String.format(vdq,checkDDL.getDatabaseName(),"AA"),
    				br(nr,"SELECT `B`.`id`,`B`.`fid` FROM `B` ORDER BY `B`.`fid` ASC"));
    		conn.assertResults(String.format(vq,checkDDL.getDatabaseName(), "AA"),
    				br(nr,"adb","AA","NONE","NO","root@%","DEFINER","emulate"));
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }

    @Test
    public void testViewDistVect() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table B (id int, fid int, primary key (id)) broadcast distribute");
    		conn.execute("create table A (id int, fid int, primary key (id)) random distribute");
    		conn.execute("create range vtr (int) persistent group " + checkDDL.getPersistentGroup().getName());
    		conn.execute("create table R (id int, fid int, primary key (id)) range distribute on (id) using vtr");
    		String fmt1 = "create view %s as select a.id as vid, b.fid as vfid from %s a inner join %s b on a.id = b.id where b.fid = 22";
    		// bcast distributed, and passthrough
    		conn.execute(String.format(fmt1,"BB","B","B"));
    		validateModeAndDistVect("BB","passthrough","Broadcast",null);
    		// random distributed, emulated
    		conn.execute(String.format(fmt1,"AA","A","A"));
    		validateModeAndDistVect("AA","emulate","Random",null);
    		// random distributed, passthrough
    		conn.execute(String.format(fmt1,"AB","A","B"));
    		validateModeAndDistVect("AB","passthrough","Random",null);
    		// range dist on the first column, passthrough
    		conn.execute(String.format(fmt1,"RR","R","R"));
    		validateModeAndDistVect("RR","passthrough","Range","vid");
    		// range dist on the first column, passthrough
    		conn.execute(String.format(fmt1,"RB","R","B"));
    		validateModeAndDistVect("RB","passthrough","Range","vid");
    		// random dist, emulated
    		conn.execute(String.format(fmt1, "RA","R","A"));
    		validateModeAndDistVect("RA","emulate","Random",null);
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }
    
    private void validateModeAndDistVect(String tableName, String expectedMode, String expectedDist, String expectedColumn) throws Throwable {
    	conn.assertResults("select mode from information_schema.views where table_schema = '" + checkDDL.getDatabaseName() + "' and table_name = '" + tableName + "'",
    			br(nr,expectedMode));
    	conn.assertResults("select model_type, column_name from information_schema.distributions where database_name = '" + checkDDL.getDatabaseName() + "' and table_name = '" + tableName + "'",
    			br(nr,expectedDist,expectedColumn));
    }
    
    @Test
    public void testShowFullTables() throws Throwable {
    	try {
    		checkDDL.create(conn);
    		conn.execute("create table A (id int, primary key (id)) broadcast distribute");
    		conn.execute("create table B (id int, stuff varchar(32), primary key (id)) random distribute");
    		conn.execute("create view C as select a.id as lid, b.id as rid, b.stuff as stuffed from A a inner join B b on a.id = b.id");
    		conn.assertResults("show full tables", br(nr,"A","BASE TABLE",nr,"B","BASE TABLE",nr,"C","VIEW"));
    		conn.assertResults("show tables", br(nr,"A",nr,"B",nr,"C"));
    		conn.assertResults("show full tables where Table_type != 'VIEW'",br(nr,"A","BASE TABLE",nr,"B","BASE TABLE"));
    	} finally {
    		checkDDL.destroy(conn);
    	}
    }
    
}
