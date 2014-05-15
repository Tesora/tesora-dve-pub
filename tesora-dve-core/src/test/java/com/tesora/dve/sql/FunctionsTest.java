// OS_STATUS: public
package com.tesora.dve.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class FunctionsTest extends SchemaTest {
	private static final ProjectDDL checkDDL = new PEDDL("checkdb",
			new StorageGroupDDL("check", 2, "checkg"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	protected DBHelperConnectionResource dbh;

	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
	}

	@After
	public void disconnect() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
	}

	@Test
	public void testLowerWithLike() throws Throwable {
		conn.execute("create table `a` (`id` int, `mask` varchar(50), primary key (`id`)) ");

		String select = "select 1 from a where id=1 and lower('MiXeD cAsE') like lower(mask)"; 
		// query should return empty result set
		conn.assertResults(select, br());

		// insert a row that does not satisfy the lower clause
		conn.execute("insert into a values (1,'some junk')");

		// query should return empty result set
		conn.assertResults(select, br());

		// update row so that it does satisfy the lower clause
		conn.execute("update a set mask='MIXED CASE'");

		// query should return 1 row
		conn.assertResults(select, br(nr, Long.valueOf(1)));
		
		// make sure we didn't break the typical like parameter
		select = "select 1 from a where id=1 and mask like '%C%'"; 
		conn.assertResults(select, br(nr, Long.valueOf(1)));
	}

	@Test
	public void testUnix_timestamp() throws Throwable {
		conn.execute("create table `tblUnixTS` (`id` int, `d` date, `dt` datetime, `ts` timestamp, primary key (`id`)) ");

		// these are the acceptable values that can be put into the unix_timestamp function
		String dateLiteral = "'1997-01-01'";
		String datetimeLiteral = "'1998-02-02 00:00:01'";
		String timestampLiteral = "'1999-03-03 23:59:59'";
		long dateFormat1Long = 970101;
		long dateFormat2Long = 19970101;
		
		conn.execute("insert into tblUnixTS values (1,"+ dateLiteral + "," + datetimeLiteral + "," + timestampLiteral + ")");
		
		// all the values are before the current time so no rows should be returned
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(d) > unix_timestamp()", 
				br());
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(dt) > unix_timestamp()", 
				br());
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(ts) > unix_timestamp()", 
				br());
		
		// this should return the row if we reverse the comparison operator
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(d) < unix_timestamp()", 
				br(nr, Long.valueOf(1)));
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(dt) < unix_timestamp()", 
				br(nr, Long.valueOf(1)));
		conn.assertResults("select 1 from tblUnixTS where unix_timestamp(ts) < unix_timestamp()", 
				br(nr, Long.valueOf(1)));
		
		// make sure using a literal or a column definition as a parameter returns the same value
		compareUnixTimestampParamLiteralVSColumn(
				"select unix_timestamp(" + dateLiteral + ")",
				"select unix_timestamp(d) from tblUnixTS where id=1");
		
		compareUnixTimestampParamLiteralVSColumn(
				"select unix_timestamp(" + datetimeLiteral + ")",
				"select unix_timestamp(dt) from tblUnixTS where id=1");

		compareUnixTimestampParamLiteralVSColumn(
				"select unix_timestamp(" + timestampLiteral + ")",
				"select unix_timestamp(ts) from tblUnixTS where id=1");

		compareUnixTimestampParamLiteralVSColumn(
				"select unix_timestamp(" + dateFormat1Long + ")",
				"select unix_timestamp(d) from tblUnixTS where id=1");

		compareUnixTimestampParamLiteralVSColumn(
				"select unix_timestamp(" + dateFormat2Long + ")",
				"select unix_timestamp(d) from tblUnixTS where id=1");
	}
	
	private void compareUnixTimestampParamLiteralVSColumn(String literalSql,
			String columnSql) throws Throwable {
		ResourceResponse resp = conn.fetch(literalSql);
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		Long literalLong = (Long) (rows.get(0).getResultColumn(1)
				.getColumnValue());

		resp = conn.fetch(columnSql);
		rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		Long columnLong = (Long) (rows.get(0).getResultColumn(1)
				.getColumnValue());

		assertEquals("Expected literal query (" + literalSql
				+ ") to return same value as column query (" + columnSql + ")",
				literalLong, columnLong);
	}
	
	@Test
	public void testPE167() throws Throwable {
		conn.execute("create table ucp (`vid` integer unsigned not null, `length` integer, `width` integer, `height` integer);");
		
		// test length as a column name
		String select = "SELECT length, width, height FROM ucp WHERE vid = 7710429"; 
		// query should return empty result set
		conn.assertResults(select, br());
	}

	@Test
	public void testPE168() throws Throwable {
		conn.execute("create table urs (`uid` integer unsigned not null, `created` timestamp);");
	
		String select;
		String[] units = {"MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR" };
		for (String unit : units) {
			select = "SELECT uid FROM `urs` WHERE uid=420185 AND TIMESTAMPDIFF(" + unit + ", FROM_UNIXTIME(created), NOW()) <= 3"; 
			// query should return empty result set
			conn.assertResults(select, br());
		}
		
		// use a literal as a parameter
		select = "SELECT uid FROM `urs` WHERE uid=420185 AND TIMESTAMPDIFF(DAY, '2012-01-01', '2012-01-01')"; 
		// query should return empty result set
		conn.assertResults(select, br());
		
		try {
			select = "SELECT uid FROM `urs` WHERE uid=420185 AND TIMESTAMPDIFF(JUNK, '2012-01-01', '2012-01-01')"; 
			conn.assertResults(select, br());
			fail("JUNK is not a valid unit for timestampdiff");
		} catch (Exception e) {
			// expected
		}
	}

	@Test
	public void test_PE222_utc_timestamp() throws Throwable {
		// From the doc: utc_timestamp returns the current UTC date and time 
		// as a value in 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS.uuuuuu format, 
		// depending on whether the function is used in a string or numeric context. 
		
		Timestamp ts;
		// try with and without parens
		ResourceResponse resp = conn.fetch("SELECT UTC_TIMESTAMP(), UTC_TIMESTAMP() + 0");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		// If the types don't cast properly then we have a problem
		assertNotNull(ts = (Timestamp) (rows.get(0).getResultColumn(1).getColumnValue()));
		assertNotNull((rows.get(0).getResultColumn(2).getColumnValue()));

		// put function into an insert
		conn.execute("create table `tblUTCTS` (`id` int, `ts` timestamp, primary key (`id`)) ");
		conn.execute("insert into tblUTCTS values (1, utc_timestamp())");
		
		// just make sure the ts2 is equal or after the above ts
		Timestamp ts2;
		resp = conn.fetch("select ts from tblUTCTS");
		rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		assertNotNull(ts2 = (Timestamp) (rows.get(0).getResultColumn(1).getColumnValue()));
		assertTrue(ts.compareTo(ts2)<1);
	}

	@Test
	public void test_PE136_REGEXP() throws Throwable {
		StringBuilder buf = new StringBuilder();
		buf.append("CREATE TABLE wpo (");
		buf.append("oid bigint(20) unsigned NOT NULL auto_increment,");
		buf.append("onm varchar(64) NOT NULL default '',");
		buf.append("ov longtext NOT NULL,");
		buf.append("al varchar(20) NOT NULL default 'yes',");
		buf.append("PRIMARY KEY  (oid),");
		buf.append("UNIQUE KEY onm (onm)");
		buf.append(") DEFAULT CHARACTER SET utf8 ");

		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO `wpo` (");
		buf.append("`onm`, `ov`, `al`) ");
		buf.append("VALUES (");
		buf.append("'_site_transient_timeout_theme_roots', '1325536233', 'yes') ");
		conn.execute(buf.toString());
		
		buf = new StringBuilder();
		buf.append("INSERT INTO `wpo` (");
		buf.append("`onm`, `ov`, `al`) ");
		buf.append("VALUES (");
		buf.append("'rss_00000000000000000000000000000000', '0000000000', 'no') ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT `onm`, `ov`, `al` ");
		buf.append("FROM wpo WHERE onm REGEXP '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.assertResults(buf.toString(), 
				br(nr, "rss_00000000000000000000000000000000", "0000000000", "no"));

		buf = new StringBuilder();
		buf.append("SELECT `onm`, `ov`, `al` ");
		buf.append("FROM wpo WHERE onm RLIKE '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.assertResults(buf.toString(), 
				br(nr, "rss_00000000000000000000000000000000", "0000000000", "no"));

		buf = new StringBuilder();
		buf.append("UPDATE wpo ");
		buf.append("SET `ov`='1111111111' ");
		buf.append("WHERE onm RLIKE '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.execute(buf.toString());
		
		buf = new StringBuilder();
		buf.append("SELECT `onm`, `ov`, `al` ");
		buf.append("FROM wpo WHERE onm RLIKE '^[0-9a-f]{32}(_ts)?$'");
		conn.assertResults(buf.toString(), 
				br());

		buf = new StringBuilder();
		buf.append("SELECT `onm`, `ov`, `al` ");
		buf.append("FROM wpo WHERE onm RLIKE '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.assertResults(buf.toString(), 
				br(nr, "rss_00000000000000000000000000000000", "1111111111", "no"));

		buf = new StringBuilder();
		buf.append("DELETE FROM wpo ");
		buf.append("WHERE onm RLIKE '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT `onm`, `ov`, `al` ");
		buf.append("FROM wpo WHERE onm RLIKE '^rss_[0-9a-f]{32}(_ts)?$'");
		conn.assertResults(buf.toString(), 
				br());
	}
	
	@Test
	public void test_NULLIF() throws Throwable {
		StringBuilder buf = new StringBuilder();
		buf.append("CREATE TABLE wp_usermeta (");
		buf.append("umid bigint(20) unsigned NOT NULL auto_increment,");
		buf.append("uid bigint(20) unsigned NOT NULL default '0',");
		buf.append("mk varchar(255) default NULL,");
		buf.append("mv longtext,");
		buf.append("PRIMARY KEY  (umid),");
		buf.append("KEY uid (uid),");
		buf.append("KEY mk (mk)");
		buf.append(") DEFAULT CHARACTER SET utf8 ");

		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO `wp_usermeta` (");
		buf.append("`uid`,`mk`,`mv`) ");
		buf.append("VALUES ");
		buf.append("('1','last_name',''),");
		buf.append("('1','nickname','admin'),");
		buf.append("('1','description',''),");
		buf.append("('1','rich_editing','true'),");
		buf.append("('1','comment_shortcuts','false'),");
		buf.append("('1','admin_color','fresh'),");
		buf.append("('1','use_ssl','0'),");
		buf.append("('1','show_admin_bar_front','true'),");
		buf.append("('1','wp_capabilities','a:1:{s:10:\"subscriber\";s:1:\"1\";}'),");
		buf.append("('1','wp_user_level','0'),");
		buf.append("('1','dismissed_wp_pointers','wp330_toolbar,wp330_media_uploader,wp330_saving_widgets'),");
		buf.append("('1','show_welcome_panel','1'),");
		buf.append("('1','wp_dashboard_quick_press_last_post_id','3')");
		conn.execute(buf.toString());
		
		buf = new StringBuilder();
		buf.append("SELECT ");
		buf.append("COUNT(NULLIF(`mv` LIKE '%administrator%', FALSE)),");
		buf.append("COUNT(NULLIF(`mv` LIKE '%editor%', FALSE)),");
		buf.append("COUNT(NULLIF(`mv` LIKE '%author%', FALSE)),");
		buf.append("COUNT(NULLIF(`mv` LIKE '%contributor%', FALSE)),");
		buf.append("COUNT(NULLIF(`mv` LIKE '%subscriber%', FALSE)),");
		buf.append("COUNT(*) ");
		buf.append("FROM wp_usermeta ");
		buf.append("WHERE mk = 'wp_capabilities'");
		conn.assertResults(buf.toString(), 
				br(nr, Long.valueOf(0), Long.valueOf(0), Long.valueOf(0), Long.valueOf(0), Long.valueOf(1), Long.valueOf(1)));
	}

	@Test
	public void test_DATE_SUB_or_ADD_and_YEAR_MONTH() throws Throwable {
		StringBuilder buf = new StringBuilder();
		
		buf.append("CREATE TABLE wpp (");
		buf.append("ID bigint(20) unsigned NOT NULL auto_increment,");
		buf.append("pd datetime NOT NULL default '0000-00-00 00:00:00',");
		buf.append("ps varchar(20) NOT NULL default 'publish',");
		buf.append("pt varchar(20) NOT NULL default 'post',");
		buf.append("PRIMARY KEY  (ID)");
		buf.append(") DEFAULT CHARACTER SET utf8 ");
		buf.append("BROADCAST DISTRIBUTE ");

		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("CREATE TABLE a_broadcast_table_to_join (");
		buf.append("ID bigint(20) unsigned NOT NULL auto_increment,");
		buf.append("pt varchar(20) NOT NULL default 'post',");
		buf.append("PRIMARY KEY  (ID)");
		buf.append(") DEFAULT CHARACTER SET utf8 ");
		buf.append("BROADCAST DISTRIBUTE ");

		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("CREATE TABLE a_random_table_to_join (");
		buf.append("ID bigint(20) unsigned NOT NULL auto_increment,");
		buf.append("pt varchar(20) NOT NULL default 'post',");
		buf.append("PRIMARY KEY  (ID)");
		buf.append(") DEFAULT CHARACTER SET utf8 ");
		buf.append("RANDOM DISTRIBUTE ");

		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO `wpp` (");
		buf.append("`pd`) ");
		buf.append("VALUES ");
		buf.append("('2012-01-02 18:30:33')");
		conn.execute(buf.toString());
		
		buf = new StringBuilder();
		buf.append("INSERT INTO `wpp` (");
		buf.append("`pd`,`pt`) ");
		buf.append("VALUES ");
		buf.append("('2012-02-02 18:30:33','page')");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO `wpp` (");
		buf.append("`pd`,`ps`,`pt`) ");
		buf.append("VALUES ");
		buf.append("('2012-03-02 18:31:00','auto-draft','post')");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT ID ");
		buf.append("FROM wpp ");
		buf.append("WHERE ps = 'auto-draft' ");
		buf.append("AND DATE_SUB( NOW(), INTERVAL 7 DAY ) > pd ");
		conn.assertResults(buf.toString(), 
				br(nr, Long.valueOf(3)));

		buf = new StringBuilder();
		buf.append("SELECT ID ");
		buf.append("FROM wpp ");
		buf.append("WHERE DATE_ADD( '2012-01-01 18:31:00', INTERVAL 2 DAY ) > pd ");
		conn.assertResults(buf.toString(), 
				br(nr, Long.valueOf(1)));

		buf = new StringBuilder();
		buf.append("SELECT DISTINCT YEAR( pd ) AS year, MONTH( pd) AS month ");
		buf.append("FROM wpp ");
		buf.append("WHERE pt = 'page' ");
		buf.append("ORDER BY pd desc");
		conn.assertResults(buf.toString(),
				br(nr, Integer.valueOf(2012), Integer.valueOf(2)));

		// make sure the following don't throw exceptions
		buf = new StringBuilder();
		buf.append("SELECT DISTINCT YEAR( p.pd ) AS year, MONTH( p.pd) AS month ");
		buf.append("FROM wpp p INNER JOIN a_broadcast_table_to_join j ");
		buf.append("ON p.ID = j.ID ");
		buf.append("ORDER BY pd desc");
		conn.assertResults(buf.toString(),
				br());

		buf = new StringBuilder();
		buf.append("SELECT DISTINCT YEAR( p.pd ) AS year, MONTH( p.pd) AS month ");
		buf.append("FROM wpp p INNER JOIN a_random_table_to_join j ");
		buf.append("ON p.ID = j.ID ");
		buf.append("ORDER BY pd desc");
		conn.assertResults(buf.toString(),
				br());
	}

	@Test
	public void testPE775() throws Throwable {
		StringBuilder buf = new StringBuilder();
		
		buf.append("CREATE TABLE `umt` ( ");
		buf.append("`uid` int(11) NOT NULL, ");
		buf.append("`token` varchar(100) NOT NULL, ");
		buf.append("`datetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, ");
		buf.append("PRIMARY KEY (`token`), ");
		buf.append("KEY `uid` (`uid`) ");
		buf.append(") ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */ ");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO umt (uid,token,datetime) ");
		buf.append("VALUES ");
		buf.append("(2904,'c0ab2d0a53de4522223ae0c8d08e871294debbe29a72fba8c38b326ad7261507',now())");		
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT count(*) FROM umt");
		conn.assertResults(buf.toString(), br(nr, Long.valueOf(1)));

		buf = new StringBuilder();
		buf.append("SELECT datetime FROM umt");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("UPDATE umt SET datetime=now()");
		conn.execute(buf.toString());
	}
	
	@Test
	public void testPE951_LEFT() throws Throwable {
		StringBuilder buf = new StringBuilder();
		
		buf.append("CREATE TABLE `pe951` ( `value` varchar(100) NOT NULL )");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("INSERT INTO pe951 VALUES ('123456789012345678901234567890'), ('This is a test string')");
		conn.execute(buf.toString());

		buf = new StringBuilder();
		buf.append("SELECT value FROM pe951 ORDER BY value");
		conn.assertResults(buf.toString(),
				br(nr, "123456789012345678901234567890", nr, "This is a test string"));

		buf = new StringBuilder();
		buf.append("SELECT LEFT(value,10) FROM pe951 ORDER BY value");
		conn.assertResults(buf.toString(),
				br(nr, "1234567890", nr, "This is a "));
	}
	
	@Test
	public void testPE988_REPLACE() throws Throwable {
		final String test1 = "SELECT REPLACE('Hello, World!', 'Hello', 'Hi')";
		conn.assertResults(test1, br(nr, "Hi, World!"));
		
		conn.execute("CREATE TABLE `pe988` ( `id` int(11) NOT NULL"
				+ ", `name` varchar(256) NOT NULL )");
		conn.execute("INSERT INTO `pe988` VALUES (1, 'float')");
		conn.execute("INSERT INTO `pe988` VALUES (2, ' int')");
		conn.execute("INSERT INTO `pe988` VALUES (3, 'text ')");
		conn.execute("INSERT INTO `pe988` VALUES (4, ' char ')");
		conn.execute("INSERT INTO `pe988` VALUES (5, 'long long')");
		conn.execute("INSERT INTO `pe988` VALUES (6, ' tiny text')");
		conn.execute("INSERT INTO `pe988` VALUES (7, 'unsigned int ')");
		conn.execute("INSERT INTO `pe988` VALUES (8, ' long double ')");
		conn.execute("INSERT INTO `pe988` VALUES (9, '  some  long  text   with  spaces  and	tabs  ');");
		
		final String test2 = "SELECT REPLACE(pe988.name, ' ', '-') FROM pe988 ORDER BY id";
		conn.assertResults(test2, br(
				nr, "float",
				nr, "-int",
				nr, "text-",
				nr, "-char-",
				nr, "long-long",
				nr, "-tiny-text",
				nr, "unsigned-int-",
				nr, "-long-double-",
				nr, "--some--long--text---with--spaces--and	tabs--"));
	}

	@Test
	public void testPE1156_GroupConcatMaxLen() throws Throwable {
		final String test = "SELECT GROUP_CONCAT('a', 'b', 'c', 'd', 'e', 'f', 'g')";
		conn.assertResults(test, br(nr, "abcdefg"));
		conn.execute("SET SESSION group_concat_max_len = 5");
		conn.assertResults(test, br(nr, "abcde"));
	}

	@Test
	public void testPE362_Char() throws Throwable {
		final String test1In = "SELECT CHAR(77,121,83,81,'76')";
		final String test1Out = "MySQL";

		/* By default, CHAR() returns a binary string. */
		conn.assertResults(test1In, br(nr, test1Out.getBytes()));

		conn.assertResults(
				"SELECT CHAR(0x4E, NULL, 0x55, NULL, 0x4C, NULL, 0x4C USING utf8)",
				br(nr, "NULL"));
	}

	@Ignore
	@Test
	public void testPE347_Interval() throws Throwable {
		conn.assertResults("SELECT INTERVAL(55,10,20,30,40,50,60,70,80,90,100)", br(nr, 5));
		conn.assertResults("SELECT INTERVAL(3,1,1+1,1+1+1+1)", br(nr, 2));
		conn.assertResults("SELECT INTERVAL(0,1,2,3,4)", br(nr, 0));
		conn.assertResults("SELECT INTERVAL(NULL,1,2,3,4)", br(nr, -1));
	}
	
	@Ignore
	@Test
	public void testPE347_Elt() throws Throwable {
		conn.assertResults("SELECT ELT(2,\"ONE\",\"TWO\",\"THREE\")", br(nr, "TWO"));
		conn.assertResults("SELECT ELT(0,\"ONE\",\"TWO\",\"THREE\")", br(nr, null));
		conn.assertResults("SELECT ELT(4,\"ONE\",\"TWO\",\"THREE\")", br(nr, null));
		conn.assertResults("SELECT ELT(1,1,2,3)|0)", br(nr, 1));
		conn.assertResults("SELECT ELT(1,1.1,1.2,1.3)+0)", br(nr, 1.1));
	}
	
	@Test
	public void testPE347_Field() throws Throwable {
		conn.assertResults("SELECT FIELD(\"IBM\",\"NCA\",\"ICL\",\"SUN\",\"IBM\",\"DIGITAL\")", br(nr, 4l));
		conn.assertResults("SELECT FIELD(\"TESORA\",\"NCA\",\"ICL\",\"SUN\",\"IBM\",\"DIGITAL\")", br(nr, 0l));
	}

	@Test
	public void testPE1403_Rand() throws Throwable {
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("SELECT RAND(1, 2, 3)");
			}
		}.assertException(PESQLException.class, "Unable to build plan - Incorrect parameter count in the call to native function 'RAND'");

		assertResultDistribution("SELECT RAND()", 1, 1, 10, true);
		assertResultDistribution("SELECT RAND(0)", 1, 1, 10, false);
		assertResultDistribution("SELECT RAND(5)", 1, 1, 10, false);

		conn.execute("CREATE TABLE pe1403 (id INT NOT NULL)");
		conn.execute("INSERT INTO pe1403 VALUES (1), (2), (3), (4), (5), (6)");
		assertResultDistribution("SELECT id, RAND() FROM pe1403", 2, 6, 1, true);
		assertResultDistribution("SELECT id, RAND(0) FROM pe1403", 2, 6, 1, true);
		assertResultDistribution("SELECT id, RAND(5) FROM pe1403", 2, 6, 1, true);
	}

	private void assertResultDistribution(final String stmt, final int columnIndex, final int expectedNumRows, final int numTrials, final boolean assertRandom)
			throws Throwable {
		final Set<Object> results = new HashSet<Object>(numTrials);
		for (int i = 0; i < numTrials; ++i) {
			final ResourceResponse response = conn.execute(stmt);
			final List<ResultRow> rows = response.getResults();

			assertEquals("Wrong number of rows in the result set.", expectedNumRows, rows.size());

			for (final ResultRow row : rows) {
				results.add(row.getResultColumn(columnIndex).getColumnValue());
			}
		}

		if (assertRandom) {
			assertTrue("Random results expected.", results.size() == (expectedNumRows * numTrials));
		} else {
			assertTrue("Equal results expected.", results.size() == 1);
		}
	}
	
	@Test
	public void testPE311_XOR() throws Throwable {
		conn.assertResults("SELECT 1 XOR 1", br(nr, 0l));
		conn.assertResults("SELECT 1 XOR 0", br(nr, 1l));
		conn.assertResults("SELECT 1 XOR NULL", br(nr, null));
		conn.assertResults("SELECT 1 XOR 1 XOR 1", br(nr, 1l));

		conn.execute("CREATE TABLE `pe311` (`id` int, `value` varchar(10), PRIMARY KEY (`id`))");
		conn.execute("INSERT INTO `pe311` VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')");
		conn.assertResults("SELECT id XOR id, value FROM pe311 WHERE ((id > 2) XOR (id < 4)) ORDER BY id ASC",
				br(nr, 0l, "one", nr, 0l, "two", nr, 0l, "four", nr, 0l, "five"));
	}
	
}
