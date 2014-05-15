// OS_STATUS: public
package com.tesora.dve.common.resultset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.collector.ResultChunkManager;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.standalone.DefaultClassLoad;
import com.tesora.dve.standalone.PETest;

public class ResultChunkMgrTest {
	static Logger logger = Logger.getLogger(ResultChunkMgrTest.class);

	static Properties dbprops;
	static Properties props;
	static DBHelper dbHelper;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestHost.startServicesTransient(PETest.class);

		dbprops = PEFileUtils.loadPropertiesFile(DefaultClassLoad.class, PEConstants.CONFIG_FILE_NAME);
		dbprops.remove(DBHelper.CONN_DBNAME); // Don't use the default database.

		dbHelper = new DBHelper(dbprops);
		dbHelper.connect();

		final String db = "resultchunkmgrtest";

		dbHelper.executeQuery("DROP DATABASE IF EXISTS " + db);
		dbHelper.executeQuery("CREATE DATABASE " + db);
		dbHelper.executeQuery("USE " + db);

		dbHelper.executeQuery("DROP TABLE IF EXISTS testtbl");
		dbHelper.executeQuery("CREATE TABLE IF NOT EXISTS testtbl( intcol int, charcol char(10))");

		PreparedStatement pstmt = dbHelper.getConnection().prepareStatement("INSERT INTO testtbl VALUES (?,?)");
		for (int i = 1; i <= 50; i++) {
			pstmt.setInt(1, i);
			pstmt.setString(2, "value " + i);
			assertEquals(1, pstmt.executeUpdate());
		}
		pstmt.close();

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		dbHelper.disconnect();
		TestHost.stopServices();
	}

	@Test
	public void testCase1() throws Exception {

		// override the ResultChunkMaxSize to fit 5 rows/chunk
		dbprops.setProperty("worker.ResultChunkMaxSize", "106");

		Statement stmt = dbHelper.getConnection().createStatement();
		assertTrue(stmt.execute("SELECT * FROM testtbl"));
		ResultChunkManager rsmgr = new ResultChunkManager(stmt.getResultSet(), dbprops, "worker", new SQLCommand(""));

		// check that the metadata contains 2 columns
		assertTrue(rsmgr.getMetaData().size() == 2);

		int chunkCnt = 0;
		ResultChunk chunk;
		while (rsmgr.nextChunk()) {
			chunk = rsmgr.getChunk();
			chunkCnt++;
			assertEquals(5, chunk.size());
			logger.debug(chunk.toString());
		}

		assertEquals(10, chunkCnt);
		stmt.close();
	}

	@Test
	public void testCase2() throws Exception {

		// override the ResultChunkMaxSize to fit only 1 row/chunk
		dbprops.setProperty("worker.ResultChunkMaxSize", "10");

		Statement stmt = dbHelper.getConnection().createStatement();
		assertTrue(stmt.execute("SELECT * FROM testtbl"));
		ResultChunkManager rsmgr = new ResultChunkManager(stmt.getResultSet(), dbprops, "worker", new SQLCommand(""));

		int chunkCnt = 0;
		ResultChunk chunk;
		while (rsmgr.nextChunk()) {
			chunk = rsmgr.getChunk();
			chunkCnt++;
			assertEquals(1, chunk.size());
			logger.debug(chunk.toString());
		}

		assertEquals(50, chunkCnt);
		stmt.close();
	}

	@Test
	public void testCase3() throws Exception {

		// use default MaxChunkSize which should result in 1 chunk with all 50
		// rows
		dbprops.remove("worker.ResultChunkMaxSize");

		Statement stmt = dbHelper.getConnection().createStatement();
		assertTrue(stmt.execute("SELECT * FROM testtbl"));
		ResultChunkManager rsmgr = new ResultChunkManager(stmt.getResultSet(), dbprops, "worker", new SQLCommand(""));

		int chunkCnt = 0;
		ResultChunk chunk;
		while (rsmgr.nextChunk()) {
			chunk = rsmgr.getChunk();
			chunkCnt++;
			assertEquals(50, chunk.size());
			logger.debug(chunk.toString());
		}

		assertEquals(1, chunkCnt);
		stmt.close();
	}

}
