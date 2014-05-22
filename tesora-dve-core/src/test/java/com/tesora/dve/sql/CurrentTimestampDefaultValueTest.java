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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TimestampVariableTestUtils;
import com.tesora.dve.standalone.PETest;

public class CurrentTimestampDefaultValueTest extends SchemaTest {
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
		dbh=null;
	}

	@Test
	public void test() throws Throwable {
		conn.execute("create table `a` (`id` int, `ts` timestamp default current_timestamp, primary key (`id`)) ");

		long preTestTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
		conn.execute("insert into `a` (`id`) values (1)");
		ResourceResponse resp = conn.fetch("select `ts` from `a` where `id`=1");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		Long insertedTime = ((Timestamp)(rows.get(0).getResultColumn(1).getColumnValue())).getTime();
		assertTrue("Inserted default time must be >= starting time", preTestTime <= insertedTime);

		// make sure specified ts still works
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
		preTestTime = formatter.parse("2012-05-05 01:00:00").getTime();
		conn.execute("insert into `a` values (2, '2012-05-05 01:00:00.0')");
		resp = conn.fetch("select `ts` from `a` where `id`=2");
		rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		insertedTime = ((Timestamp)(rows.get(0).getResultColumn(1).getColumnValue())).getTime();
		assertTrue("Inserted time must be = starting time", preTestTime == insertedTime);
	}
	
	@Ignore
	@Test
	public void testAlterAddCurrentTimestampStringAsDefault() throws Throwable {
		// make sure user can alter a table and timestamp column default
		conn.execute("create table b (id int, ts timestamp default '2012-05-05 01:00:00.0')");
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
		long preTestTime = formatter.parse("2012-05-05 01:00:00").getTime();
		
		// make sure value inserted is the expected value
		conn.execute("insert into b (id) values (1))");
		ResourceResponse resp = conn.fetch("select ts from b where id=1");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		Timestamp insertedValue = (Timestamp)(rows.get(0).getResultColumn(1).getColumnValue());
		assertTrue("Inserted time must be = starting time", preTestTime == insertedValue.getTime());

		// this fails in native MySQL!
		try {
			String query = "alter table b alter ts set default current_timestamp"; 
			conn.execute(query);
			fail("Expected alter statement to fail: " + query);
		} catch (PEException e) {
			// expected
		}
		
		conn.execute("alter table b alter ts drop default");
		conn.execute("insert into b (id) values (2))");
		resp = conn.fetch("select ts from b where id=2");
		rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		// TODO mysql fails because the ts column has a not null attribute due to the 
		// default value on create
		// we should fail too but we insert the current timestamp...
		assertTrue("Inserted value must be null", rows.get(0).getResultColumn(1).getColumnValue()==null);
	}

	@Test
	public void testCurrentTimestampStringAsDefault() throws Throwable {
		// make sure user can specify current_timestamp as a literal still
		conn.execute("create table c (id int, data varchar(50) default 'current_timestamp')");
		
		conn.execute("insert into c (id) values (1)");
		ResourceResponse resp = conn.fetch("select data from c where id=1");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		String insertedValue = (String)(rows.get(0).getResultColumn(1).getColumnValue());
		assertEquals("Default literal must be 'current_timestamp'", "current_timestamp", insertedValue);
	}

	@Test
	public void testSimpleQueriesForTimestampVariable() throws Throwable {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
		
		int i = 0;
		for (Object[] objects : TimestampVariableTestUtils.getTestValues()) {
			String value = (String)objects[0];
			Boolean nullable = BooleanUtils.toBoolean((Integer)objects[1]);
			String defaultValue = (String)objects[2];
			Boolean onUpdate = BooleanUtils.toBoolean((Integer)objects[3]);
			Boolean expectedInsertTSVarSet = BooleanUtils.toBoolean((Integer)objects[4]);
			Boolean expectedUpdateTSVarSet = BooleanUtils.toBoolean((Integer)objects[5]);
			Boolean ignoreTest = BooleanUtils.toBoolean((Integer)objects[6]);
			
			if (ignoreTest) {
				continue;
			}
			
			String tableName = "ts" + i;
			
			String createTableSQL = TimestampVariableTestUtils.buildCreateTableSQL(tableName, nullable, defaultValue, onUpdate);
			String insertSQL = TimestampVariableTestUtils.buildInsertTestSQL(tableName, value, 1, Integer.toString(i));
			String updateSQL = TimestampVariableTestUtils.buildUpdateTestSQL(tableName, value, 1, Integer.toString(i)+Integer.toString(i));
			
			++i;

			conn.execute(createTableSQL);
			
			long preTestTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
			
			conn.execute(insertSQL);
			
			ResourceResponse resp = conn.fetch("select ts from " + tableName + " where id=1");
			List<ResultRow> rows = resp.getResults();
			assertEquals("Expected one row only", 1, rows.size());
			ResultColumn rc = rows.get(0).getResultColumn(1);
			if (expectedInsertTSVarSet) {
				// if we expected to set the timestamp variable then the ts column must contain the current time
				Timestamp ts = (Timestamp)(rc.getColumnValue());
				assertTrue("Inserted time must be >= starting time", preTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
			} else {
				boolean isNull = rc.isNull();
				if (!isNull) {
					// ts column is not null so let's see if the column value was specified and if it was it should match the ts value
					Timestamp ts = (Timestamp)(rc.getColumnValue());
					if (StringUtils.contains(value,"2000-01-01 01:02:03")) {
						assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
								TimeUnit.MILLISECONDS.toSeconds(formatter.parse("2000-01-01 01:02:03").getTime()));
					} else if (StringUtils.equals("0",value)) {
						assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
								TimeUnit.MILLISECONDS.toSeconds(formatter.parse("0000-00-00 00:00:00").getTime()));
					} else if (StringUtils.equals("current_timestamp",value)) {
						assertTrue("Inserted time must be >= starting time", preTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
					} else {
						// if we get here column value is not specified so figure out what default value we need
						if (StringUtils.contains(defaultValue,"2000-01-01 01:02:03")) {
							assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
									TimeUnit.MILLISECONDS.toSeconds(formatter.parse("2000-01-01 01:02:03").getTime()));
						} else if (StringUtils.equals("0",defaultValue)) {
							assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
									TimeUnit.MILLISECONDS.toSeconds(formatter.parse("0000-00-00 00:00:00").getTime()));
						} else if (StringUtils.equals("current_timestamp",defaultValue)) {
							assertTrue("Inserted time must be >= starting time", preTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
						}
					}
				} else {
					// column better be nullable
					assertTrue(nullable);
					if (StringUtils.isBlank(value)) {
						// column not specified better validate the default value
						assertTrue(StringUtils.equals("null", defaultValue) || StringUtils.isBlank(defaultValue));
					} else {
						// column value was specified as null
						assertEquals("null", value);
					}
				}
			}
			
			long updatePreTestTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

			conn.execute(updateSQL);
			resp = conn.fetch("select ts from " + tableName + " where id=1");
			rows = resp.getResults();
			assertEquals("Expected one row only", 1, rows.size());
			rc = rows.get(0).getResultColumn(1);
			if (expectedUpdateTSVarSet) {
				// if we expected to set the timestamp variable then the ts column must contain the current time
				Timestamp ts = (Timestamp)(rc.getColumnValue());
				assertTrue("Update time must be >= starting time", updatePreTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
			} else {
				boolean isNull = rc.isNull();
				if (!isNull) {
					// ts column is not null so let's see if the column value was specified and if it was it should match the ts value
					Timestamp ts = (Timestamp)(rc.getColumnValue());
					if (StringUtils.contains(value,"2000-01-01 01:02:03")) {
						assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
								TimeUnit.MILLISECONDS.toSeconds(formatter.parse("2000-01-01 01:02:03").getTime()));
					} else if (StringUtils.equals("0",value)) {
						assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
								TimeUnit.MILLISECONDS.toSeconds(formatter.parse("0000-00-00 00:00:00").getTime()));
					} else if (StringUtils.equals("current_timestamp",value)) {
						assertTrue("Inserted time must be >= starting time", preTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
					} else {
						// if we get here column value is not specified so figure out what default value we need
						if (StringUtils.contains(defaultValue,"2000-01-01 01:02:03")) {
							assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
									TimeUnit.MILLISECONDS.toSeconds(formatter.parse("2000-01-01 01:02:03").getTime()));
						} else if (StringUtils.equals("0",defaultValue)) {
							assertTrue(TimeUnit.MILLISECONDS.toSeconds(ts.getTime()) == 
									TimeUnit.MILLISECONDS.toSeconds(formatter.parse("0000-00-00 00:00:00").getTime()));
						} else if (StringUtils.equals("current_timestamp",defaultValue)) {
							assertTrue("Inserted time must be >= starting time", preTestTime <= TimeUnit.MILLISECONDS.toSeconds(ts.getTime()));
						}
					}
				} else {
					// column better be nullable
					assertTrue(nullable);
					if (StringUtils.isBlank(value)) {
						// column not specified better validate the default value
						// note in one case the default is not specified and no value was specified for the column
						// then the column value is null so the blank default is ok
						assertTrue(StringUtils.equals("null", defaultValue) || StringUtils.isBlank(defaultValue));
					} else {
						// column value was specified as null
						assertEquals("null", value);
					}
				}
			}

		}
	}
	
	@Test
	public void testTimezoneVariableChange() throws Throwable {
		// set timezone to UTC+1
		conn.execute("set time_zone='+01:00'");

		// insert a row into the test table and get the timestamp value back
		conn.execute("create table `tz` (`id` int, `ts` timestamp default current_timestamp, primary key (`id`)) ");
		conn.execute("insert into `tz` (`id`) values (1)");
		
		ResourceResponse resp = conn.fetch("select ts from tz where id=1");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		
		Timestamp utcValue = (Timestamp)(rows.get(0).getResultColumn(1).getColumnValue());

		// change the timezone and do the select again
		conn.execute("set time_zone='+02:00'");

		resp = conn.fetch("select ts from tz where id=1");
		rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());
		
		Timestamp newTZValue = (Timestamp)(rows.get(0).getResultColumn(1).getColumnValue());
		
		// there should be a 1 hour difference now between the selected timestamp values
		// because of the time_zone change
		long timeDiff = TimeUnit.MILLISECONDS.toHours(newTZValue.getTime()-utcValue.getTime());
		
		assertTrue("Changing time_zone variable should alter returned timestamp value.", timeDiff==1);
	}

	@Test 
	public void testPE688() throws Throwable {
		conn.execute("CREATE TABLE tstest (`id` int, `history` timestamp default current_timestamp on update current_timestamp) broadcast distribute");
		conn.execute("insert into tstest (id) values (1),(10)"); 
		Thread.sleep(1000);
		conn.execute("insert into tstest (id) values (2),(20)"); 
		ResourceResponse resp1 = conn.fetch("select id, unix_timestamp(history) from tstest where id=1 or id=10");  
		ResourceResponse resp2 = conn.fetch("select id, unix_timestamp(history) from tstest where id=2 or id=20");
		assertEquals("Timestamps for id=1 and 10 must be the same", resp1.getResults().get(0).getResultColumn(2).getColumnValue(), resp1.getResults().get(1).getResultColumn(2).getColumnValue());
		assertEquals("Timestamps for id=2 and 20 must be the same", resp2.getResults().get(0).getResultColumn(2).getColumnValue(), resp2.getResults().get(1).getResultColumn(2).getColumnValue());
		assertFalse("Timestamps for id=1 and 2 must be different", resp1.getResults().get(0).getResultColumn(2).getColumnValue().equals(resp2.getResults().get(0).getResultColumn(2).getColumnValue()));
		assertFalse("Timestamps for id=10 and 20 must be different", resp1.getResults().get(1).getResultColumn(2).getColumnValue().equals(resp2.getResults().get(1).getResultColumn(2).getColumnValue()));
		
		conn.execute("update tstest set history=now() where id=1"); 
		Thread.sleep(1000);
		conn.execute("update tstest set history=now() where id=10"); 
		ResourceResponse resp3 = conn.fetch("select id, unix_timestamp(history) from tstest where id=1");  
		ResourceResponse resp4 = conn.fetch("select id, unix_timestamp(history) from tstest where id=10");
		assertFalse("Timestamps for id=1 and 10 must be different", resp3.getResults().get(0).getResultColumn(2).getColumnValue().equals(resp4.getResults().get(0).getResultColumn(2).getColumnValue()));

		// this is problematic due to timing need to think of a better way to set this
//		conn.assertResults("select id from tstest where history >= now() order by id", br(nr,10));
//		Thread.sleep(1000);
//		conn.assertResults("select id from tstest where history >= now() order by id", br());
	}
}
