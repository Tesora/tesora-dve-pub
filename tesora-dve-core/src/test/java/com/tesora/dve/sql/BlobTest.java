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


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.db.ValueConverter;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;

public class BlobTest extends SchemaMirrorTest {

	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",3,1,"checkg"),
				"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("checkdb");

	static protected ProxyConnectionResource conn;
	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(null,checkDDL,nativeDDL,getSchema());
		
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
	}

	@AfterClass
	public static void cleanup() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
	}

	private static final String[] types = new String[] { "LONG VARBINARY", "MEDIUMBLOB", "LONGBLOB", "BLOB" };
	private static final String[] tnames = new String[] { "LVARB", "MB", "LB", "BL" }; 
	private static final String[] binTypes = new String[] { "BINARY(2)", "VARBINARY(2)" };
	private static final String[] binTypesTNames = new String[] { "BN", "VBN" };
 
	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int i = 0; i < types.length; i++) {
			String tab = "create table " + tnames[i] + " (`id` integer not null, `bt` " + types[i] + ") ";
			out.add(new StatementMirrorProc(tab));
		}
		return out;
	}
	
	@Override
	protected ProjectDDL getSingleDDL() {
		// return checkDDL;
		return null;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}
	

	private byte[] allBytes(boolean escaped) throws Throwable {
		ArrayList<Byte> buf = new ArrayList<Byte>();
		for(int i = 0; i < 255; i++) {
			byte b = (byte)i;
			if (escaped && (b == '\\' || b == '\'')) {
				byte p = '\\';
				buf.add(new Byte(p));
			}
			buf.add(new Byte(b));
		}
		byte[] bytes = new byte[buf.size()];
		for(int i = 0; i < bytes.length; i++) {
			bytes[i] = buf.get(i).byteValue();
		}
		return bytes;
	}
	
	private List<Byte> toObject(byte[] in) {
		return ValueConverter.toObject(in);
	}
	
	private byte[] toPrimitive(List<Byte> in) {
		return ValueConverter.toPrimitive(in);
	}

	@Test
	public void testBlob() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("set names utf8"));
		Charset charset = CharsetUtil.ISO_8859_1;
		byte[] esc = allBytes(true);
		for(int i = 0; i < types.length; i++) {
			String prefix = "insert into " + tnames[i] + " values (1, '";
			String postfix = "')";
			List<Byte> prebytes = toObject(prefix.getBytes(charset));
			List<Byte> postbytes = toObject(postfix.getBytes(charset));
			ArrayList<Byte> buf = new ArrayList<Byte>();
			buf.addAll(prebytes);
			buf.addAll(toObject(esc));
			buf.addAll(postbytes);
			String command = PECharsetUtils.getString(toPrimitive(buf), charset, true);
			if (command == null)
				fail("Not a valid string");
			out.add(new StatementMirrorProc(command));
			String query = "select bt from " + tnames[i] + " where id = 1";
			out.add(new StatementMirrorFun(true,query));
		}
		
		// Test for returning a null blob
		String nullTest = "insert into LB (id) values (2)";
		out.add(new StatementMirrorProc(nullTest));
		String nullQuery = "select bt from LB where id = 2"; 
		out.add(new StatementMirrorFun(nullQuery));
		runTest(out,getTestConfig(),false);
	}

	@Test
	public void testAllBinaryFieldValues() throws Throwable {
		for(int i = 0; i < binTypes.length; ++i) {
			String tab = "create table " + binTypesTNames[i] + " (`id` integer not null, `bt` " + binTypes[i] + ") ";
			conn.execute(tab);
			for(byte j = 0; j < 64; ++j) {
//				for(byte k = 63; k < 127; ++k) {
				byte k = (byte) (j+64);
					String value = createPaddedBinaryString(j, k);

					int key = j << 8 | k;
					String query = "select bt from " + binTypesTNames[i] + " where id = " + key;
					
					// do an insert into the var/binary column
					conn.execute("insert into " + binTypesTNames[i] + 
							" values (" + key + ", " + value + ")");
					conn.assertResults(query, br(nr,new byte[] {j,k}));

					// do an update 
					value = createPaddedBinaryString(k, j);
					conn.execute("update " + binTypesTNames[i] + 
							" set bt=" + value + " where id = " + key);
					conn.assertResults(query, br(nr,new byte[] {k,j}));
					//				}
			}
		}
	}

	private String createPaddedBinaryString(byte b1, byte b2) {
		return "B'" + String.format("%8s", Integer.toBinaryString(b1)).replace(" ", "0") + 
				String.format("%8s", Integer.toBinaryString(b2)).replace(" ", "0") + "'";
	}

	@Test
	public void test_PE156() throws Throwable {
		String sg = checkDDL.getPersistentGroup().getName();
		String range = "field_range_PE156";

		// make table with range distribution on the binary column
		conn.execute("create range " + range + " (binary(36) using 'com.tesora.dve.comparator.UUIDComparator') persistent group " + sg);
		conn.execute("create table tablePE156 (a int, b int, c binary(36)) range distribute on (`c`) using " + range);
		conn.execute("create table tablePE156_redist1 (a int, b int, c binary(36)) random distribute");
		conn.execute("create table tablePE156_redist2 (a int, b int, c binary(36)) range distribute on (`c`) using " + range);

		// test UUID function
		conn.execute("insert into tablePE156 values ( 1, 1, UUID())");
		conn.execute("insert into tablePE156 values ( 2, 2, UUID())");
		conn.execute("insert into tablePE156 values ( 3, 3, UUID())");
		conn.execute("insert into tablePE156 values ( 4, 4, UUID())");
		conn.execute("insert into tablePE156 values ( 5, 5, UUID())");

		// add a generation
		conn.execute(checkDDL.getPersistentGroup().getAddGenerations());		

		conn.execute("insert into tablePE156 values ( 6, 6, UUID())");
		conn.execute("insert into tablePE156 values ( 7, 7, UUID())");
		conn.execute("insert into tablePE156 values ( 8, 8, UUID())");
		conn.execute("insert into tablePE156 values ( 9, 9, UUID())");
		conn.execute("insert into tablePE156 values ( 10, 10, UUID())");

		// after inserting new row into the new generation that has site2 all inserts should be at site2
		nativeResource.getConnection().execute("use check2_checkdb");
		nativeResource.getConnection().assertResults("select count(*) from tablePE156", br(nr,Long.valueOf(5)));

		// test the redist
		conn.execute("insert into tablePE156_redist1 values ( 1, 1, '11111111-1111-1111-1111-111111111111')");
		conn.execute("insert into tablePE156_redist1 values ( 2, 2, '22222222-2222-2222-2222-222222222222')");
		conn.execute("insert into tablePE156_redist1 values ( 3, 3, '33333333-3333-3333-3333-333333333333')");
		conn.execute("insert into tablePE156_redist1 values ( 4, 4, '44444444-4444-4444-4444-444444444444')");
		conn.execute("insert into tablePE156_redist1 values ( 5, 5, '55555555-5555-5555-5555-555555555555')");

		conn.execute("insert into tablePE156_redist2 values ( 5, 5, '11111111-1111-1111-1111-111111111111')");
		conn.execute("insert into tablePE156_redist2 values ( 4, 4, '22222222-2222-2222-2222-222222222222')");
		conn.execute("insert into tablePE156_redist2 values ( 3, 3, '33333333-3333-3333-3333-333333333333')");
		conn.execute("insert into tablePE156_redist2 values ( 2, 2, '44444444-4444-4444-4444-444444444444')");
		conn.execute("insert into tablePE156_redist2 values ( 1, 1, '55555555-5555-5555-5555-555555555555')");

		conn.assertResults("select t1.a, t1.b, t2.a, t2.b, t1.c from tablePE156_redist1 t1, tablePE156_redist2 t2 where t1.c=t2.c order by t1.a, t1.b", 
				br(nr, Integer.valueOf(1), Integer.valueOf(1), Integer.valueOf(5), Integer.valueOf(5), "11111111-1111-1111-1111-111111111111".getBytes(),
						nr, Integer.valueOf(2), Integer.valueOf(2), Integer.valueOf(4), Integer.valueOf(4), "22222222-2222-2222-2222-222222222222".getBytes(),
						nr, Integer.valueOf(3), Integer.valueOf(3), Integer.valueOf(3), Integer.valueOf(3), "33333333-3333-3333-3333-333333333333".getBytes(),
						nr, Integer.valueOf(4), Integer.valueOf(4), Integer.valueOf(2), Integer.valueOf(2), "44444444-4444-4444-4444-444444444444".getBytes(),
						nr, Integer.valueOf(5), Integer.valueOf(5), Integer.valueOf(1), Integer.valueOf(1), "55555555-5555-5555-5555-555555555555".getBytes()
						));
	}
	
	@Test
	public void test_PE157() throws Throwable {
		conn.execute("create range rangePE157 (binary(36)) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table tablePE157 (a int, b int, c binary(36)) range distribute on (c) using rangePE157");
		conn.execute("insert into tablePE157 values ( 1, 1, '00000000-1000-0000-0101-000000000001')");
		conn.assertResults("select c from tablePE157 where a=1", br(nr,"00000000-1000-0000-0101-000000000001".getBytes()));
	}

	@Test
	public void test_PE203() throws Throwable {
		conn.execute("create range rangePE203 (binary(36)) persistent group " + checkDDL.getPersistentGroup().getName());
		conn.execute("create table tablePE203_ingest (a binary(36)) broadcast distribute");
		conn.execute("create table tablePE203 (a binary(36)) range distribute on (a) using rangePE203");
		conn.execute("insert into tablePE203_ingest values ('0ae3afab-9d9d-11df-aaf6-1231390955c1'),('0ae3afab-9d9d-11df-aaf6-1231390955c1'),('0ae3afab-9d9d-11df-aaf6-1231390955c1'),('0ae3afab-9d9d-11df-aaf6-1231390955c1')");

		conn.execute("insert into tablePE203 select * from tablePE203_ingest;");

		long rowCount = 0;

		List<String> sites = checkDDL.getPersistentGroup().getPhysicalSiteNames("checkdb");
		for(String site : sites) {
			rowCount = 0;
			ResourceResponse resp = nativeResource.getConnection().execute("select count(*) from " + site + ".tablePE203");
			List<ResultRow> rows = resp.getResults();
			for(ResultRow row : rows) {
				ResultColumn col = row.getResultColumn(1);
				rowCount = (Long)col.getColumnValue();
			}
			if (rowCount == 4) {
				break;
			}
		}
		assertTrue("Range distributed table with same key values should be all on same persistent site", rowCount==4);
	}

	@Test
	public void test_PE218() throws Throwable {
		conn.execute("create table tablePE218 (a int, b int, c varchar(36)) random distribute");
		conn.execute("insert into tablePE218 values ( 1, 1, BINARY '00000000-1000-0000-0101-000000000001')");
		
		conn.assertResults("select c from tablePE218 where a=1", br(nr,"00000000-1000-0000-0101-000000000001"));
		conn.assertResults("select a from tablePE218 where c=BINARY '00000000-1000-0000-0101-000000000001'", 
				br(nr,Integer.valueOf(1)));

		conn.assertResults("select BINARY 'something'", 
				br(nr,"something".getBytes()));
	}
}
