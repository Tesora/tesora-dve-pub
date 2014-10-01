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

import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.primitives.Bytes;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.MysqlConnectionResource;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class InsertMysqlConnTest extends MysqlConnSchemaMirrorTest {
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL = new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	static final NativeDDL nativeDDL = new NativeDDL("cdb");

	// This test is inserting string literals into varbinary fields. This doesn't work so well via JDBC (which
	// we are using for native (the mirror). All string literals are encoded from Java UCS-2 into some other 
	// character set which doesn't work so well for binary fields. We are going to turn off UTF8 which will 
	// set the JDBC driver to use latin1, which for the most part will do the least harm to these literals.
	public InsertMysqlConnTest() {
		setUseUTF8(false);
	}

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
		setup(sysDDL,null,nativeDDL,getSchema());
	}

	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		return out;
	}
	
	/**
	 * Reproduce PE-969
	 */
	@Test
	public void testPE969() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc(
				"CREATE TABLE `bug_repro_969` (`message_id` int(11) unsigned NOT NULL AUTO_INCREMENT,`ip` varbinary(16) DEFAULT NULL,PRIMARY KEY (`message_id`)) ENGINE=InnoDB AUTO_INCREMENT=1262 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		final byte[] failingInput = { 75, -108, -84, 9 };
		final List<Byte> binarySql = new ArrayList<Byte>();
		binarySql.addAll(Bytes.asList("INSERT INTO `bug_repro_969` VALUES (1234,'".getBytes()));
		binarySql.addAll(Bytes.asList(failingInput));
		binarySql.addAll(Bytes.asList("'),(1235,NULL)".getBytes()));
		binaryTestHelper(binarySql, tests);

		// the byte array gets mangled on the way in for native.
		// native shows 'KÂ”Â¬' on disk, but pe shows 'K”¬' - notice the two extra A characters
		// this is likely due to the encoding issues
		tests.add(new StatementMirrorFun("SELECT binary ip FROM `bug_repro_969` order by message_id"));
		runTest(tests);
	}

	/**
	 * Reproduce PE-1149
	 */
	@Test
	public void testPE1149() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc(
				"CREATE TABLE `bug_repro_1149` (`user_session_id` varchar(32) NOT NULL,`detail` blob, PRIMARY KEY (`user_session_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"));
		final byte[] failingInput = { (byte) 255 }; // any value above 127 will fail
		final List<Byte> binarySql = new ArrayList<Byte>();
		binarySql.addAll(Bytes
				.asList("INSERT INTO `bug_repro_1149` VALUES ('somekey','"
						.getBytes()));
		binarySql.addAll(Bytes.asList(failingInput));
		binarySql.addAll(Bytes.asList("'),('anotherkey','hi')".getBytes()));
		binaryTestHelper(binarySql, tests);

		runTest(tests);
	}

	/*
	 * Reproduce PE-1327
	 */
	@Test
	public void testPE1327() throws Throwable {
	final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("CREATE TABLE `ecb` (`bip` int(10) unsigned NOT NULL AUTO_INCREMENT, " 
										+ "`start` varbinary(16) NOT NULL, " 
										+ "`stop` varbinary(16) NOT NULL, "
										+ "PRIMARY KEY (`bip`) "
										+ ") ENGINE=InnoDB AUTO_INCREMENT=80 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		final byte[] failingInput = { (byte) 0xBE, (byte) 0x0E, (byte) 0x30, (byte) 0x5c, (byte) 0x5c };
		final List<Byte> binarySql = new ArrayList<Byte>();
		binarySql.addAll(Bytes.asList("INSERT INTO `ecb` VALUES (24,'".getBytes()));
		binarySql.addAll(Bytes.asList(failingInput));
		binarySql.addAll(Bytes.asList("','".getBytes()));
		binarySql.addAll(Bytes.asList(failingInput));
		binarySql.addAll(Bytes.asList("')".getBytes()));
		binaryTestHelper(binarySql, tests);

		tests.add(new StatementMirrorFun("SELECT start FROM `ecb`"));

		runTest(tests);
	}

	@Test
	public void testPE1482() throws Throwable {
		final byte[] failingInput = { (byte)0x80, 0x5c, 0x72, 0x5c, 0x6e, 0x5c, 0x74, 0x5c, 0x62, 0x5c, 0x5a, 0x5c, 0x27, 0x5c, 0x22 }; // out of range char, \r, \n, \t, \b, \Z, \', \" 

		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		
		tests.add(new StatementMirrorProc("CREATE TABLE `dgid` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `p12Key` mediumblob NOT NULL, `email` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		
		final List<Byte> binarySql = new ArrayList<Byte>();
		binarySql.addAll(Bytes.asList("INSERT INTO `dgid` VALUES (1,'".getBytes()));
		binarySql.addAll(Bytes.asList(failingInput));
		binarySql.addAll(Bytes.asList("','')".getBytes()));
		binaryTestHelper(binarySql, tests);

		tests.add(new StatementMirrorFun("SELECT p12Key FROM `dgid`"));

		runTest(tests);
	}

	private void binaryTestHelper(List<Byte> binarySql,
			ArrayList<MirrorTest> tests) {
		final byte[] backingBinaryArray = ArrayUtils.toPrimitive(binarySql
				.toArray(new Byte[] {}));
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr.getDDL().isNative()) {
					return mr.getConnection().execute(
							new String(backingBinaryArray,
									CharsetUtil.ISO_8859_1));
				}

				MysqlConnectionResource pcr = (MysqlConnectionResource) mr.getConnection();
				return pcr.execute(null, CharsetUtil.ISO_8859_1, backingBinaryArray);
			}
		});
	}

}
