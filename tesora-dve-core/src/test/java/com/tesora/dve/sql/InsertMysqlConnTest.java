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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

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
	private static final int SITES = 3;

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
	
	@Test
	public void testPE969() throws Throwable {
		final byte[] failingInput = { 75, -108, -84, 9 };

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc(
				"CREATE TABLE `bug_repro_969` (`message_id` int(11) unsigned NOT NULL AUTO_INCREMENT,`ip` varbinary(16) DEFAULT NULL,PRIMARY KEY (`message_id`)) ENGINE=InnoDB AUTO_INCREMENT=1262 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		binaryTestHelper("INSERT INTO `bug_repro_969` (`ip`) VALUES (?)",
				Collections.singletonList(new SerialBlob(failingInput)),
				CharsetUtil.ISO_8859_1,
				tests);
		tests.add(new StatementMirrorFun("SELECT binary ip FROM `bug_repro_969` order by message_id"));
		runTest(tests);
	}

	@Test
	public void testPE1149() throws Throwable {
		final byte[] failingInput = { (byte) 255 }; // any value above 127 will fail

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc(
				"CREATE TABLE `bug_repro_1149` (`user_session_id` INT NOT NULL AUTO_INCREMENT,`detail` blob, PRIMARY KEY (`user_session_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"));
		binaryTestHelper("INSERT INTO `bug_repro_1149` (`detail`) VALUES (?),('hi')",
				Collections.singletonList(new SerialBlob(failingInput)),
				CharsetUtil.ISO_8859_1,
				tests);
		tests.add(new StatementMirrorFun("SELECT * from `bug_repro_1149` ORDER BY `user_session_id`"));
		runTest(tests);
	}

	@Test
	public void testPE1327() throws Throwable {
		final byte[] failingInput = { (byte) 0xBE, (byte) 0x0E, (byte) 0x30, (byte) 0x5c, (byte) 0x5c };

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("CREATE TABLE `ecb` (`bip` int(10) unsigned NOT NULL AUTO_INCREMENT, " 
										+ "`start` varbinary(16) NOT NULL, " 
										+ "`stop` varbinary(16) NOT NULL, "
										+ "PRIMARY KEY (`bip`) "
										+ ") ENGINE=InnoDB AUTO_INCREMENT=80 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		binaryTestHelper("INSERT INTO `ecb` (`start`, `stop`) VALUES (?, ?)",
				Arrays.asList(new SerialBlob(failingInput), new SerialBlob(failingInput)),
				CharsetUtil.ISO_8859_1,
				tests);
		tests.add(new StatementMirrorFun("SELECT start FROM `ecb`"));
		runTest(tests);
	}

	@Test
	public void testPE1482() throws Throwable {
		final byte[] failingInput = { (byte)0x80, 0x5c, 0x72, 0x5c, 0x6e, 0x5c, 0x74, 0x5c, 0x62, 0x5c, 0x5a, 0x5c, 0x27, 0x5c, 0x22 }; // out of range char, \r, \n, \t, \b, \Z, \', \" 

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("CREATE TABLE `dgid` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `p12Key` mediumblob NOT NULL, `email` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"));
		binaryTestHelper("INSERT INTO `dgid` (`p12Key`, `email`) VALUES (?, '')",
				Arrays.asList(new SerialBlob(failingInput)),
				CharsetUtil.ISO_8859_1,
				tests);

		tests.add(new StatementMirrorFun("SELECT p12Key FROM `dgid`"));
		runTest(tests);
	}

	private void binaryTestHelper(final String genericSql, final List<SerialBlob> params, final Charset encoding, final List<MirrorTest> tests)
			throws SerialException {
		final List<Byte> binarySql = new ArrayList<Byte>();
		final StringBuilder textSql = new StringBuilder();
		final Iterator<SerialBlob> param = params.iterator();
		int lastParamPos = -1;
		while (param.hasNext()) {
			final SerialBlob paramBytes = param.next();
			final int nextParamPos = genericSql.indexOf("?", lastParamPos + 1);
			final String textSqlFragment = getSqlFragment(genericSql, lastParamPos, nextParamPos);
			
			emitSqlFragment(textSqlFragment, textSql, binarySql, encoding);
			emitSqlFragment("'", textSql, binarySql, encoding);

			final byte[] rawParamBytes = paramBytes.getBytes(1, (int) paramBytes.length());
			emitSqlFragment(rawParamBytes, textSql, binarySql, encoding);

			emitSqlFragment("'", textSql, binarySql, encoding);
			
			lastParamPos = nextParamPos;
		}
		emitSqlFragment(getSqlFragment(genericSql, lastParamPos, -1), textSql, binarySql, encoding);

		testWithDecodedStmt(textSql.toString(), tests);
		testWithEncodedStmt(textSql.toString(), Bytes.toArray(binarySql), encoding, tests);
	}
	
	private static String getSqlFragment(final String genericSql, final int startIndexInclusive, final int endIndexExclusive) {
		return genericSql.substring(startIndexInclusive + 1, (endIndexExclusive > 0) ? endIndexExclusive : genericSql.length());
	}

	private static void emitSqlFragment(final String fragment, final StringBuilder textContainer, final List<Byte> binaryContainer, final Charset encoding) {
		textContainer.append(fragment);
		binaryContainer.addAll(Bytes.asList(fragment.getBytes(encoding)));
	}

	private static void emitSqlFragment(final byte[] fragment, final StringBuilder textContainer, final List<Byte> binaryContainer, final Charset encoding) {
		textContainer.append(new String(fragment, encoding));
		binaryContainer.addAll(Bytes.asList(fragment));
	}

	private void testWithEncodedStmt(final String textSql, final byte[] binarySql, final Charset encoding,
			final List<MirrorTest> tests) {
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr.getDDL().isNative()) {
					return mr.getConnection().execute(textSql);
				}

				final MysqlConnectionResource pcr = (MysqlConnectionResource) mr.getConnection();
				return pcr.execute(null, encoding, binarySql);
			}
		});
	}

	private void testWithDecodedStmt(final String stmt, final List<MirrorTest> tests) {
		tests.add(new StatementMirrorProc(stmt));
	}

}
