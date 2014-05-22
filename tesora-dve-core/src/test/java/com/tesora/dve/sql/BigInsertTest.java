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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

@Ignore
public class BigInsertTest extends SchemaMirrorTest {
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,Collections.EMPTY_LIST);
	}

	private ArrayList<MirrorTest> buildBigInsert(int nrows, int ncols, int colwidth, String tabName, final String distVect) {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		ArrayList<String> columnDecls = new ArrayList<String>();
		columnDecls.add("`id` int");
		ArrayList<String> rowValues = new ArrayList<String>();
		char[] charval = new char[colwidth];
		char[] letters = new String("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").toCharArray(); 
		for(int i = 0; i < charval.length; i++) {
			charval[i] = letters[i % letters.length];
		}
		String strval = "'" + new String(charval) + "'";
		int factor = new Random().nextInt(12345);
		for(int i = 0; i < ncols; i++) {
			String type = null;
			if ((i % 2) == 0) {
				type = "int";
				rowValues.add(Long.toString((i + 1) * factor));
			} else {
				type = "varchar(" + colwidth +")";
				rowValues.add(strval);
			}
			columnDecls.add("`p" + i + "` " + type);
		}
		columnDecls.add("primary key (`id`)");
		final String decl = "create table " + tabName + " ( " + Functional.join(columnDecls, ", ") + " ) ";
		final String drop = "drop table if exists " + tabName;
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				mr.getConnection().execute(drop);
				if (mr.getDDL().isNative()) 
					return mr.getConnection().execute(decl);
				return mr.getConnection().execute(decl + distVect);
			}
			
		});
		StringBuffer buf = new StringBuffer();
		buf.append("insert into " + tabName + " values ");
		String commonRowVals = Functional.join(rowValues, ", ");
		for(int i = 0; i < nrows; i++) {
			if (i > 0)
				buf.append(", ");
			buf.append("( ").append(Integer.toString(i + 1)).append(", ").append(commonRowVals).append(")");
		}
		out.add(new StatementMirrorProc(buf.toString()));
		return out;
	}
	
	private void runBigInsertTest(int nrows, int ncols, int colwidth, String tabName, String distVect, boolean onNative, boolean onSys) throws Throwable {
		List<MirrorTest> out = buildBigInsert(nrows, ncols, colwidth, tabName, distVect);
		ListOfPairs<TestResource, TestResource> conns = new ListOfPairs<TestResource, TestResource>();
		if (onNative && onSys)
			conns.add(nativeResource, sysResource);
		else if (onNative)
			conns.add(nativeResource, null);
		else
			conns.add(sysResource, null);
		runTest(out,conns,false);
	}

	@Test
	public void testSmallA() throws Throwable {
		runBigInsertTest(10,3,128,"smallA10_3","random distribute",true,true);
	}
	
	@Test
	public void testSmallB() throws Throwable {
		runBigInsertTest(1000,3,128,"smallB1000_3","random distribute",true,true);
	}
	
	@Test
	public void testMediumANativeOnly() throws Throwable {
		runBigInsertTest(10000,1,128,"mediumA_n_100k_3","random distribute",true,false);
	}
	
	@Test
	public void testMediumAPEOnly() throws Throwable {
		runBigInsertTest(10000,1,128,"mediumA_pe_100k_3","random distribute",false,true);		
	}
	
	@Test
	public void testMediumBNativeOnly() throws Throwable {
		runBigInsertTest(100,100,128,"mediumB_n_1k_100","random distribute",true, false);
	}
	
	@Test
	public void testMediumBPEOnly() throws Throwable {
		runBigInsertTest(100,100,128,"mediumB_pe_1k_100","random distribute",false, true);
	}

	@Test
	public void testMediumCNativeOnly() throws Throwable {
		runBigInsertTest(30000,2,10,"mediumC_n_1k_100","random distribute",true,false);
	}

	@Test
	public void testMediumCPEOnly() throws Throwable {
		runBigInsertTest(30000,2,10,"mediumC_pe_1k_100","random distribute",false,true);
	}

	@Ignore
	@Test
	public void testMediumCPEOnlyRepeat() throws Throwable {
		int nruns = 50;
		double scaling = 1000000.0;
		long[] deltas = new long[50];
		for(int i = 0; i < nruns; i++) {
			long before = System.nanoTime();
			testMediumCPEOnly();
			deltas[i] = System.nanoTime() - before;
			System.out.println(deltas[i]/scaling + " ms.");
		}
		long sum = 0;
		for(int i = 0; i < nruns; i++)
			sum += deltas[i];
		double avg = sum/(scaling * nruns);
		System.out.println(avg  + " ms (avg)");
	}
	
	@Test
	public void testLargeCPEOnly() throws Throwable {
		runBigInsertTest(1000000,10,10,"largeC_pe_1k_100","random distribute",false,true);
	}

	
}
