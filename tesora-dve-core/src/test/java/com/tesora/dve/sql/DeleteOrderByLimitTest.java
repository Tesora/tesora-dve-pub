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
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class DeleteOrderByLimitTest extends SchemaMirrorTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL = new PEDDL("sysdb",
			new StorageGroupDDL("sys", SITES, "sysg"), "schema");

	static final NativeDDL nativeDDL = new NativeDDL("cdb");

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
		setup(sysDDL, null, nativeDDL, getSchema());
	}

	static final String[] tabNames = new String[] { "B", "R", "A", "S" };
	static final String[] distVects = new String[] {
			"broadcast distribute",
			"range distribute on (`id1`, `id2`) using ", 
			"random distribute",
			"static distribute on (`id1`, `id2`)" };
	private static final String tabBody = " `id1` int, `id2` int, `pa` int(10), `pb` int(10), `pc` float, `pd` double, primary key (`id1`, `id2`)";

	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null)
					return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;
				if (ext)
					// declare the range
					mr.getConnection().execute(
							"create range open"
									+ mr.getDDL().getDatabaseName()
									+ " (int, int) persistent group "
									+ mr.getDDL().getPersistentGroup()
											.getName());
				List<String> actTabs = new ArrayList<String>();
				actTabs.addAll(Arrays.asList(tabNames));
				actTabs.add("T");
				for (int i = 0; i < actTabs.size(); i++) {
					String tn = actTabs.get(i);
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ")
							.append(tabBody).append(" ) ");
					if (ext && i < tabNames.length) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(
									mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		ArrayList<String> rows = new ArrayList<String>();
		for (int i = 1; i <= (2 * SITES); i++) {
			// id1, id2, pa, pb, pc, pd
			rows.add("( " + i + ", " + i + ", " + i + "," + i + ", '" + i
					+ ".0', '" + i + ".0' )");
		}
		String rest = "(`id1`, `id2`, `pa`, `pb`, `pc`, `pd`) values "
				+ Functional.join(rows, ", ");
		for (int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + rest));
		}
		return out;
	}

	private void forAll(String test, List<MirrorTest> acc, String[] tables) {
		for (int i = 0; i < tables.length; i++) {
			final String actual = test.replace("#", tables[i]);
			acc.add(new StatementMirrorProc(actual));
		}
	}

	@Test
	public void testDeleteOrderByLimit() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for (int i = 0; i < tabNames.length; i++) {
			forAll("delete from # where `id1` < 6 order by `id1`, `id2` limit 0",
					out, tabNames);
//			forAll("delete from # where `id1` < 6 order by `id1`, `id2` limit 3",
//					out, tabNames);
		}
		runTest(out);
	}

	@Test
	public void testPE770() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for (int i = 0; i < tabNames.length; i++) {
			forAll("delete LOW_PRIORITY from #",
					out, tabNames);
			forAll("delete QUICK from #",
					out, tabNames);
			forAll("delete LOW_PRIORITY QUICK from #",
					out, tabNames);
		}
		runTest(out);
	}

}
