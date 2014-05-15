// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class SessVarsTest extends SchemaMirrorTest {

	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getSingleDDL() {
		return checkDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void setup() throws Throwable {
		setup(null, checkDDL, nativeDDL, Collections.EMPTY_LIST);
	}

	@Test
	public void testPE685() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("set SQL_MODE=IFNULL(@OLD_SQL_MODE,'')"));
		tests.add(new StatementMirrorProc("set FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS=0,0,1)"));
		// make sure that we actually pass everything through
		tests.add(new StatementMirrorProc("set @OLD_SQL_MODE='foobar'"));
		tests.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				try {
					mr.getConnection().execute("set SQL_MODE=IFNULL(@OLD_SQL_MODE,'')");
				} catch (Throwable t) {
					// supposed to fail
					return null;
				}
				throw new Throwable("Setting sql_mode to foobar should fail!");
			}
			
		});
		runTest(tests);
	}
	
	@Test
	public void testPE1329() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("set @@autocommit = off"));
		// PE-1481 below
		tests.add(new StatementMirrorProc("SET collation_connection = utf8mb4_unicode_ci"));
		runTest(tests);
	}
	
}
