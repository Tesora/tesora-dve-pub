// OS_STATUS: public
package com.tesora.dve.common;

import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;

import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.standalone.PETest;

public class CatalogHelperTest {

	@Test
	public void testDumpCatalog() throws PEException {
		TestCatalogHelper ch = null;
		try {
			ch = new TestCatalogHelper(PETest.class);
			ch.createTestCatalogWithDB(2, false);

			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ch.dumpCatalogInfo(pw);
			assertTrue(sw.toString().contains("Catalog contains:"));
		} finally {
			if ( ch != null )
				ch.close();
		}
	}

}
