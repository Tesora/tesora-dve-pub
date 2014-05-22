package com.tesora.dve.common;

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
