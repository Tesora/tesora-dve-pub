// OS_STATUS: public
package com.tesora.dve.tools;

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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerCorpus;
import com.tesora.dve.tools.analyzer.jaxb.StatementPopulationType;

public class DVEAnalyzerCLITest extends SchemaTest {

	private static final String TEST_DATABASE_NAME = "analyzertestdb";
	private static final String TEST_TABLE_NAME = "A";
	private static final NativeDDL nativeDDL = new NativeDDL(TEST_DATABASE_NAME);
	private static DBHelperConnectionResource nativeConnection;
	private static TestResource nativeResource;

	@BeforeClass
	public static void setUp() throws Throwable {
		PETest.projectSetup(nativeDDL);
		nativeConnection = new DBHelperConnectionResource();
		nativeResource = new TestResource(nativeConnection, nativeDDL);
		nativeResource.create();

		nativeConnection.execute("USE " + TEST_DATABASE_NAME);
		nativeConnection.execute("CREATE TABLE IF NOT EXISTS " + TEST_TABLE_NAME + " ("
				+ "id INT NOT NULL AUTO_INCREMENT,"
				+ "name TEXT NOT NULL,"
				+ "PRIMARY KEY (id))");
		nativeConnection.execute("INSERT INTO " + TEST_TABLE_NAME + " VALUES (1, 'ParElastic')");
	}

	@AfterClass
	public static void tearDown() throws Throwable {
		nativeResource.destroy();
		nativeConnection.disconnect();
	}

	/**
	 * Generate templates while testing various template generator methods.
	 */
	private static void testTemplateGenerators(final DVEClientToolTestConsole console, final String cardinalityCutoff, final String frequencyCorpus) {
		console.executeCommand("generate broadcast templates");
		console.executeCommand("generate random templates");
		console.executeCommand("generate basic templates " + cardinalityCutoff);
		console.executeCommand("generate guided templates " + cardinalityCutoff + " false false " + frequencyCorpus);
		console.executeCommand("generate templates false false " + frequencyCorpus);
	}

	private static void assertStatementCounts(final String frequencyCorpus, final Map<String, Integer> expectedStatementCounts) throws PEException {
		final DbAnalyzerCorpus frequencyAnalysis = PEXmlUtils.unmarshalJAXB(
				new File(frequencyCorpus),
				DbAnalyzerCorpus.class);

		for (final StatementPopulationType statement : frequencyAnalysis
				.getPopulation()) {
			final String statementKind = statement.getKind();
			if (expectedStatementCounts.containsKey(statementKind)) {
				assertEquals(expectedStatementCounts.get(statementKind), Integer.valueOf(statement.getFreq()));
			}
		}
	}

	@SuppressWarnings("resource")
	@Test
	public void testDynamicAnalysisStack() throws Throwable {
		final DVEClientToolTestConsole console = new DVEClientToolTestConsole(new DVEAnalyzerCLI(null));
		final String staticReport = getTempFile("static", null);
		final String frequencyCorpus = getTempFile("corpus", null);
		final String template = getTempFile("template", null);
		final String generalLog = getTempFile("general",
				Arrays.asList(
						"		1 Connect	" + nativeConnection.getUserid() + "@localhost on " + TEST_DATABASE_NAME,
						"		1 Query	SELECT * FROM " + TEST_TABLE_NAME,
						"		1 Quit	"
						)
				);
		final String dynamicLog = getTempFile("dynamic", null);

		console.executeCommand("connect " + getConnectionString());
		console.executeCommand("set database " + TEST_DATABASE_NAME);
		console.executeCommand("static");
		console.executeCommand("save report " + staticReport);
		console.executeCommand("frequencies mysql " + frequencyCorpus + " "
				+ generalLog + " " + generalLog + " " + generalLog);

		testTemplateGenerators(console, "10", frequencyCorpus);

		console.executeCommand("save template " + TEST_DATABASE_NAME + " " + template);
		console.executeCommand("dynamic mysql " + generalLog + " " + dynamicLog);

		console.assertValidConsoleOutput();

		final Map<String, Integer> expectedStatementCounts = new HashMap<String, Integer>();
		expectedStatementCounts.put(SelectStatement.class.getSimpleName(), 3);
		assertStatementCounts(frequencyCorpus, expectedStatementCounts);
	}

	private String getTempFile(final String name, final List<String> lines) throws IOException {
		final File tempFile = File.createTempFile("PEDBAnalyzerTest_" + name, ".tmp");

		if (lines != null) {
			FileUtils.writeLines(tempFile, lines);
		}

		return tempFile.getCanonicalPath();
	}

	private String getConnectionString() throws Throwable {
		return nativeConnection.getUrl() + " " + nativeConnection.getUserid() + " " + nativeConnection.getPassword();
	}

}
