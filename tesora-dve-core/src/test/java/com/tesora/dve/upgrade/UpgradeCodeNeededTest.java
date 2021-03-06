package com.tesora.dve.upgrade;

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
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.spi.CatalogGenerator;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.standalone.PETest;

public class UpgradeCodeNeededTest extends PETest {
	
	@BeforeClass
	public static void startUp() throws Exception {
		TestCatalogHelper.createTestCatalog(PETest.class);

		bootHost = BootstrapHost.startServices(PETest.class);
	}
	
	@Test
	public void testCurrentVersionStructureAgainstGold() throws Throwable {
		int latestVersion = CatalogVersions.getCurrentVersion().getSchemaVersion();
		String[] lastKnown = UpgradeTestUtils.getGoldVersion(latestVersion);
		String[] current = generateCatalog();
		Pattern thanksHibernate = Pattern.compile("(alter table )(\\w+)( add index )(\\w+)(.*add constraint )(\\w+)(.*)");
		assertEquals("number of stmts in schema ddl",lastKnown.length, current.length - 1);
		for(int i = 0; i < current.length - 1; i++) {
			if (!lastKnown[i].equals(current[i])) {
				// accept differences in index, constraint names
				if (lastKnown[i].startsWith("alter table") && current[i].startsWith("alter table")) {
					Matcher last = thanksHibernate.matcher(lastKnown[i]);
					Matcher now = thanksHibernate.matcher(current[i]);
					if (last.matches() && now.matches()) {
						String knownConstraint = lastKnown[i].substring(last.start(6),last.end(6));
						String knownIndex = lastKnown[i].substring(last.start(4),last.end(4));

						StringBuilder buf = new StringBuilder();
						buf.append(current[i].substring(0,now.start(4)));
						buf.append(knownIndex);
						buf.append(current[i].substring(now.end(4),now.start(6)));
						buf.append(knownConstraint);
						buf.append(current[i].substring(now.end(6)));
						String munged = buf.toString();
						if (lastKnown[i].equals(munged))
							continue;
					}
				}				
				assertEquals("schema ddl stmt " + i, lastKnown[i], current[i]);
			}
		}
	}

	public String[] generateCatalog() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		String[] current = null;
		try {
			CatalogGenerator generator = Singletons.require(CatalogGenerator.class);
            current = generator.buildCreateCurrentSchema(c, Singletons.require(HostService.class).getProperties()); //TODO: this looks like we are only looking up the host to get something for the catalog. -sgossard
		} finally {
			c.close();
		}
		return current;
	}


	@Test
	public void testCurrentVersionContentsAgainstGold() throws Throwable {
		int latestVersion = CatalogVersions.getCurrentVersion().getSchemaVersion();
		String[] lastKnown = UpgradeTestUtils.getInfoSchema(latestVersion);
		String[] current = CatalogSchemaGenerator.buildTestCurrentInfoSchema();
		assertEquals("number of stmts in info schema dml",lastKnown.length,current.length);
		for(int i = 0; i < lastKnown.length; i++) {
			assertEquals("info schema dml " + i,lastKnown[i],current[i]);
		}
	}
	
	private static final String propDirName = "com.tesora.dve.upgrade.golddir";
	private static final String propEnableName = "com.tesora.dve.upgrade.writegold";
	
	@Test
	public void writeCurrentSchemaGold() throws Throwable {
		if (Boolean.getBoolean(propEnableName)) {
			String filename = System.getProperty(propDirName);
			if (filename == null)
				throw new PEException("Must specify a directory via -D" + propDirName + " prior to running writeCurrentSchemaGold.");
			int latestVersion = CatalogVersions.getCurrentVersion().getSchemaVersion();
			String[] current = generateCatalog();
			writeFile(filename, "catalog_version" + latestVersion + ".sql",current,current.length - 1);			
			current = CatalogSchemaGenerator.buildTestCurrentInfoSchema();
			writeFile(filename, "infoschema_version" + latestVersion + ".sql",current,current.length);			
		}
	}

	private void writeFile(String dirName, String fn, String[] contents, int nrows) throws Throwable {
		File golddir = new File(dirName);
		File nf = new File(golddir,fn);
		PrintWriter pw = new PrintWriter(nf);
		try {
			for(int i = 0; i < nrows; i++) {
				pw.println(contents[i] + ";");
			}
		} finally {
			pw.close();
		}
		System.out.println("Wrote file " + nf);
	}
	
}
