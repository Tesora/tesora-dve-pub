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
// NOPMD by doug on 04/12/12 12:05 PM


import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.template.jaxb.FkModeType;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class MetadataInjectionTest extends SchemaTest {

	// only one site for this test
	private static final int SITES = 1;
	
	private static final ProjectDDL testDDL = 
		new PEDDL("mitdb",
				new StorageGroupDDL("mit",SITES,"mitg"),"schema");
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	ProxyConnectionResource conn = null;
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		testDDL.clearCreated();
		testDDL.create(conn);
		conn.execute("drop database mitdb");
	}
	
	@After
	public void after() throws Throwable {
		testDDL.destroy(conn);
		if(conn != null) {
			conn.execute("drop template if exists mit");
			conn.disconnect();
		}
		conn = null;
	}
	
	@Test
	public void testInjection() throws Throwable {
		
		// ok, let's start by ensuring the range doesn't exist yet
		conn.assertResults("show ranges like 'openrange'",br());
		
		conn.execute(new TemplateBuilder("mit")
			.withRequirement("create range openrange (int) persistent group #sg#")
			.withRequirement("create range closedrange (int) persistent group #sg#")
			.withRangeTable("laws","openrange","id")
			.toCreateStatement());
		conn.execute("create database mitdb default persistent group mitg using template mit strict");
		conn.assertResults("select template,  template_mode from information_schema.schemata where schema_name = 'mitdb'",
				br(nr, "mit", "STRICT"));
		conn.assertResults("show ranges like 'openrange'",br(nr,"openrange",testDDL.getPersistentGroup().getName(),"int"));
		conn.assertResults("show ranges like 'closedrange'",br(nr,"closedrange",testDDL.getPersistentGroup().getName(),"int"));
	
		// set the database on the connection
		conn.execute("use mitdb");
		
		// create the table - this should pass
		conn.execute("create table `laws` (`id` int auto_increment, `law` longtext)");
		conn.assertResults("select model_type,  model_name from information_schema.distributions where database_name = 'mitdb' and table_name = 'laws'",
				br(nr,RangeDistributionModel.MODEL_NAME,"openrange"));
		
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {

			// this should fail, as we had strict on
			conn.execute("create table `titles` (`id` int auto_increment, `name` varchar(50))");
			fail("strict template should cause failure on missing template");

			}
		}.assertError(SchemaException.class, MySQLErrors.internalFormatter,
					"Internal error: No matching template found for table `titles`");

		conn.execute("drop database mitdb");

		// set the template
		conn.execute(new TemplateBuilder("mit")
			.withRangeTable("laws","longrange","id").toAlterStatement());
		conn.execute("create database mitdb default persistent group mitg using template mit strict");

		// set the database on the connection
		conn.execute("use mitdb");
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {

			conn.execute("create table `laws` (`id` int auto_increment, `law` longtext)");
			fail("Missing range should be caught");

			}
		}.assertError(SchemaException.class, MySQLErrors.internalFormatter,
					"Internal error: No such range from template 'mit' on storage group mitg: longrange");
		
		conn.execute("drop database mitdb");
		// set the template again, this time with a regex
		conn.execute(new TemplateBuilder("mit")
			.withRequirement("create range longrange (int) persistent group #sg#")
			.withRangeTable(".*_laws","longrange","id")
			.toAlterStatement());
		conn.execute("create database mitdb default persistent group mitg using template mit strict");
		conn.execute("use mitdb");
		conn.execute("create table `lefty_laws` (`id` int auto_increment, `law` longtext)");
		conn.assertResults("select model_type,  model_name from information_schema.distributions where database_name = 'mitdb' and table_name = 'lefty_laws'",
				br(nr,RangeDistributionModel.MODEL_NAME,"longrange"));
		
		conn.execute("drop database mitdb");
		conn.execute("create database mitdb default persistent group mitg using template mit");
		conn.execute("use mitdb");
		conn.execute("create table `righty_lawyers`(`id` int auto_increment, `law` longtext)");
		conn.assertResults("select model_type from information_schema.distributions where database_name = 'mitdb' and table_name = 'righty_lawyers'",
				br(nr,RandomDistributionModel.MODEL_NAME));
	}	

	@Test
	public void testDefaultBroadcast() throws Throwable {
		conn.execute(new TemplateBuilder("mit")
			.withRequirement("create range openrange (int) persistent group #sg#")
			.withRequirement("create range closedrange (int) persistent group #sg#")
			.withRangeTable(".*cache", "openrange", "id")
			.withRangeTable(".*states", "openrange", "id")
			.withTable(".*", BroadcastDistributionModel.MODEL_NAME)
			.toCreateStatement());
		conn.execute("create database mitdb default persistent group mitg using template mit strict");
		conn.execute("use mitdb");
		conn.execute("create table `should_match_cache` (`id` int auto_increment, `law` longtext)");
		conn.execute("create table `no_match_at_all` (`id` int auto_increment, `law` longtext)");

		conn.assertResults("select model_type,  model_name from information_schema.distributions where database_name = 'mitdb' and table_name = 'should_match_cache'",
				br(nr,RangeDistributionModel.MODEL_NAME,"openrange"));
		conn.assertResults("select model_type from information_schema.distributions where database_name = 'mitdb' and table_name = 'no_match_at_all'",
				br(nr,BroadcastDistributionModel.MODEL_NAME));
		
	}
	
	@Test
	public void testFKModeInjection() throws Throwable {
		conn.execute(new TemplateBuilder("mit")
			.withFKMode(FkModeType.IGNORE)
			.withTable(".*", BroadcastDistributionModel.MODEL_NAME)
			.toCreateStatement());
		conn.execute("create database mitdb default persistent group mitg using template mit strict");
		conn.assertResults("select * from information_schema.schemata where schema_name = 'mitdb'",
				br(nr, "def", "mitdb", "mitg", "mit", "STRICT", "off", "ignore", "utf8", "utf8_general_ci"));
	}
	
	@Test
	public void testAdditionalFields() throws Throwable {
		TemplateBuilder tb = new TemplateBuilder("mit").withTable(".*", BroadcastDistributionModel.MODEL_NAME);
		String basic = tb.toCreateStatement();
		conn.execute(basic + " match='mit*' comment='test comment'");
		conn.assertResults("select dbmatch,  template_comment from information_schema.templates where name = 'mit'",
				br(nr,"mit*","test comment"));
		conn.execute("alter template mit set comment = 'new comment'");
		conn.assertResults("select dbmatch,  template_comment from information_schema.templates where name = 'mit'",
				br(nr,"mit*","new comment"));
		conn.execute("alter template mit set match='mitdb' comment='exact match'");
		conn.assertResults("select dbmatch,  template_comment from information_schema.templates where name = 'mit'",
				br(nr,"mitdb","exact match"));
		conn.execute("create database mitdb default persistent group mitg");
		conn.assertResults("select template,  template_mode from information_schema.schemata where schema_name = 'mitdb'",
				br(nr, "mit", TemplateMode.getCurrentDefault().toString()));
	}
	
	@Test
	public void testRangeScoping() throws Throwable {
		TemplateBuilder tb = new TemplateBuilder("rst")
				.withRequirement("create range openrange (int) persistent group #sg#")
				.withRangeTable(".*cache", "openrange", "id");
		conn.execute(tb.toCreateStatement());
		try {
			conn.execute("create persistent group ga add mit0");
			conn.execute("create persistent group gb add mit0");
			conn.execute("create database dba default persistent group ga using template rst");
			conn.assertResults("show ranges", br(nr,"openrange","ga","int"));
			conn.execute("create database dbb default persistent group gb using template rst");
			conn.assertResults("show ranges", br(nr,"openrange","ga","int", nr, "openrange","gb","int"));
			try {
				conn.execute("drop persistent group ga");
				fail("shouldn't be able to drop the group - has a db");
			} catch (Throwable t) {
				SchemaTest.assertSchemaException(t, "Unable to drop persistent group ga because used by database dba");
			}
			conn.execute("drop database dba");
			conn.execute("drop persistent group ga");
			conn.execute("create persistent group ga add mit0");
			conn.execute("create database dba default persistent group ga using template rst");
			conn.execute("drop database dba");
			try {
				conn.execute("drop range openrange");
				fail("ambiguous, shouldn't be able to drop");
			} catch (Throwable t) {
				SchemaTest.assertSchemaException(t, "More than one range named openrange please specify group (add persistent group <name>)");
			}
			conn.execute("drop range openrange persistent group ga");
		} finally {
			conn.execute("drop database if exists dba");
			conn.execute("drop database if exists dbb");
			conn.execute("drop persistent group ga");
			conn.execute("drop persistent group gb");
		}

	}
}
