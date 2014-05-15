// OS_STATUS: public
package com.tesora.dve.sql;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.standalone.PETest;

public class TestEmptyCatalog extends SchemaTest {

	// the tests for when we don't have any persistent groups or sites

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup();
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	@Test
	public void testEmptyCatalog() throws Throwable {
		try (ProxyConnectionResource sysconn = new ProxyConnectionResource()) {
            sysconn.assertResults("select @@version_comment limit 1",br(nr, Singletons.require(HostService.class).getServerVersionComment()));
            sysconn.assertResults("select @@version limit 1",br(nr, Singletons.require(HostService.class).getServerVersion()));
            sysconn.assertResults("select /* test comments */ @@version /* again */ limit /* at last */ 1",br(nr, Singletons.require(HostService.class).getServerVersion()));
			sysconn.assertResults("/* absolutely nothing */",br());
			sysconn.assertResults(";",br());
			sysconn.assertResults("/*!40101 SET @saved_cs_client     = @@character_set_client */;",br());
			sysconn.assertResults("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */     ",br());
		}
	}
}
