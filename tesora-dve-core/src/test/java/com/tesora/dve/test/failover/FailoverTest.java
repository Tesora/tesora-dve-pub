// OS_STATUS: public
package com.tesora.dve.test.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.common.SiteInstanceStatus;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.PersistentSiteAccessor;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.DBConnectionParameters;
import com.tesora.dve.worker.FailingMasterMasterStatement;
import com.tesora.dve.worker.FailingMasterMasterWorker;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.NotificationManagerSync;
import com.tesora.dve.worker.UserCredentials;
public class FailoverTest extends PETest {

	public Logger logger = Logger.getLogger(FailoverTest.class);

	public SSConnectionProxy conProxy;
	public static DBConnectionParameters dbParams;
	
	static UserDatabase db;
	static SiteInstance instanceA;
	static SiteInstance instanceB;
	static PersistentSite haSite;
	static PersistentGroup haGroup;

	SSConnection ssConnection;
	static UserTable fuu;
	
	QueryPlan plan;

	@BeforeClass
	public static void setup() throws Exception {
		TestCatalogHelper.createTestCatalog(PETest.class,2);
		bootHost = BootstrapHost.startServices(PETest.class);

        populateMetadata(FailoverTest.class, Singletons.require(HostService.class).getProperties());
        populateSites(FailoverTest.class, Singletons.require(HostService.class).getProperties());

		PersistentSiteAccessor.injectTestClasses();

        dbParams = new DBConnectionParameters(Singletons.require(HostService.class).getProperties());
		
		PersistentSite site1 = catalogDAO.findPersistentSite("site1");
		
		DistributionModel broadcast = catalogDAO.findDistributionModel(BroadcastDistributionModel.MODEL_NAME);

		catalogDAO.begin();

		SiteInstance site1Instance = site1.getMasterInstance();

		instanceA = catalogDAO.createSiteInstance(site1.getName() + "_1", site1.getMasterUrl(), site1Instance.getUser(), site1Instance.getDecryptedPassword());
		instanceB = catalogDAO.createSiteInstance(site1.getName() + "_2", site1.getMasterUrl(), site1Instance.getUser(), site1Instance.getDecryptedPassword());

		haSite = catalogDAO.createPersistentSite("ha1", FailingMasterMasterWorker.HA_TYPE, instanceA, new SiteInstance[]{instanceB});
		haGroup = catalogDAO.createPersistentGroup("haGroup");
		haGroup.addStorageSite(haSite);
		catalogDAO.persistToCatalog(haGroup);
        Singletons.require(HostService.class);
        Singletons.require(HostService.class);
        db = catalogDAO.createDatabase("hadb", haGroup, Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet(),
				Singletons.require(HostService.class).getDBNative().getDefaultServerCollation());
		fuu = catalogDAO.createUserTable(db, "fuu", broadcast, haGroup, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
		catalogDAO.commit();
		catalogDAO.refresh(haGroup);
	}

	@Before
	public void setupEachTest() throws PEException, SQLException {
        DBHelper dbh = new DBHelper(Singletons.require(HostService.class).getProperties()).connect();
		dbh.executeQuery("use " + PEConstants.CATALOG);
		dbh.executeQuery("update storage_site set haType='FailingSingle' where name in ('site1','site2')");
		dbh.executeQuery("update site_instance set status='ONLINE'");
		dbh.disconnect();
		CatalogDAOFactory.clearCache();
		
//		CatalogDAO c = CatalogDAOFactory.newInstance();
//		EntityManager em = CatalogDAOAccessor.getEntityManager(c);
//		em.getTransaction().begin();
//		Query q;
//		q = em.createQuery("update PersistentSite set haType='FailingSingle' where name in ('site1','site2')");
//		assertEquals(2,q.executeUpdate());
//		q = em.createQuery("update SiteInstance set isEnabled=1 where name in ('site1_1','site1_2')");
//		assertEquals(2,q.executeUpdate());
//		em.getTransaction().commit();
//		c.close();

		getSSConnection();
		plan = new QueryPlan();

		FailingMasterMasterStatement.setFailProbability(1);
		
		catalogDAO.begin();
//		PersistentSite site1 = catalogDAO.findStorageSite("site1");
//		site1.setHaType(FailingSingleWorker.HA_TYPE);
//		PersistentSite site2 = catalogDAO.findStorageSite("site2");
//		site2.setHaType(FailingSingleWorker.HA_TYPE);
		catalogDAO.refresh(instanceA);
		instanceA.setStatus(SiteInstanceStatus.ONLINE.name());
		instanceA.setMaster(true);
		catalogDAO.refresh(instanceB);
		instanceB.setStatus(SiteInstanceStatus.ONLINE.name());
		instanceB.setMaster(false);
		catalogDAO.commit();
		
	}

	private SSConnection getSSConnection() throws PEException {
		if (conProxy == null) {
			conProxy = new SSConnectionProxy();
			ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
			SSConnectionAccessor.setCatalogDAO(ssConnection, CatalogDAOFactory.newInstance());
			ssConnection.startConnection(new UserCredentials(PEConstants.ROOT, PEConstants.PASSWORD));
			ssConnection.setPersistentDatabase(db);
		}
		return ssConnection;
	}
	
	private void closeSSConnection() throws PEException {
		if (conProxy != null) {
			ssConnection.getCatalogDAO().close();
			conProxy.close();
			conProxy = null;
		}
	}
	
	@After
	public void testCleanup() throws PEException {
		closeSSConnection();
		if(plan != null)
			plan.close();
		plan = null;
	}

	@Test
	public void testMasterMasterFailover() throws Throwable {
		
		NotificationManager.PER_SECOND_THRESHOLD = 1;
		
		catalogDAO.refresh(haSite);
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertTrue(instanceA.isMaster());
		assertTrue(instanceA.isEnabled());
		assertFalse(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());
		
		FailingMasterMasterStatement.setFailProbability(1);
		QueryStepOperation op = new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON, 
				new SQLCommand("select * from " + fuu.getNameAsIdentifier()));
		plan.addStep(new QueryStep(haGroup, op));
		try {
			MysqlTextResultCollector rc = new MysqlTextResultCollector();
			plan.executeStep(getSSConnection(), rc);
			assertTrue("executeStep should have thrown CommunicationsException", rc.hasResults());
		} catch (Exception e) {
			boolean communicationsExceptionCaught = false;
			for (Throwable root=e; (root=root.getCause()) != null && !communicationsExceptionCaught;)
				if (root instanceof PECommunicationsException)
					communicationsExceptionCaught = true;
			if (!communicationsExceptionCaught)
				throw e;
		}
		
		// Give the NotificationManager a chance to eject the old master
        getSSConnection().sendAndReceive(getSSConnection().newEnvelope(new NotificationManagerSync()).to(Singletons.require(HostService.class).getNotificationManagerAddress()));
		closeSSConnection();
		
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertFalse(instanceA.isMaster());
		assertFalse(instanceA.isEnabled());
		assertTrue(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());
		
		FailingMasterMasterStatement.setFailProbability(0);
		try {
			MysqlTextResultCollector rc = new MysqlTextResultCollector();
			plan.executeStep(getSSConnection(), rc);
			assertTrue(rc.hasResults());
		} catch (Exception e) {
			for (Throwable root=e; (root=root.getCause()) != null;)
				if (root instanceof MySQLSyntaxErrorException)
					throw root;
		}
	}
	
	@Test
	public void testFailedFailback() throws Throwable {
		
		NotificationManager.PER_SECOND_THRESHOLD = 1;
		
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertTrue(instanceA.isMaster());
		assertTrue(instanceA.isEnabled());
		assertFalse(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());
		
		FailingMasterMasterStatement.setFailProbability(1);
		QueryStepOperation op = new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON, 
				new SQLCommand("select * from " + fuu.getNameAsIdentifier()));
		plan.addStep(new QueryStep(haGroup, op));
		try {
			MysqlTextResultCollector rc = new MysqlTextResultCollector();
			plan.executeStep(getSSConnection(), rc);
			assertTrue("executeStep should have thrown PECommunicationsException", rc.hasResults());
		} catch (Exception e) {
			boolean communicationsExecptionCaught = false;
			for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
				if (root instanceof PECommunicationsException)
					communicationsExecptionCaught = true;
			if (!communicationsExecptionCaught)
				throw e;
		}
		
		// Give the NotificationManager a chance to eject the old master
        getSSConnection().sendAndReceive(getSSConnection().newEnvelope(new NotificationManagerSync()).to(Singletons.require(HostService.class).getNotificationManagerAddress()));
		closeSSConnection();
		
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertFalse(instanceA.isMaster());
		assertFalse(instanceA.isEnabled());
		assertTrue(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());

		FailingMasterMasterStatement.setFailProbability(0);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				for (Throwable root=e; (root=root.getCause()) != null;)
					if (root instanceof MySQLSyntaxErrorException)
						throw root;
			}
		} catch (MySQLSyntaxErrorException e) {
			// this is what we are looking for
		} catch (Throwable t) {
			throw t;
		}
		
		// now cause the second site failure
		FailingMasterMasterStatement.setFailProbability(1);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				boolean communicationsExecptionCaught = false;
				for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
					if (root instanceof PECommunicationsException)
						communicationsExecptionCaught = true;
				if (!communicationsExecptionCaught)
					throw e;
			}
		} catch (PECommunicationsException e) {
			// this is what we are looking for
		} catch (Throwable t) {
			throw t;
		}

		// Give the NotificationManager a chance to eject the old master
        getSSConnection().sendAndReceive(getSSConnection().newEnvelope(new NotificationManagerSync()).to(Singletons.require(HostService.class).getNotificationManagerAddress()));
		closeSSConnection();
		
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertFalse(instanceA.isMaster());
		assertFalse(instanceA.isEnabled());
		assertFalse(instanceB.isMaster());
		assertFalse(instanceB.isEnabled());

		FailingMasterMasterStatement.setFailProbability(0);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				boolean communicationsExecptionCaught = false;
				for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
					if (root instanceof PECommunicationsException)
						communicationsExecptionCaught = true;
				if (!communicationsExecptionCaught)
					throw e;
			}
		} catch (PEException e) {
			if (e.hasCause(PESQLStateException.class))
				;// this is what we are looking for
			else 
				throw e;
		} catch (Throwable t) {
			throw t;
		}
	}
	
	@Test
	public void testSuccessfulFailback() throws Throwable {
		
		NotificationManager.PER_SECOND_THRESHOLD = 1;
		
		catalogDAO.refresh(haSite);
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertEquals(haSite.getMasterInstance(), instanceA);
		assertTrue(instanceA.isMaster());
		assertTrue(instanceA.isEnabled());
		assertFalse(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());
		
		FailingMasterMasterStatement.setFailProbability(1);
		QueryStepOperation op = new QueryStepSelectAllOperation(db, BroadcastDistributionModel.SINGLETON, 
				new SQLCommand("select * from " + fuu.getNameAsIdentifier()));
		plan.addStep(new QueryStep(haGroup, op));
		try {
			MysqlTextResultCollector rc = new MysqlTextResultCollector();
			plan.executeStep(getSSConnection(), rc);
			assertTrue("executeStep should have thrown PECommunicationsException", rc.hasResults());
		} catch (Exception e) {
			boolean communicationsExecptionCaught = false;
			for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
				if (root instanceof PECommunicationsException)
					communicationsExecptionCaught = true;
			if (!communicationsExecptionCaught)
				throw e;
		}
		
		// Give the NotificationManager a chance to eject the old master
        getSSConnection().sendAndReceive(getSSConnection().newEnvelope(new NotificationManagerSync()).to(Singletons.require(HostService.class).getNotificationManagerAddress()));
		closeSSConnection();
		
		catalogDAO.refresh(haSite);
//		haSite = catalogDAO.findByKey(haSite.getClass(), haSite.getId());
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
//		assertEquals(haSite.getMasterInstance(), instanceB);
		assertFalse(instanceA.isMaster());
		assertFalse(instanceA.isEnabled());
		assertTrue(instanceB.isMaster());
		assertTrue(instanceB.isEnabled());

		FailingMasterMasterStatement.setFailProbability(0);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				for (Throwable root=e; (root=root.getCause()) != null;)
					if (root instanceof MySQLSyntaxErrorException)
						throw root;
			}
		} catch (MySQLSyntaxErrorException e) {
			// this is what we are looking for
		} catch (Throwable t) {
			throw t;
		}
		
		// now cause the second site failure
		FailingMasterMasterStatement.setFailProbability(1);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				boolean communicationsExecptionCaught = false;
				for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
					if (root instanceof PECommunicationsException)
						communicationsExecptionCaught = true;
				if (!communicationsExecptionCaught)
					throw e;
			}
		} catch (PECommunicationsException e) {
			// this is what we are looking for
		} catch (Throwable t) {
			throw t;
		}

		// Give the NotificationManager a chance to eject the old master
        getSSConnection().sendAndReceive(getSSConnection().newEnvelope(new NotificationManagerSync()).to(Singletons.require(HostService.class).getNotificationManagerAddress()));
		closeSSConnection();
		
		catalogDAO.refresh(instanceA);
		catalogDAO.refresh(instanceB);
		assertFalse(instanceA.isMaster());
		assertFalse(instanceA.isEnabled());
		assertFalse(instanceB.isMaster());
		assertFalse(instanceB.isEnabled());

		catalogDAO.begin();
		catalogDAO.refresh(instanceA);
		instanceA.setStatus(SiteInstanceStatus.ONLINE.name());
		catalogDAO.commit();

		FailingMasterMasterStatement.setFailProbability(0);
		try {
			try {
				MysqlTextResultCollector rc = new MysqlTextResultCollector();
				plan.executeStep(getSSConnection(), rc);
				assertTrue(rc.hasResults());
			} catch (Exception e) {
				boolean communicationsExecptionCaught = false;
				for (Throwable root=e; (root=root.getCause()) != null && !communicationsExecptionCaught;)
					if (root instanceof PECommunicationsException)
						communicationsExecptionCaught = true;
				if (!communicationsExecptionCaught)
					throw e;
			}
		} catch (PEException e) {
			if (e.hasCause(PESQLStateException.class))
				;// this is what we are looking for
			else 
				throw e;
		} catch (Throwable t) {
			throw t;
		}
	}
}
