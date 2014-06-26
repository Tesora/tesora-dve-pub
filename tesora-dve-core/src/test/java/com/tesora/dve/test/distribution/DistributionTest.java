package com.tesora.dve.test.distribution;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageGroupAccessor;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.GenerationKeyRange;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStepInsertByKeyOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepSelectByKeyOperation;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.queryplan.QueryStepUpdateByKeyOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.UserCredentials;

public class DistributionTest extends PETest{

	public Logger logger = Logger.getLogger(DistributionTest.class);

	static SSConnection ssConnection;
	static SSConnectionProxy ssConnProxy;

	static UserTable tRand;
	static UserTable tBcast;
	static UserTable tStatic;
	static UserTable tRandGen2;
	static UserTable tBcastGen2;
	static UserTable tStaticGen2;
	static UserTable tRandGen3;
	static UserTable tBcastGen3;
	static UserTable tStaticGen3;
	static UserTable tRange;
	static UserTable tRangeGen2;
	static UserTable tRangeGen3;
	static UserTable tRandOneSite;
	static UserTable tBcastOneSite;
	static UserTable tStaticOneSite;
	static UserTable tRangeOneSite;

	static Map<String, PersistentSite> siteMap = new HashMap<String, PersistentSite>();

	@BeforeClass
	public static void setup() throws Exception {
		Class<?> bootClass = PETest.class;
		TestCatalogHelper.createTestCatalog(bootClass,2);
		bootHost = BootstrapHost.startServices(bootClass);
        populateMetadata(DistributionTest.class, Singletons.require(HostService.class).getProperties());

        populateSites(DistributionTest.class, Singletons.require(HostService.class).getProperties());

		ssConnProxy = new SSConnectionProxy();
		ssConnection = SSConnectionAccessor.getSSConnection(ssConnProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		UserDatabase db = ssConnection.getPersistentDatabase();

		tRand = db.getTableByName("Random");
		tBcast = db.getTableByName("Broadcast");
		tStatic = db.getTableByName("Static");
		tRange = db.getTableByName("Range");
		tRangeGen2 = db.getTableByName("RangeGen2");
		tRangeGen3 = db.getTableByName("RangeGen3");
		tRandGen2 = db.getTableByName("RandomGen2");
		tBcastGen2 = db.getTableByName("BroadcastGen2");
		tStaticGen2 = db.getTableByName("StaticGen2");
		tRandGen3 = db.getTableByName("RandomGen3");
		tBcastGen3 = db.getTableByName("BroadcastGen3");
		tStaticGen3 = db.getTableByName("StaticGen3");
		tRandOneSite = db.getTableByName("RandomOneSite");
		tBcastOneSite = db.getTableByName("BroadcastOneSite");
		tStaticOneSite = db.getTableByName("StaticOneSite");
		tRangeOneSite = db.getTableByName("RangeOneSite");

		siteMap = Functional.buildMap(catalogDAO.findAllPersistentSites(), new UnaryFunction<String, PersistentSite>() {
			@Override
			public String evaluate(PersistentSite site) {
				return site.getName();
			}
		});
	}

	@AfterClass
	public static void shutdown() throws Exception {
		ssConnProxy.close();
	}

	@Test public void randSelectAll() throws Throwable { selectAll(tRand, 5); }
	@Test public void randSelectOne() throws Throwable { selectOne(tRand, 1, 1); }
	@Test public void randSelectOneByKey() throws Throwable { selectByKey(tRand, 1); }
	@Test public void randSelectTwoByKey() throws Throwable { selectByKey(tRand, 2); }
	@Test public void randSelectThreeByKey() throws Throwable { selectByKey(tRand, 3); }
	@Test public void randInsertOneRecord() throws Throwable { insertOneRecord(tRand, 10); }
	@Test public void randUpdateOne() throws Throwable { updateOne(tRand, 1, "ThereOne"); }
	@Test public void randUpdateThree() throws Throwable { updateOne(tRand, 3, "ThereThree"); }
	@Test public void randUpdateAll() throws Throwable { updateRange(tRand, 1, 4, 4); }
	@Test public void randInsertAndDelete() throws Throwable { insertAndDelete(tRand, 11, "Hello"); }

	@Test public void randSelectThreeByKeyTxn() throws Throwable { ssConnection.userBeginTransaction(); selectByKey(tRand, 3); ssConnection.userCommitTransaction();}
	@Test public void randInsertOneRecordTxn() throws Throwable { ssConnection.userBeginTransaction(); insertOneRecord(tRand, 12);  ssConnection.userCommitTransaction();}
	@Test public void randUpdateOneTxn() throws Throwable { ssConnection.userBeginTransaction(); updateOne(tRand, 1, "ThereOne");  ssConnection.userCommitTransaction();}

	@Test public void bcastSelectAllFromAll() throws Throwable { selectAllFromAll(tBcast, 10); }
	@Test public void bcastSelectAll() throws Throwable { selectAll(tBcast, 5); }
	@Test public void bcastSelectOne() throws Throwable { selectOne(tBcast, 1, 1); }
	@Test public void bcastSelectOneByKey() throws Throwable { selectByKey(tBcast, 1); }
	@Test public void bcastSelectTwoByKey() throws Throwable { selectByKey(tBcast, 2); }
	@Test public void bcastSelectThreeByKey() throws Throwable { selectByKey(tBcast, 3); }
	@Test public void bcastSelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tBcast, 1, 1); }
	@Test public void bcastInsertOneRecord() throws Throwable { insertOneRecord(tBcast, 10); }
	@Test public void bcastUpdateOne() throws Throwable { updateOne(tBcast, 1, "ThereOne"); }
	@Test public void bcastUpdateThree() throws Throwable { updateOne(tBcast, 3, "ThereThree"); }
	@Test public void bcastUpdateAll() throws Throwable { updateRange(tBcast, 1, 4, 4); }
	@Test public void bcastInsertAndDelete() throws Throwable { insertAndDelete(tBcast, 11, "Hello"); }

	@Test public void staticSelectAll() throws Throwable { selectAll(tStatic, 5); }
	@Test public void staticSelectOneByKey() throws Throwable { selectByKey(tStatic, 1); }
	@Test public void staticSelectTwoByKey() throws Throwable { selectByKey(tStatic, 2); }
	@Test public void staticSelectThreeByKey() throws Throwable { selectByKey(tStatic, 3); }
	@Test public void staticSelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tStatic, 1, 0); }
	@Test public void staticInsertOneRecord() throws Throwable { insertOneRecord(tStatic, 10); }
	@Test public void staticUpdateOne() throws Throwable { updateOne(tStatic, 1, "ThereOne"); }
	@Test public void staticUpdateThree() throws Throwable { updateOne(tStatic, 3, "ThereThree"); }
	@Test public void staticUpdateAll() throws Throwable { updateRange(tStatic, 1, 4, 4); }
	@Test public void staticInsertAndDelete() throws Throwable { insertAndDelete(tStatic, 11, "Hello"); }

	@Test public void rangeSelectAll() throws Throwable { selectAll(tRange, 5); }
	@Test public void rangeSelectOneByKey() throws Throwable { selectByKey(tRange, 1); }
	@Test public void rangeSelectTwoByKey() throws Throwable { selectByKey(tRange, 2); }
	@Test public void rangeSelectThreeByKey() throws Throwable { selectByKey(tRange, 3); }
	@Test public void rangeSelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tRange, 1, 0); }
	@Test public void rangeInsertOneRecord() throws Throwable { insertOneRecord(tRange, 10); }
	@Test public void rangeUpdateOne() throws Throwable { updateOne(tRange, 1, "ThereOne"); }
	@Test public void rangeUpdateThree() throws Throwable { updateOne(tRange, 3, "ThereThree"); }
	@Test public void rangeUpdateAll() throws Throwable { updateRange(tRange, 1, 4, 4); }
	@Test public void rangeInsertAndDelete() throws Throwable { insertAndDelete(tRange, 11, "Hello"); }
	
	@Test
	public void addStorageGroup() throws PEException {
		PersistentGroup sg2Test = new PersistentGroup("sg2Test");
		sg2Test.addStorageSite(siteMap.get("site1"));
		sg2Test.addStorageSite(siteMap.get("site2"));
		StorageGroupAccessor.addGeneration(sg2Test);
		assertTrue(siteMap.containsKey("site3"));
		sg2Test.addStorageSite(siteMap.get("site3"));
		catalogDAO.begin();
		catalogDAO.persistToCatalog(sg2Test);
		catalogDAO.commit();
		catalogDAO.refresh(sg2Test);
		PersistentGroup sg2Ref = catalogDAO.findPersistentGroup("sg2Ref");
		assertTrue(StorageGroupAccessor.areEquivalent(sg2Test,sg2Ref));
	}

	@Test public void randGen2SelectAll() throws Throwable { selectAll(tRandGen2, 7); }
	@Test public void randGen2SelectOne() throws Throwable { selectOne(tRandGen2, 1, 1); }
	@Test public void randGen2SelectTwenty() throws Throwable { selectOne(tRandGen2, 6, 1); }
	@Test public void randGen2SelectTwentyOne() throws Throwable { selectOne(tRandGen2, 7, 1); }
	@Test public void randGen2SelectOneByKey() throws Throwable { selectByKey(tRandGen2, 1); }
	@Test public void randGen2SelectTwoByKey() throws Throwable { selectByKey(tRandGen2, 2); }
	@Test public void randGen2SelectThreeByKey() throws Throwable { selectByKey(tRandGen2, 3); }
	@Test public void randGen2InsertOneRecord() throws Throwable { insertOneRecord(tRandGen2, 10); }
	@Test public void randGen2UpdateOne() throws Throwable { updateOne(tRandGen2, 1, "ThereOne"); }
	@Test public void randGen2UpdateThree() throws Throwable { updateOne(tRandGen2, 3, "ThereThree"); }
	@Test public void randGen2UpdateAll() throws Throwable { updateRange(tRandGen2, 1, 4, 4); }
	@Test public void randGen2InsertandDelete() throws Throwable { insertAndDelete(tRandGen2, 11, "Hello"); }

	@Test public void bcastGen2SelectAllFromAll() throws Throwable { selectAllFromAll(tBcastGen2, 21); }
	@Test public void bcastGen2SelectAll() throws Throwable { selectAll(tBcastGen2, 7); }
	@Test public void bcastGen2SelectOne() throws Throwable { selectOne(tBcastGen2, 1, 1); }
	@Test public void bcastGen2SelectOneByKey() throws Throwable { selectByKey(tBcastGen2, 1); }
	@Test public void bcastGen2SelectTwoByKey() throws Throwable { selectByKey(tBcastGen2, 2); }
	@Test public void bcastGen2SelectThreeByKey() throws Throwable { selectByKey(tBcastGen2, 3); }
	@Test public void bcastGen2SelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tBcastGen2, 1, 1); }
	@Test public void bcastGen2InsertOneRecord() throws Throwable { insertOneRecord(tBcastGen2, 10); }
	@Test public void bcastGen2UpdateOne() throws Throwable { updateOne(tBcastGen2, 1, "ThereOne"); }
	@Test public void bcastGen2UpdateThree() throws Throwable { updateOne(tBcastGen2, 3, "ThereThree"); }
	@Test public void bcastGen2UpdateAll() throws Throwable { updateRange(tBcastGen2, 1, 4, 4); }
	@Test public void bcastGen2InsertAndDelete() throws Throwable { insertAndDelete(tBcastGen2, 11, "Hello"); }

	@Test public void staticGen2SelectAll() throws Throwable { selectAll(tStaticGen2, 2); }
	@Test public void staticGen2SelectOneByKey() throws Throwable { selectOne(tStaticGen2, 1, 0); }
	@Ignore @Test public void staticGen2SelectTwoByKey() throws Throwable { selectByKey(tStaticGen2, 6); }
	@Test public void staticGen2SelectThreeByKey() throws Throwable { selectByKey(tStaticGen2, 7); }
	@Test public void staticGen2SelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tStaticGen2, 1, 0); }
	@Test public void staticGen2InsertOneRecord() throws Throwable { insertOneRecord(tStaticGen2, 10); }
	@Ignore @Test public void staticGen2UpdateSix() throws Throwable { updateOne(tStaticGen2, 6, "ThereSix"); }
	@Test public void staticGen2UpdateAll() throws Throwable { updateRange(tStaticGen2, 6, 7, 2); }
	@Test public void staticGen2InsertAndDelete() throws Throwable { insertAndDelete(tStaticGen2, 11, "Hello"); }

	@Test public void rangeGen2SelectAll() throws Throwable { selectAll(tRangeGen2, 7); }
	@Test public void rangeGen2SelectTwoByKey() throws Throwable { selectByKey(tRangeGen2, 6); }
	@Test public void rangeGen2SelectThreeByKey() throws Throwable { selectByKey(tRangeGen2, 7); }
	@Test public void rangeGen2InsertOneRecord() throws Throwable { insertOneRecord(tRangeGen2, 10); }
	@Test public void rangeGen2UpdateSix() throws Throwable { updateOne(tRangeGen2, 6, "ThereSix"); }
	@Test public void rangeGen2UpdateAll() throws Throwable { updateRange(tRangeGen2, 6, 7, 2); }
	@Test public void rangeGen2InsertAndDelete() throws Throwable { insertAndDelete(tRangeGen2, 11, "Hello"); }

	@Test
	public void rangeGen2PreviousGen() throws Throwable {
		PersistentGroup sg = tRangeGen2.getPersistentGroup();
		StorageGroupGeneration gen = sg.getGenerations().get(0);
		DistributionRange dr = catalogDAO.findRangeForTable(tRangeGen2);

		KeyValue dvStart = tRangeGen2.getDistValue(catalogDAO);
		dvStart.get("id").setValue(1);
		KeyValue dvEnd= tRangeGen2.getDistValue(catalogDAO);
		dvEnd.get("id").setValue(5);

		catalogDAO.begin();
		GenerationKeyRange rangeGen = new GenerationKeyRange(dr, gen, dvStart, dvEnd);
		dr.addRangeGeneration(rangeGen);
		catalogDAO.commit();

		for (int i = 1; i < 8; ++i)
			selectByKey(tRangeGen2, i);
	}

	@Test public void randGen3SelectAll() throws Throwable { selectAll(tRandGen3, 7); }
	@Test public void randGen3SelectOne() throws Throwable { selectOne(tRandGen3, 1, 1); }
	@Test public void randGen3SelectTwenty() throws Throwable { selectOne(tRandGen3, 6, 1); }
	@Test public void randGen3SelectTwentyOne() throws Throwable { selectOne(tRandGen3, 7, 1); }
	@Test public void randGen3SelectOneByKey() throws Throwable { selectByKey(tRandGen3, 1); }
	@Test public void randGen3SelectTwoByKey() throws Throwable { selectByKey(tRandGen3, 2); }
	@Test public void randGen3SelectThreeByKey() throws Throwable { selectByKey(tRandGen3, 3); }
	@Test public void randGen3InsertOneRecord() throws Throwable { insertOneRecord(tRandGen3, 10); }
	@Test public void randGen3UpdateOne() throws Throwable { updateOne(tRandGen3, 1, "ThereOne"); }
	@Test public void randGen3UpdateThree() throws Throwable { updateOne(tRandGen3, 3, "ThereThree"); }
	@Test public void randGen3UpdateAll() throws Throwable { updateRange(tRandGen3, 1, 4, 4); }
	@Test public void randGen3InsertandDelete() throws Throwable { insertAndDelete(tRandGen3, 11, "Hello"); }

	@Test public void bcastGen3SelectAllFromAll() throws Throwable { selectAllFromAll(tBcastGen3, 21); }
	@Test public void bcastGen3SelectAll() throws Throwable { selectAll(tBcastGen3, 7); }
	@Test public void bcastGen3SelectOne() throws Throwable { selectOne(tBcastGen3, 1, 1); }
	@Test public void bcastGen3SelectOneByKey() throws Throwable { selectByKey(tBcastGen3, 1); }
	@Test public void bcastGen3SelectTwoByKey() throws Throwable { selectByKey(tBcastGen3, 2); }
	@Test public void bcastGen3SelectThreeByKey() throws Throwable { selectByKey(tBcastGen3, 3); }
	@Test public void bcastGen3SelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tBcastGen3, 1, 1); }
	@Test public void bcastGen3InsertOneRecord() throws Throwable { insertOneRecord(tBcastGen3, 10); }
	@Test public void bcastGen3UpdateOne() throws Throwable { updateOne(tBcastGen3, 1, "ThereOne"); }
	@Test public void bcastGen3UpdateThree() throws Throwable { updateOne(tBcastGen3, 3, "ThereThree"); }
	@Test public void bcastGen3UpdateAll() throws Throwable { updateRange(tBcastGen3, 1, 4, 4); }
	@Test public void bcastGen3InsertAndDelete() throws Throwable { insertAndDelete(tBcastGen3, 11, "Hello"); }

	@Test public void staticGen3SelectAll() throws Throwable { selectAll(tStaticGen3, 2); }
	@Test public void staticGen3SelectOneByKey() throws Throwable { selectByKey(tStaticGen3, 6); }
	@Ignore @Test public void staticGen3SelectTwoByKey() throws Throwable { selectByKey(tStaticGen3, 7); }
	@Test public void staticGen3SelectItemIdMismatch() throws Throwable { selectItemIdMismatch(tStaticGen3, 1, 0); }
	@Test public void staticGen3InsertOneRecord() throws Throwable { insertOneRecord(tStaticGen3, 10); }
	@Test public void staticGen3UpdateOne() throws Throwable { updateOne(tStaticGen3, 6, "ThereSix"); }
	@Ignore @Test public void staticGen3UpdateThree() throws Throwable { updateOne(tStaticGen3, 7, "ThereSeven"); }
	@Test public void staticGen3UpdateAll() throws Throwable { updateRange(tStaticGen3, 1, 9, 2); }
	@Test public void staticGen3InsertAndDelete() throws Throwable { insertAndDelete(tStaticGen3, 11, "Hello"); }

	@Test public void rangeGen3SelectAll() throws Throwable { selectAll(tRangeGen3, 7); }
	@Test public void rangeGen3SelectTwoByKey() throws Throwable { selectByKey(tRangeGen3, 6); }
	@Test public void rangeGen3SelectThreeByKey() throws Throwable { selectByKey(tRangeGen3, 7); }
	@Test public void rangeGen3InsertOneRecord() throws Throwable { insertOneRecord(tRangeGen3, 10); }
	@Test public void rangeGen3UpdateSix() throws Throwable { updateOne(tRangeGen3, 6, "ThereSix"); }
	@Test public void rangeGen3UpdateAll() throws Throwable { updateRange(tRangeGen3, 6, 7, 2); }
	@Test public void rangeGen3InsertAndDelete() throws Throwable { insertAndDelete(tRangeGen3, 11, "Hello"); }

	@Test public void randSelectAllOneSite() throws Throwable { selectAll(tRandOneSite, 3); }
	@Test public void randSelectOneOneSite() throws Throwable { selectOne(tRandOneSite, 1, 1); }
	@Test public void randSelectOneByKeyOneSite() throws Throwable { selectByKey(tRandOneSite, 1); }
	@Test public void randSelectTwoByKeyOneSite() throws Throwable { selectByKey(tRandOneSite, 2); }
	@Test public void randInsertOneRecordOneSite() throws Throwable { insertOneRecord(tRandOneSite, 10); }
	@Test public void randUpdateOneOneSite() throws Throwable { updateOne(tRandOneSite, 1, "ThereOne"); }
	@Test public void randUpdateThreeOneSite() throws Throwable { updateOne(tRandOneSite, 2, "ThereThree"); }
	@Test public void randUpdateAllOneSite() throws Throwable { updateRange(tRandOneSite, 1, 4, 2); }
	@Test public void randInsertAndDeleteOneSite() throws Throwable { insertAndDelete(tRandOneSite, 11, "Hello"); }

	@Test public void randSelectThreeByKeyTxnOneSite() throws Throwable { ssConnection.userBeginTransaction(); selectByKey(tRandOneSite, 2); ssConnection.userCommitTransaction();}
	@Test public void randInsertOneRecordTxnOneSite() throws Throwable { ssConnection.userBeginTransaction(); insertOneRecord(tRandOneSite, 12);  ssConnection.userCommitTransaction();}
	@Test public void randUpdateOneTxnOneSite() throws Throwable { ssConnection.userBeginTransaction(); updateOne(tRandOneSite, 1, "ThereOneT");  ssConnection.userCommitTransaction();}

	@Test public void bcastSelectAllFromAllOneSite() throws Throwable { selectAllFromAll(tBcastOneSite, 5); }
	@Test public void bcastSelectAllOneSite() throws Throwable { selectAll(tBcastOneSite, 5); }
	@Test public void bcastSelectOneOneSite() throws Throwable { selectOne(tBcastOneSite, 1, 1); }
	@Test public void bcastSelectOneByKeyOneSite() throws Throwable { selectByKey(tBcastOneSite, 1); }
	@Test public void bcastSelectTwoByKeyOneSite() throws Throwable { selectByKey(tBcastOneSite, 2); }
	@Test public void bcastSelectThreeByKeyOneSite() throws Throwable { selectByKey(tBcastOneSite, 3); }
	@Test public void bcastSelectItemIdMismatchOneSite() throws Throwable { selectItemIdMismatch(tBcastOneSite, 1, 1); }
	@Test public void bcastInsertOneRecordOneSite() throws Throwable { insertOneRecord(tBcastOneSite, 10); }
	@Test public void bcastUpdateOneOneSite() throws Throwable { updateOne(tBcastOneSite, 1, "ThereOne"); }
	@Test public void bcastUpdateThreeOneSite() throws Throwable { updateOne(tBcastOneSite, 3, "ThereThree"); }
	@Test public void bcastUpdateAllOneSite() throws Throwable { updateRange(tBcastOneSite, 1, 4, 4); }
	@Test public void bcastInsertAndDeleteOneSite() throws Throwable { insertAndDelete(tBcastOneSite, 11, "Hello"); }

	@Test public void staticSelectAllOneSite() throws Throwable { selectAll(tStaticOneSite, 2); }
	@Test public void staticSelectOneByKeyOneSite() throws Throwable { selectByKey(tStaticOneSite, 4); }
	@Test public void staticSelectTwoByKeyOneSite() throws Throwable { selectByKey(tStaticOneSite, 2); }
	@Test public void staticSelectThreeByKeyOneSite() throws Throwable { selectByKey(tStaticOneSite, 4); }
	@Test public void staticSelectItemIdMismatchOneSite() throws Throwable { selectItemIdMismatch(tStaticOneSite, 2, 0); }
	@Test public void staticInsertOneRecordOneSite() throws Throwable { insertOneRecord(tStaticOneSite, 10); }
	@Test public void staticUpdateOneOneSite() throws Throwable { updateOne(tStaticOneSite, 2, "ThereOne"); }
	@Test public void staticUpdateThreeOneSite() throws Throwable { updateOne(tStaticOneSite, 4, "ThereThree"); }
	@Test public void staticUpdateAllOneSite() throws Throwable { updateRange(tStaticOneSite, 1, 4, 2); }
	@Test public void staticInsertAndDeleteOneSite() throws Throwable { insertAndDelete(tStaticOneSite, 11, "Hello"); }

	@Test public void rangeSelectAllOneSite() throws Throwable { selectAll(tRangeOneSite, 2); }
	@Test public void rangeSelectOneByKeyOneSite() throws Throwable { selectByKey(tRangeOneSite, 2); }
	@Test public void rangeSelectTwoByKeyOneSite() throws Throwable { selectByKey(tRangeOneSite, 4); }
	@Test public void rangeSelectItemIdMismatchOneSite() throws Throwable { selectItemIdMismatch(tRangeOneSite, 2, 0); }
	@Test public void rangeInsertOneRecordOneSite() throws Throwable { insertOneRecord(tRangeOneSite, 10); }
	@Test public void rangeUpdateOneOneSite() throws Throwable { updateOne(tRangeOneSite, 2, "ThereOne"); }
	@Test public void rangeUpdateThreeOneSite() throws Throwable { updateOne(tRangeOneSite, 4, "ThereThree"); }
	@Test public void rangeUpdateAllOneSite() throws Throwable { updateRange(tRangeOneSite, 1, 4, 2); }
	@Test public void rangeInsertAndDeleteOneSite() throws Throwable { insertAndDelete(tRangeOneSite, 11, "Hello"); }

	@Test
	public void rangeGen3PreviousGen() throws Throwable {
		PersistentGroup sg = tRangeGen3.getPersistentGroup();
		StorageGroupGeneration gen = sg.getGenerations().get(0);
		DistributionRange dr = catalogDAO.findRangeForTable(tRangeGen3);

		KeyValue dvStart = tRangeGen3.getDistValue(catalogDAO);
		dvStart.get("id").setValue(1);
		KeyValue dvEnd= tRangeGen3.getDistValue(catalogDAO);
		dvEnd.get("id").setValue(5);

		catalogDAO.begin();
		GenerationKeyRange rangeGen = new GenerationKeyRange(dr, gen, dvStart, dvEnd);
		dr.addRangeGeneration(rangeGen);
		catalogDAO.commit();

		for (int i = 1; i < 8; ++i)
			selectByKey(tRangeGen3, i);
	}

	public void selectAll(UserTable t, int expectedCount) throws Throwable {
		QueryStepOperation op = new QueryStepSelectAllOperation(t.getDatabase(), t.getDistributionModel(),
				"select * from "+t.getNameAsIdentifier()+" where id < 10");
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		rc.printRows();
		assertEquals(expectedCount, rc.getUpdateCount());
		plan.close();
	}

	public void selectOne(UserTable t, int recId, int expectedCount)
			throws Throwable {
		QueryStepOperation op = new QueryStepSelectAllOperation(t.getDatabase(), t.getDistributionModel(),
				"select * from "+t.getNameAsIdentifier()+" where id = "+recId);
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
//		rc.printRows();
		assertEquals(expectedCount, rc.getUpdateCount());
		plan.close();
	}

	public void selectByKey(UserTable t, int recId) throws Throwable {
		KeyValue dv = t.getDistValue(catalogDAO);
		dv.get("id").setValue(recId);
		selectByKey(t, dv, recId);
	}

	public void selectByKey(UserTable t, KeyValue dv, int recId,
			int expectedCount) throws Throwable {
		QueryStepOperation op = new QueryStepSelectByKeyOperation(t.getDatabase(), dv,
				"select * from "+t.getNameAsIdentifier()+" where id = "+recId);
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		try {
			MysqlTextResultCollector rc = new MysqlTextResultCollector();
			plan.executeStep(ssConnection, rc);
			assertTrue(rc.hasResults());
//			rc.printRows();
			assertEquals(expectedCount, rc.getUpdateCount());
		} finally {
			plan.close();
		}
	}

	public void selectByKey(UserTable t, KeyValue dv, int recId)
			throws Throwable {
		selectByKey(t, dv, recId, 1);
	}

	public void selectItemIdMismatch(UserTable t, int recId, int expectedCount)
			throws Throwable {
		KeyValue dv = t.getDistValue(catalogDAO);
		dv.get("id").setValue(recId);
		selectByKey(t, dv, recId+1, expectedCount);
	}

	public void insertRecord(UserTable t, KeyValue distValue, int recId,
			String value) throws Throwable {
		QueryPlan plan;
		QueryStepOperation op = new QueryStepInsertByKeyOperation(t.getDatabase(), distValue, 
				"insert into "+t.getNameAsIdentifier()+" values ("+recId+", '"+value+"')");
		plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertFalse(rc.hasResults());
		assertEquals(1, rc.getUpdateCount());
		plan.close();
	}

	void selectWithDiscriminator(UserTable t, String col, String val,
			int count) throws Throwable {
		QueryStepOperation op = new QueryStepSelectAllOperation(t.getDatabase(), t.getDistributionModel(),
				"select * from "+t.getNameAsIdentifier()+" where "+col+" = "+val);
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		rc.printRows();
		assertEquals(count, rc.getUpdateCount());
		plan.close();
	}

	public void insertOneRecord(UserTable t, int recId) throws Throwable {
		KeyValue dv = t.getDistValue(catalogDAO);
		dv.get("id").setValue(recId);

		insertRecord(t, dv, recId, "Hello");
		selectByKey(t, dv, recId);
		selectWithDiscriminator(t, "id", Integer.toString(recId), 1);
	}

	void updateRecord(UserTable t, KeyValue dv, int recId, String value)
			throws Throwable {
		QueryStepOperation op = new QueryStepUpdateByKeyOperation(t.getDatabase(), dv, 
				new SQLCommand("update "+t.getNameAsIdentifier()+" set value = '"+value+"' where id = "+recId));
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		plan.executeStep(ssConnection, rc);
		assertFalse(rc.hasResults());
		assertEquals(1, rc.getNumRowsAffected());
		plan.close();
	}

	public void updateOne(UserTable t, int recId, String value)
			throws Throwable {
		KeyValue dv = t.getDistValue(catalogDAO);
		dv.get("id").setValue(recId);

		updateRecord(t, dv, recId, value);
		selectByKey(t, dv, recId);
		selectWithDiscriminator(t, "id", Integer.toString(recId), 1);
	}

	void updateRange(UserTable t, int lowVal, int highVal, int count,
			String value) throws Throwable {
		QueryStepOperation op = new QueryStepUpdateAllOperation(t.getDatabase(), t.getDistributionModel(), 
				"update "+t.getNameAsIdentifier()
				+ " set value = '"+value+"'"
				+ " where id >="+lowVal+" and id <="+highVal);
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertFalse(rc.hasResults());
		assertEquals(count, rc.getUpdateCount());
		plan.close();
	}

	protected void updateRange(UserTable t, int lowVal, int highVal,
			int expected) throws Throwable {
		updateRange(t, lowVal, highVal, expected, "AllUpdate");
		selectWithDiscriminator(t, "value", "'AllUpdate'", expected);
	}

	long getRowCount(UserTable t) throws Throwable {
		QueryStepOperation op = new QueryStepSelectAllOperation(t.getDatabase(), t.getDistributionModel(),
				"select * from "+t.getNameAsIdentifier());
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		rc.printRows();
		long rowCount = rc.getUpdateCount();
		plan.close();
		return rowCount;
	}

	void deleteRow(UserTable t, KeyValue dv, int recId) throws Throwable {
		QueryStepOperation op = new QueryStepUpdateByKeyOperation(t.getDatabase(), dv, 
				new SQLCommand("delete from "+t.getNameAsIdentifier()+" where id = "+recId));
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertFalse(rc.hasResults());
		assertEquals(1, rc.getUpdateCount());
		plan.close();
	}

	protected void insertAndDelete(UserTable t, int recId, String value)
			throws Throwable {
		KeyValue distValue = t.getDistValue(catalogDAO);
		distValue.get("id").setValue(recId);

		insertRecord(t, distValue, recId, value);
		selectByKey(t, distValue, recId);
		long startCount = getRowCount(t);
		deleteRow(t, distValue, recId);
		long endCount = getRowCount(t);
		assertEquals(startCount-1, endCount);
	}

	protected void selectAllFromAll(UserTable t, int expectedCount)
			throws Throwable {
		QueryStepOperation op = new QueryStepSelectAllOperation(t.getDatabase(), StaticDistributionModel.SINGLETON,
				"select * from "+t.getNameAsIdentifier()+" where id < 10");
		QueryPlan plan = new QueryPlan(t.getPersistentGroup(), op);
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		rc.printRows();
		assertEquals(expectedCount, rc.getUpdateCount());
		plan.close();
	}

}
