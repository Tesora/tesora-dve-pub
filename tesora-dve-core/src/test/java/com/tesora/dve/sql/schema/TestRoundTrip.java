// OS_STATUS: public
package com.tesora.dve.sql.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.standalone.PETest;

// parse some decls, persist them to the db, and make sure we can get them back out again

public class TestRoundTrip extends PETest {
	
	private static final boolean noisy = Boolean.valueOf(System.getProperty("parser.debug")).booleanValue();
	
	public static void echo(String what) {
		if (noisy)
			System.out.println(what);
	}
	
	@BeforeClass
	public static void setup() throws Throwable {
		TestCatalogHelper.createTestCatalog(PETest.class,4);
		bootHost = BootstrapHost.startServices(PETest.class);
		SchemaTest.setTemplateModeOptional();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void checkLoad(CatalogDAO catalog, Persistable obj, String defaultDatabase) throws Exception {
		// this is all about loading, so force the global cache to be cleaned out
		SchemaSourceFactory.reset();
		SchemaContext pc = SchemaContext.createContext(catalog);
		if (defaultDatabase != null)
			pc.setCurrentDatabase(pc.findDatabase(defaultDatabase));
		Persistable loaded = obj.reload(pc);
		String diffs = obj.differs(pc,loaded, false);
		if (diffs != null)
			fail(diffs);
	}
	
	private void persistToCatalog(CatalogDAO catalog, SchemaContext sc, PECreateStatement<?, ?> pecs) throws Exception {
		List<CatalogEntity> catalogObjects = pecs.createCatalogObjects(sc);
		for(CatalogEntity ce : catalogObjects) {
			catalog.persistToCatalog(ce);
		}
	}
	
	/**
	 * @param pc
	 * @param catalog
	 * @param sql
	 * @param defaultDatabase
	 * @return
	 * @throws Exception
	 */
	private PECreateStatement<?,?> trip(SchemaContext pc, CatalogDAO catalog, String sql, String defaultDatabase) throws Exception {
		PECreateStatement<?,?> pecs = (PECreateStatement<?,?>)InvokeParser.parse(sql,pc).get(0);
		return pecs;
	}
	
	// you'll have to set the def db, project - the session context would do that for us -
	// i.e. this test does not handle use statements correctly
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Pair<SchemaContext, Persistable<?,?>> roundTrip(String sql, String defaultDatabase) throws Exception {
		PECreateStatement firstTime = null;
		PECreateStatement secondTime = null;
		CatalogDAO catalog = CatalogDAOFactory.newInstance();
		SchemaContext secondPC = null;
		try {
			catalog.begin();
			SchemaContext firstPC = SchemaContext.createContext(catalog);
			if (defaultDatabase != null)
				firstPC.setCurrentDatabase(firstPC.findDatabase(defaultDatabase));
			firstTime = trip(firstPC,catalog, sql, defaultDatabase);
			// don't persist yet
			String gen = firstTime.getSQL(firstPC,true, false);
			echo(gen);
			secondPC = SchemaContext.createContext(catalog);
			if (defaultDatabase != null)
				secondPC.setCurrentDatabase(secondPC.findDatabase(defaultDatabase));
			secondTime = trip(secondPC, catalog, gen, defaultDatabase);
			// make sure the objects are the same 
			String diffs = firstTime.getCreated().differs(firstPC,secondTime.getCreated(), false);
			if (diffs != null)
				fail(diffs);
			persistToCatalog(catalog, secondPC, secondTime);
			catalog.commit();
			// now, we're going to load that persistable object and compare it to the second time
			catalog.begin();
			checkLoad(catalog, secondTime.getCreated(), defaultDatabase);
			catalog.rollbackNoException();
		} finally {
			catalog.close();
		}
		return new Pair<SchemaContext, Persistable<?,?>>(secondPC, secondTime.getCreated());
	}

	@Test
	public void simpleTest() throws Exception {
		roundTrip("create persistent site s1 url='jdbc:mysql://s1/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent site s2 url='jdbc:mysql://s2/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent group sg1 add s1, s2", null);
		roundTrip("create persistent site t1 url='jdbc:mysql://t1/db' user='floyd' password='woof'",null);
		roundTrip("create persistent site t2 url='jdbc:mysql://t1/db' user='floyd' password='woof'",null);
		roundTrip("create persistent group tg1 add t1, t2", null);
		roundTrip("create database mydb default persistent group sg1", null);
		// have to set up the template
		roundTrip(new TemplateBuilder("mytt").toCreateStatement(),null);
		roundTrip("create database ttdb default persistent group sg1 using template mytt strict", null);
		// use database mydb
		roundTrip("CREATE TABLE foo (`id` tinyint, `waa` char(20)) static distribute on (`id`)", "mydb");
	}

	@Test
	public void createTableDVAlts() throws Exception {
		roundTrip("create persistent site s3 url='jdbc:mysql://s3/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent site s4 url='jdbc:mysql://s4/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent group sg2 add s3, s4", null);
		roundTrip("create database mydb2 default persistent group sg2", null);
		// use database mydb
		roundTrip("create range field_range (varchar,int) persistent group sg2", "mydb2");
		roundTrip("create range openrange (int) persistent group sg2", "mydb2");
		Pair<SchemaContext, Persistable<?,?>> results = roundTrip("CREATE TABLE A (`id` tinyint)", "mydb2");
		PETable tab = (PETable)results.getSecond();
		// the default distribution is random, make sure we set that
		assertEquals(tab.getDistributionVector(results.getFirst()).getModel(), DistributionVector.Model.RANDOM);
		roundTrip("CREATE TABLE B (`id` tinyint) broadcast distribute", "mydb2");
		roundTrip("CREATE TABLE C (`id` tinyint) random distribute", "mydb2");
		roundTrip("CREATE TABLE D (`id` tinyint) static distribute on (`id`)", "mydb2");
		roundTrip("CREATE TABLE E (`id` int) range distribute on (`id`) using openrange", "mydb2");
		roundTrip("CREATE TABLE F (`id` int) range distribute on (`id`) using openrange", "mydb2");
	}	
	
	@Test
	public void testTypes() throws Exception {
		roundTrip("create persistent site s5 url='jdbc:mysql://s5/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent group sg3 add s5", null);
		roundTrip("create database mydb3 default persistent group sg3", null);
        NativeTypeCatalog tc = Singletons.require(HostService.class).getDBNative().getTypeCatalog();
		StringBuilder buf = new StringBuilder();
		buf.append("CREATE TABLE typeRoundTrip ( ").append(PEConstants.LINE_SEPARATOR);
		int counter = 0;
		for (Iterator<Map.Entry<String, NativeType>> iter = tc.getTypeCatalogEntries().iterator(); iter
				.hasNext();) {
			Map.Entry<String, NativeType> me = iter.next();
			NativeType nt = me.getValue();
			if (nt.isUsedInCreate()) {
				if (counter > 0)
					buf.append(", ").append(PEConstants.LINE_SEPARATOR);
				buf.append("`f").append(++counter).append("` ").append(nt.getTypeName());
				if (nt.isUnsignedAttribute())
					buf.append(", `f").append(++counter).append("` ").append(nt.getTypeName())
							.append(" ").append(MysqlNativeType.MODIFIER_UNSIGNED);
			}
		}
		buf.append(") ");
		roundTrip(buf.toString(), "mydb3");
	}

	@Test
	public void testKeys() throws Exception {
		roundTrip("create persistent site s6 url='jdbc:mysql://s3/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent site s7 url='jdbc:mysql://s4/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent group sg4 add s6, s7", null);
		roundTrip("create database mydb4 default persistent group sg4", null);
		roundTrip("create table keytest (`id` int not null, `fid` int not null, "
				+ "`sid` int not null, `stuff` varchar(32), "
				+ "primary key (`id`), key (`id`, `fid`), "
				+ "unique key (`sid`), index (`stuff`), fulltext index (`stuff`)) engine = myisam broadcast distribute","mydb4");
		roundTrip("create table fktest (`id` int not null, `fid` int, primary key (`id`), foreign key (fid) references keytest (sid) ON DELETE SET NULL ON UPDATE CASCADE) broadcast distribute","mydb4");
	}
	
	@Test
	public void testContainers() throws Exception {
		roundTrip("create persistent site s8 url='jdbc:/mysql://s5/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent site s9 url='jdbc:mysql://s4/db1' user='floyd' password='woof'", null);
		roundTrip("create persistent group sg5 add s8, s9", null);
		roundTrip("create database mydb5 default persistent group sg5", null);
		roundTrip("create range cont_range (int) persistent group sg5", "mydb5");
		roundTrip("create container cont1 persistent group sg5 range distribute using cont_range","mydb5");
		roundTrip("create table basetable (`id` int not null, primary key (`id`)) discriminate on (id) using container cont1","mydb5");
		roundTrip("create table nonbasetable (`id` int not null) container distribute cont1","mydb5");	
	}
	
}
