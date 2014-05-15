// OS_STATUS: public
package com.tesora.dve.test.security;

import java.sql.SQLException;
import java.util.Properties;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;

public class SiteSecurityTest extends PETest {

	static Properties props;

	@BeforeClass
	public static void setup() throws Exception {
		Class<?> bootClass = PETest.class;

		TestCatalogHelper.createTestCatalog(bootClass, 2, "root2", "password2");

		bootHost = BootstrapHost.startServices(bootClass);

        populateMetadata(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
        populateSites(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());

		props = PEFileUtils.loadPropertiesFile(SiteSecurityTest.class, PEConstants.CONFIG_FILE_NAME);

		DBHelper dbHelper = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "root2", "password2");

		try {
			dbHelper.connect();
			dbHelper.executeQuery("USE TestDB");
			dbHelper.executeQuery("CREATE TABLE table1 ( col1 int, col2 varchar(10))");
			for (int i = 0; i < 100; i++) {
				dbHelper.executeQuery("INSERT INTO table1 VALUES (" + i + " , 'val" + i + "')");
			}

			// Create a new user as 'root2'
			dbHelper.executeQuery("CREATE USER 'test1'@'localhost' IDENTIFIED BY 'test1'");
			dbHelper.executeQuery("GRANT ALL ON *.* to 'test1'@'localhost'");
		} finally {
			dbHelper.disconnect();
		}
	}

	@Test
	public void connectTest() throws PEException, SQLException {
		// Attempt to connect as root - should fail
		PEUrl myURL = PEUrl.fromUrlString(props.getProperty(PEConstants.PROP_JDBC_URL));
		final ExpectedExceptionTester exceptionTester = new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				DBHelper dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL),
						props.getProperty(PEConstants.PROP_JDBC_USER),
						props.getProperty(PEConstants.PROP_JDBC_PASSWORD));

				try {
					dbh.connect();
				} finally {
					dbh.disconnect();
				}
			}
		};
		exceptionTester
				.assertException(PEException.class,
						String.format("Error connecting to database 'jdbc:mysql://localhost:%d' - PEException: Connection refused - User 'root' not found",
								myURL.getPort()));

		// Attempt to connect as root2 - should work
		DBHelper dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "root2", "password2");
		try {
			dbh.connect();
		} finally {
			dbh.disconnect();
		}
		
		// Attempt to connect as test1 - should work
		dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "test1", "test1");
		try {
			dbh.connect();
		} finally {
			dbh.disconnect();
		}


	}

	@Test
	public void selectTest() throws PEException, SQLException {
		// We should be able to read the data in table1 as root2 or as test1

		DBHelper dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "root2", "password2");
		try {
			dbh.connect();
			dbh.executeQuery("USE TestDB");
			dbh.executeQuery("SELECT * from table1");
		} finally {
			dbh.disconnect();
		}
		
		dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "test1", "test1");
		try {
			dbh.connect();
			dbh.executeQuery("USE TestDB");
			dbh.executeQuery("SELECT * from table1");
		} finally {
			dbh.disconnect();
		}
	}
	
	
	@Test
	public void redistTest() throws PEException, SQLException {
		// We should be able to read the data in table1 as root2 or as test1

		DBHelper dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "root2", "password2");
		try {
			dbh.connect();
			dbh.executeQuery("USE TestDB");
			dbh.executeQuery("SELECT * from table1 ORDER BY col2 DESC");
		} finally {
			dbh.disconnect();
		}
		
		dbh = new DBHelper(props.getProperty(PEConstants.PROP_JDBC_URL), "test1", "test1");
		try {
			dbh.connect();
			dbh.executeQuery("USE TestDB");
			dbh.executeQuery("SELECT * from table1 ORDER BY col2 DESC");
		} finally {
			dbh.disconnect();
		}
	}
}
