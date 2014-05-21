// OS_STATUS: public
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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class AlterDistributionTest extends SchemaTest {

	private static final int srcTablePower = 5;
	
	private static StorageGroupDDL sg = new StorageGroupDDL("sys",5, 2,"sysg");
	 
	private static final ProjectDDL sysDDL =
			new PEDDL("adt",sg,
					"database").withFKMode(FKMode.IGNORE);
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
		for(int i = 0; i < rangeDecls.length; i++) {
			pcr.execute(rangeDecls[i]);
		}

		pcr.execute("create table vsrc" + body + decls[0]);
		pcr.execute("insert into vsrc (fid,sid) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)");
		for(int i = 0; i < srcTablePower; i++) {
			pcr.execute("insert into vsrc (fid,sid) select id as fid, id as sid from vsrc");
		}
		pcr.disconnect();
	}

	private ProxyConnectionResource conn;
	private DBHelperConnectionResource dbh;
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		conn.execute("use " + sysDDL.getDatabaseName());
		dbh = new DBHelperConnectionResource();
	}
	
	@After
	public void after() throws Throwable {
		conn.disconnect();
		dbh.disconnect();
		conn = null;
		dbh = null;		
	}
	
	private static final String[] tabNames = new String[] { "BC", "AR", "RA", "ST", "DRA" };
	private static final String[] decls = new String[] {
		"broadcast distribute",
		"random distribute",
		"range distribute on (id) using uni_range",
		"static distribute on (id)",
		"range distribute on (id,fid) using duo_range"
	};
	private static final Object[] rr = new Object[] {
		br(nr,null,null,"Broadcast",null),
		br(nr,null,null,"Random",null),
		br(nr,"id",1,"Range","uni_range"),
		br(nr,"id",1,"Static",null),
		br(nr,"id",1,"Range","duo_range",nr,"fid",2,"Range","duo_range")
	};
	private static final String[] rangeDecls = new String[] {
		"create range uni_range (int) persistent group " + sg.getName(),
		"create range duo_range (int,int) persistent group " + sg.getName()
	};
	
	private static final String body = 
			" (id int auto_increment, fid int, sid int, primary key(id)) ";

	
	
	private void declareAndPopulate(int which) throws Throwable {
		conn.execute("create table " + tabNames[which] + body + decls[which]);
		conn.execute("insert into " + tabNames[which] + " (id, fid, sid) select * from vsrc");
	}
	
	@Test
	public void testAlterDistVect() throws Throwable {
		for(int i = 0; i < tabNames.length; i++) {
			for(int j = 0; j < tabNames.length; j++) {
				if (j == i) continue;
				conn.execute("drop table if exists " + tabNames[i]);
				declareAndPopulate(i);
				String infoSQL = "select column_name, vector_position, model_type, model_name "
						+"from information_schema.distributions "
						+"where database_name = '" + sysDDL.getDatabaseName() + "' and table_name = '" + tabNames[i] + "'";
				conn.assertResults(infoSQL,(Object[]) rr[i]);
				String alterSQL = "alter table " + tabNames[i] + " " + decls[j];
				conn.execute(alterSQL);
//				System.out.println(alterSQL);
//				System.out.println(conn.printResults(infoSQL));
				conn.assertResults(infoSQL,(Object[]) rr[j]);
				// make sure further inserts do not fail (i.e. the autoinc was done correctly)
				conn.execute("insert into " + tabNames[i] + " (fid,sid) values (-1,-1)");
				conn.assertResults("select count(*) from " + tabNames[i],br(nr,10 * Math.round(Math.pow(2, srcTablePower)) + 1));
			}
			
		}
	}
	
}
