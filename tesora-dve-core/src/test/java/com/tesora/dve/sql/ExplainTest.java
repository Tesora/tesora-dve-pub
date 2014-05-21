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

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variable.SchemaVariableConstants;

public class ExplainTest extends SchemaTest {

	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",2,"checkg"),
				"schema");
	private static final ProjectDDL mtDDL =
		new PEDDL("mtdb",
				new StorageGroupDDL("mt",2,"mtg"),
				"database").withMTMode(MultitenantMode.ADAPTIVE);
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(checkDDL, mtDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource root = new ProxyConnectionResource();
		checkDDL.create(root);
		for(int i = 0; i < tabDefs.length; i++)
			root.execute(tabDefs[i]);
		mtDDL.create(root);
		for(int i = 0; i < tabDefs.length; i++)
			root.execute(tabDefs[i]);
		root.disconnect();
		root = null;
	}


	protected ProxyConnectionResource conn = null;

	private static final String tabDefs[] = new String[] {
		"create table `users` (`uid` int, `name` varchar(60), primary key (`uid`))",
		"create table `sessions` (`uid` int, `sid` varchar(128), `ssid` varchar(128), primary key (`sid`, `ssid`))",
		"insert into users (uid, name) values (1,'duk'),(2,'morgan'),(3,'petr'),(4,'peter')",
		"insert into sessions (uid, sid, ssid) values (1, 'foo', 'foofoo'),(2,'foo','foobar'),(3,'foo','baz')"
	};
	
	private static final String SESSION_QUERY = "select u.name, s.ssid from users u inner join sessions s on u.uid = s.uid where s.sid = 'foo' order by u.name";
	private static final String SESSION_QUERY_2 = "select max(s.sid),s.ssid from sessions s left outer join users u on u.uid = s.uid group by s.ssid order by s.ssid";
	
	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
	}
	
	@After
	public void disconnect() throws Throwable {
		conn.disconnect();
		conn = null;
	}
	
	@Test
	public void testSimple() throws Throwable {
		conn.execute("use " + checkDDL.getDatabaseName());
		conn.execute("alter dve set " + SchemaVariableConstants.STEPWISE_STATISTICS_NAME + " = true");
		conn.assertResults(SESSION_QUERY,
				br(nr,"duk","foofoo",
				   nr,"morgan","foobar",
				   nr,"petr","baz"));
//		System.out.println(conn.printResults("explain " + SESSION_QUERY));
		// explain reasons are slightly messed up right now
		conn.assertResults("explain " + SESSION_QUERY,
				br(nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute on su0_4","[su0_4]",ignore,getIgnore(),
				   nr,"REDISTRIBUTE","checkg",getIgnore(),"Broadcast distribute","[t3s1]",ignore,/*DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE.getDescription(),*/getIgnore(),
				   nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute on uu0_7","[uu0_7]",ignore,/*DMLExplainReason.LOOKUP_JOIN.getDescription(),*/getIgnore(),
				   nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute","",ignore,/*DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT.getDescription(),*/getIgnore(),
				   nr,"SELECT",null,null,"",null,ignore,/*DMLExplainReason.IN_MEMORY_LIMIT_NOT_APPLICABLE.getDescription(),*/getIgnore()));
		conn.execute(SESSION_QUERY);
//		System.out.println(conn.printResults("explain statistics=true " + SESSION_QUERY));
		conn.assertResults("explain statistics=true " + SESSION_QUERY, 
				br(nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute on su0_4","[su0_4]",ignore,2,4L,getIgnore(),getIgnore(),
				   nr,"REDISTRIBUTE","checkg",getIgnore(),"Broadcast distribute","[t3s1]",ignore,/*DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE.getDescription(),*/2,4L,getIgnore(),getIgnore(),
				   nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute on uu0_7","[uu0_7]",ignore,/*DMLExplainReason.LOOKUP_JOIN.getDescription(),*/2,4L,getIgnore(),getIgnore(),
				   nr,"REDISTRIBUTE",getIgnore(),getIgnore(),"Static distribute","",ignore,/*DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT.getDescription(),*/2,4L,getIgnore(),getIgnore(),
				   nr,"SELECT",null,null,"",null,ignore,/*DMLExplainReason.IN_MEMORY_LIMIT_NOT_APPLICABLE.getDescription(),*/2,6L,getIgnore(),getIgnore()));
//		System.out.println(conn.printResults("explain noplan=true " + SESSION_QUERY));
		conn.assertResults("explain noplan=true " + SESSION_QUERY,
				br(nr,"SELECT",null,null,"",null,DMLExplainReason.EXPLAIN_NOPLAN.getDescription(),
						"SELECT u.`name` AS un1,s.`ssid` AS ss2 FROM `users` AS u INNER JOIN `sessions` AS s ON u.`uid` = s.`uid` WHERE s.`sid` = 'foo' ORDER BY un1 ASC"));
		conn.assertResults(SESSION_QUERY_2,
				br(nr,"foo","baz",
				   nr,"foo","foobar",
				   nr,"foo","foofoo"));
		ResourceResponse rr = conn.fetch("show plan cache");
		assertEquals("should have 10 rows",10,rr.getResults().size());
		rr = conn.fetch("show plan cache statistics");
		assertEquals("should have 10 rows",10,rr.getResults().size());
	}

	@Test
	public void testMT() throws Throwable {
		conn.execute("use " + mtDDL.getDatabaseName());
		conn.assertResults("explain noplan=true " + SESSION_QUERY,
				br(nr,"SELECT",null,null,"",null,DMLExplainReason.EXPLAIN_NOPLAN.getDescription(),
						"SELECT u.name AS un1,s.ssid AS ss2 FROM `_1users0` AS u INNER JOIN `_1sessions0` AS s ON u.`___mtid` = s.`___mtid` AND u.uid = s.uid WHERE  (s.sid = 'foo')  AND u.`___mtid` = 1 ORDER BY un1 ASC"));
	}
	
}
