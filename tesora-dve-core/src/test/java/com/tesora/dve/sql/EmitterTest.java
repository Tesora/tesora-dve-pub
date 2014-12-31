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

import java.util.Collections;

import com.tesora.dve.db.DBNative;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

// make sure we emit proper sql for various scenarios
public class EmitterTest extends SchemaTest {

	private static final StorageGroupDDL sg = new StorageGroupDDL("check",1,"checkg");
	
	private static final ProjectDDL checkDDL =
		new PEDDL("adb",sg,"database");
	private static final ProjectDDL oDDL =
		new PEDDL("odb",sg,"database");
	static final NativeDDL nativeDDL =
			new NativeDDL("adb");

	// some schema.  none of these are populated
	private static final String[] schema = new String[]{
		"create table A (aid int, afid int, asid int, primary key (aid))",
		"create table B (bid int, bfid int, bsid int, primary key (bid))",
		"create table C (cid int, cfid int, csid int, primary key (cid))"
	};


	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(nativeDDL,checkDDL,oDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		oDDL.create(pcr);
		checkDDL.create(pcr);
		DBHelperConnectionResource dbh = new DBHelperConnectionResource();
		nativeDDL.create(dbh);
		for(String s : schema) {
			pcr.execute(s + " broadcast distribute");
			dbh.execute(s);
		}
		dbh.close();
		pcr.close();
	}

	protected DBHelperConnectionResource dbh;
	protected ProxyConnectionResource pcr;
	
	@Before
	public void connect() throws Throwable {
		dbh = new DBHelperConnectionResource();
		pcr = new ProxyConnectionResource();
		dbh.execute("use " + nativeDDL.getDatabaseName());
		pcr.execute("use " + checkDDL.getDatabaseName());
	}

	@After
	public void disconnect() throws Throwable {
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
		if (pcr != null)
			pcr.close();
		pcr = null;
	}
	
	@Test
	public void testGenAddDelete() throws Throwable {
		SchemaContext sc = pcr.getBackingConnection().getSchemaContext();
		// it's important that we don't go in through the parser for this - we're testing construction, not representation
		DeleteStatement ds = buildDeleteWithJoin(sc);
		testOptions(ds,EmitOptions.NONE.addQualifiedTables());
	}
	
	private void testOptions(Statement stmt, EmitOptions opts) throws Throwable {
		pcr.execute("use " + oDDL.getDatabaseName());
		SchemaContext sc = pcr.getBackingConnection().getSchemaContext();
		GenericSQLCommand gsql = stmt.getGenericSQL(sc, Singletons.require(DBNative.class).getEmitter(), opts);
		SQLCommand sqlc = gsql.resolve(sc.getValues(), true, null).getSQLCommand();
		String sql = sqlc.getSQL();
		dbh.execute(sql);
	}
	
	private DeleteStatement buildDeleteWithJoin(SchemaContext sc) throws Throwable {
		PETable A = findTable(sc,"A");
		PETable B = findTable(sc,"B");
		TableInstance ati = new TableInstance(A,A.getName().getUnqualified(), new UnqualifiedName("a"), false);
		TableInstance bti = new TableInstance(B,B.getName().getUnquotedName(), new UnqualifiedName("b"), false);
		ExpressionNode eqj = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(A.lookup(sc, "aid"),ati),
				new ColumnInstance(B.lookup(sc, "bid"),bti));
		FromTableReference ftr = new FromTableReference(ati);
		JoinedTable join = new JoinedTable(bti,eqj,JoinSpecification.INNER_JOIN);
		ftr.addJoinedTable(join);
		DeleteStatement ds = new DeleteStatement(Collections.singletonList(ati),
				Collections.singletonList(ftr), null, Collections.EMPTY_LIST, null, false,
				new AliasInformation(), null);
		return ds;
	}
	
	private PETable findTable(SchemaContext sc, String name) {
		return sc.findTable(PETable.getTableKey(sc.getCurrentPEDatabase(), new UnqualifiedName(name))).asTable();
	}
	
	
}
