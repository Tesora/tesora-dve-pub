package com.tesora.dve.upgrade.versions;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.util.Pair;

public abstract class ComplexCatalogVersion extends BasicCatalogVersion {

	public ComplexCatalogVersion(int version, boolean infoSchemaUpgrade) {
		super(version, infoSchemaUpgrade);
	}

	protected Pair<Long,Long> getGeneralBounds(DBHelper helper, String sql, String tabName) throws PEException {
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery(sql);
				rs = helper.getResultSet();
				rs.next();
				return new Pair<Long,Long>(rs.getLong(1), rs.getLong(2));
			} finally {
				if (rs != null) 
					rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to determine bounds on " + tabName + " table",sqle);
		}		
	}
	
	protected Pair<Long, Long> getSimpleBounds(DBHelper helper, String tableName, String idcol) throws PEException {
		String sql = String.format("select min(%s), max(%s) from %s", idcol, idcol, tableName);
		return getGeneralBounds(helper, sql, tableName);
	}
	
	protected void execQuery(DBHelper helper, String sql) throws PEException {
		try {
			helper.executeQuery(sql);
		} catch (SQLException sqle) {
			throw new PEException("Unable to execute '" + sql + "'", sqle);
		}
	}

	protected void execQuery(DBHelper helper, String[] sql) throws PEException {
		for(String s : sql)
			execQuery(helper,s);
	}
	
	protected TransientExecutionEngine buildExecutionEngine() throws PEException {
		try {
			TransientExecutionEngine t = new TransientExecutionEngine("peanalyzer");
            String[] decls = new String[] {
					"create persistent site site1 'jdbc:mysql://s1/db1'", "create persistent site site2 'jdbc:mysql://s2/db2'",
					"create persistent group g1 add site1, site2",
					"create project `" + Singletons.require(HostService.class).getDefaultProjectName() + "` default persistent group g1",
					"create database updb default persistent group g1",
					"use updb",
					"set foreign_key_checks=0"};
			
			t.parse(decls);
			return t;
		} catch (Throwable t) {
			throw new PEException("Unable to build trans exec engine for parser",t);
		}
	}

	@SuppressWarnings("unchecked")
	protected Pair<SchemaContext,PETable> buildTable(DBHelper helper, long tableid) throws PEException {
		String cts = (String)getSingleField(helper, "select create_table_stmt from user_table where table_id = " + tableid);
		if (cts == null)
			return null;
		TransientExecutionEngine tee = buildExecutionEngine();
		SchemaContext db = tee.getPersistenceContext();
		db.forceMutableSource();
		List<Statement> parsed = InvokeParser.parse(cts, db, Collections.EMPTY_LIST);
		if (parsed.size() != 1) return null; 
		Statement first = parsed.get(0);
		if (first instanceof PECreateStatement) {
			PECreateStatement<?,?> pecs = (PECreateStatement<?, ?>) first;
			if (pecs.getCreated() instanceof PETable) {
				PETable def = (PETable) pecs.getCreated();
				return new Pair<SchemaContext,PETable>(db,def);
			}
		}
		return null;

	}
	
	protected Object getSingleField(DBHelper helper, String sql) throws PEException {
		Object out = null;
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery(sql);
				rs = helper.getResultSet();
				if (rs.next())
					out = rs.getObject(1);
			} finally {
				if (rs != null)
					rs.close();
			}
		} catch (SQLException sqle) {
				throw new PEException("Unable to execute '" + sql + "'",sqle);
		}
		return out;
	}

}
