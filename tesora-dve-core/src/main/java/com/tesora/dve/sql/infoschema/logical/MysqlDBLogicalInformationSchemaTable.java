package com.tesora.dve.sql.infoschema.logical;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class MysqlDBLogicalInformationSchemaTable extends
		LogicalInformationSchemaTable {

	protected LogicalInformationSchemaTable privTable;
	protected LogicalInformationSchemaTable tenantTable;
	protected LogicalInformationSchemaTable databaseTable;
	protected LogicalInformationSchemaTable userTable;
	
	protected LogicalInformationSchemaColumn dbColumn;
	protected DelegatingInformationSchemaColumn hostColumn;
	protected DelegatingInformationSchemaColumn userColumn;
	
	protected LogicalInformationSchemaColumn dbTableNameColumn;
	protected LogicalInformationSchemaColumn privTableDBColumn;
	protected LogicalInformationSchemaColumn tenantTableNameColumn;
	protected LogicalInformationSchemaColumn privTableTenantColumn;
	
	public MysqlDBLogicalInformationSchemaTable(LogicalInformationSchema logical) {
		super(new UnqualifiedName("dbview"));
		privTable = logical.lookup("priviledge");
		userTable =	logical.lookup("user");
		tenantTable = logical.lookup("tenant");
		databaseTable = logical.lookup("database");
	
		LogicalInformationSchemaColumn privUser = privTable.lookup("user");

		dbTableNameColumn = databaseTable.lookup("name");
		privTableDBColumn = privTable.lookup("database");
		tenantTableNameColumn = tenantTable.lookup("name");
		privTableTenantColumn = privTable.lookup("tenant");
		
		// db column is completely synthetic
		dbColumn = new LogicalInformationSchemaColumn(new UnqualifiedName("db"), dbTableNameColumn.getType());
		hostColumn = 
			new DelegatingInformationSchemaColumn(Arrays.asList(new LogicalInformationSchemaColumn[] { privUser, userTable.lookup("access") }),new UnqualifiedName("host"));
		userColumn = 
			new DelegatingInformationSchemaColumn(Arrays.asList(new LogicalInformationSchemaColumn[] { privUser, userTable.lookup("name")}),new UnqualifiedName("user"));
		
		addColumn(null,dbColumn);
		addColumn(null,hostColumn);
		addColumn(null,userColumn);
	}

	@Override
	public boolean requiresRawExecution() {
		return true;
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}
	
	@Override
	public LogicalQuery explode(SchemaContext sc, LogicalQuery lq) {
		SelectStatement in = lq.getQuery();
		TableInstance myref = null;
		TableInstance privRef = null;
		for(Iterator<FromTableReference> iter = in.getTablesEdge().iterator(); iter.hasNext();) {
			FromTableReference ftr = iter.next();
			if (ftr.getBaseTable() != null && ftr.getBaseTable().getTable() == this) {
				myref = ftr.getBaseTable();
				privRef = new TableInstance(privTable,null,in.getAliases().buildNewAlias(new UnqualifiedName("pt")),sc.getNextTable(),false);
				myref.getParentEdge().set(privRef);
				in.getDerivedInfo().addLocalTable(privRef.getTableKey());
				break;
			}
		}
		HashMap<LogicalInformationSchemaTable, TableInstance> forwarding = new HashMap<LogicalInformationSchemaTable,TableInstance>();
		forwarding.put(privTable,privRef);
		ColumnReplacementTraversal.replace(in,forwarding);
		
		ScopedColumnInstance dbname = new ScopedColumnInstance(dbTableNameColumn,new ColumnInstance(privTableDBColumn,privRef));
		ScopedColumnInstance tenantName = new ScopedColumnInstance(tenantTableNameColumn,new ColumnInstance(privTableTenantColumn,privRef));
		FunctionCall coalesce = new FunctionCall(FunctionName.makeCoalesce(),tenantName,dbname);
		LogicalInformationSchemaTable.ComplexReplacementTraversal.replace(in,dbColumn,coalesce);

		// we modified the tables without making a copy, clear the engine block
		in.getBlock().clear();

		for(Iterator<TableKey> iter = in.getDerivedInfo().getLocalTableKeys().iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (tk.getTable() == this)
				iter.remove();
		}
		
		return new LogicalQuery(lq,in);
	}
	
}
