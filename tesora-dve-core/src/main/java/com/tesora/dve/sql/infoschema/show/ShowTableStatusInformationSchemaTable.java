package com.tesora.dve.sql.infoschema.show;

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


import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.logical.TableStatusInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.ddl.ShowTableStatusStatement;
import com.tesora.dve.sql.util.ListSet;

// we use the catalog to figure out where to send the actual show table status command
// then plan the actual as regular dml.
// nonmt only for now.
public class ShowTableStatusInformationSchemaTable extends ShowInformationSchemaTable {

	private TableStatusInformationSchemaTable tableStatusTable;
	private ShowInformationSchemaTable tableTable = null;
	@SuppressWarnings("unused")
	private InformationSchemaColumnView nameColumn = null;
	
	public ShowTableStatusInformationSchemaTable(TableStatusInformationSchemaTable basedOn) {
		super(basedOn, new UnqualifiedName("TABLE STATUS"), new UnqualifiedName("TABLE STATUS"), false, false);
		tableStatusTable = basedOn;
		nameColumn = passthroughView(tableStatusTable);
	}


	@Override
	protected void validate(SchemaView ofView) {
		tableTable = (ShowInformationSchemaTable) ofView.lookup("table");
		if (tableTable == null)
			throw new InformationSchemaException("Cannot find show table view in show view");
	}
	
	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName) {
		throw new SchemaException(Pass.SECOND, "Invalid show table status command");
	}
	
	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping, ExpressionNode likeExpr, ExpressionNode whereExpr, ShowOptions options) {
				
		if (sc == null)
			return new ShowTableStatusStatement(likeExpr,this);
		
		boolean tenant = sc.getPolicyContext().isSchemaTenant();

		if (whereExpr != null) {
			throw new SchemaException(Pass.SECOND,"Currently unsupported: show table status where ....");			
		}
		LiteralExpression litex = (LiteralExpression)likeExpr;
		String likeStr = (litex == null ? "%" : (String)litex.getValue(sc));
		// we have the like use either the tenant query or a regular user table query
		String query = null;
		HashMap<String,Object> params = new HashMap<String,Object>();
		params.put("likeExpr", likeStr);
		if (tenant) {
			query = "from TableVisibility tv where tv.ofTenant.id = :tenid and ((tv.localName like :likeExpr) or (tv.table.name like :likeExpr))";
			params.put("tenid",sc.getPolicyContext().getTenantID(true).intValue());
		} else {
			query = "from UserTable ut where ut.name like :likeExpr and ut.userDatabase.name = :dbname";
			if (scoping == null || scoping.isEmpty())
				params.put("dbname", sc.getCurrentDatabase(true).getName().getUnquotedName().get());
			else if (scoping.size() > 1)
				throw new SchemaException(Pass.SECOND,"Overly scoped show table status");
			else
				params.put("dbname",scoping.get(0).getUnquotedName().get());
		}
		List<CatalogEntity> ents = sc.getCatalog().query(query, params);
		ListSet<TableKey> tks = new ListSet<TableKey>();
		// only support a single persistent group for now; we can relax later when we want to do a lot more planning.
		PEPersistentGroup onGroup = null;
		for(CatalogEntity ce : ents) {
			if (tenant) {
				TableVisibility tv = (TableVisibility) ce;
				TableScope ts = TableScope.load(tv, sc);
				tks.add(TableKey.make(sc,ts,0));
				if (onGroup == null) onGroup = ts.getTable(sc).getPersistentStorage(sc);
				else if (!onGroup.equals(ts.getTable(sc).getPersistentStorage(sc)))
					throw new SchemaException(Pass.PLANNER, "Unable to execute show status across groups");
			} else {
				UserTable ut = (UserTable)ce;
				PEAbstractTable<?> pet = PEAbstractTable.load(ut, sc);
				tks.add(TableKey.make(sc,pet,0));
				if (onGroup == null) onGroup = pet.getPersistentStorage(sc);
				else if (!onGroup.equals(pet.getPersistentStorage(sc)))
					throw new SchemaException(Pass.PLANNER, "Unable to execute show status across groups");
			}
		}
		if(onGroup == null) {
			return new EmptyShowTableStatusStatement();
		}
		
		return new ShowTableStatusStatement(tks,whereExpr,onGroup,this);
	}	
	
	public static class EmptyShowTableStatusStatement extends SchemaQueryStatement {

		public EmptyShowTableStatusStatement() {
			super(false, "", buildTempResultSet());
		}

		public static IntermediateResultSet buildTempResultSet() {
			ColumnSet cs = new ColumnSet();
			cs.addColumn("Name", 64, "varchar", java.sql.Types.VARCHAR);
			cs.addColumn("Engine", 64, "varchar", java.sql.Types.VARCHAR);
			cs.addColumn("Version", 21, "bigint", Types.BIGINT);
			cs.addColumn("Row_format", 10, "varchar", java.sql.Types.VARCHAR);
			cs.addColumn("Rows", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Avg_row_length", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Data_length", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Max_data_length", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Index_length", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Data_free", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Auto_increment", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Create_time", 20, "datetime", Types.TIMESTAMP);
			cs.addColumn("Update_time", 20, "datetime", Types.TIMESTAMP);
			cs.addColumn("Check_time", 20, "datetime", Types.TIMESTAMP);
			cs.addColumn("Collation", 32, "varchar", java.sql.Types.VARCHAR);
			cs.addColumn("Checksum", 21, "bigint", java.sql.Types.BIGINT);
			cs.addColumn("Create_options", 255, "varchar", java.sql.Types.VARCHAR);
			cs.addColumn("Comment", 2048, "varchar", java.sql.Types.VARCHAR);

			List<ResultRow> rows = new ArrayList<ResultRow>();
			return new IntermediateResultSet(cs, rows);
		}

	}
}
