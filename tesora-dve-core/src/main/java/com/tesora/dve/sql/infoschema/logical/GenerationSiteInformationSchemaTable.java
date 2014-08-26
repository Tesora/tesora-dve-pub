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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;

// this table is backed by the following query:
// select pg.name as `group_name`
//        sg.version as `version`
//        ss.name as `site_name`
// from generation_sites gs,
//      storage_site ss,
//      storage_generation sg,
//      persistent group pg
// where sg.persistent_group_id = pg.persistent_group_id
//   and gs.site_id = ss.id
//   and gs.generation_id = sg.generation_id
//
// the default ordering is: pg.name, sg.version, ss.name
public class GenerationSiteInformationSchemaTable extends
		LogicalInformationSchemaTable {

	private CatalogInformationSchemaTable storageSitesTable = null;
	private CatalogInformationSchemaTable storageGenerationsTable = null;
	private CatalogInformationSchemaTable persistentGroupsTable = null;
	private GenerationSitesTable generationSitesTable = null;
	
	private DelegatingInformationSchemaColumn group_name = null;
	private DelegatingInformationSchemaColumn version = null;
	private DelegatingInformationSchemaColumn site_name = null;
	
	public GenerationSiteInformationSchemaTable(LogicalInformationSchema schema) {
		super(new UnqualifiedName("generation_site"));
		storageSitesTable = (CatalogInformationSchemaTable) schema.buildInstance(null,new UnqualifiedName("storage_site"),null).getTable();
		storageGenerationsTable = (CatalogInformationSchemaTable) schema.buildInstance(null,new UnqualifiedName("storage_generation"),null).getTable();
		persistentGroupsTable = (CatalogInformationSchemaTable) schema.buildInstance(null,new UnqualifiedName("persistent_group"),null).getTable();
		if (storageSitesTable == null) throw new InformationSchemaException("persistent sites table?");
		if (storageGenerationsTable == null) throw new InformationSchemaException("generations table?");
		if (persistentGroupsTable == null) throw new InformationSchemaException("persistent groups table?");
		group_name = new DelegatingInformationSchemaColumn(persistentGroupsTable.lookup(null,new UnqualifiedName("name")),
				new UnqualifiedName("group_name"));
		version = new DelegatingInformationSchemaColumn(storageGenerationsTable.lookup(null,new UnqualifiedName("version")),
					new UnqualifiedName("version"));
		site_name = new DelegatingInformationSchemaColumn(storageSitesTable.lookup(null,new UnqualifiedName("name")),
				new UnqualifiedName("site_name"));
		addColumn(null,group_name);
		addColumn(null,version);
		addColumn(null,site_name);
	}

	
	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		generationSitesTable = new GenerationSitesTable(dbn);
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
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		SelectStatement in = lq.getQuery();
		FromTableReference anchor = null;
		for(Iterator<FromTableReference> iter = in.getTablesEdge().iterator(); iter.hasNext();) {
			FromTableReference ftr = iter.next();
			if (ftr.getBaseTable() != null) {
				if (ftr.getBaseTable().getTable() == this) {
					if (!ftr.getTableJoins().isEmpty())
						throw new InformationSchemaException("Use of table joins not supported on table " + getName());
					anchor = ftr;
					iter.remove();
					break;
				}
			}
		}
		if (anchor == null)
			throw new InformationSchemaException("Unable to find table " + getName() + " within info schema query");
		AliasInformation aliases = in.getAliases();
		TableInstance gsTable = new TableInstance(generationSitesTable,null,aliases.buildNewAlias(new UnqualifiedName("gs")),sc.getNextTable(), false);
		TableInstance ssTable = new TableInstance(storageSitesTable,null,aliases.buildNewAlias(new UnqualifiedName("ss")),sc.getNextTable(), false);
		TableInstance sgTable = new TableInstance(storageGenerationsTable,null,aliases.buildNewAlias(new UnqualifiedName("sg")),sc.getNextTable(), false);
		TableInstance pgTable = new TableInstance(persistentGroupsTable,null,aliases.buildNewAlias(new UnqualifiedName("pg")),sc.getNextTable(), false);
		in.getDerivedInfo().addLocalTable(gsTable.getTableKey(), ssTable.getTableKey(), sgTable.getTableKey(), pgTable.getTableKey());
		// record which forwards to where for later
		HashMap<LogicalInformationSchemaTable, TableInstance> forwarding = new HashMap<LogicalInformationSchemaTable, TableInstance>();
		forwarding.put(generationSitesTable,gsTable);
		forwarding.put(storageSitesTable,ssTable);
		forwarding.put(storageGenerationsTable,sgTable);
		forwarding.put(persistentGroupsTable,pgTable);
		
		// add these tables as ftrs
		in.getTablesEdge().add(new FromTableReference(pgTable));
		in.getTablesEdge().add(new FromTableReference(sgTable));
		in.getTablesEdge().add(new FromTableReference(ssTable));
		in.getTablesEdge().add(new FromTableReference(gsTable));
		// add our standard where clause
		List<ExpressionNode> decompAnd = null;
		if (in.getWhereClause() == null)
			decompAnd = new ArrayList<ExpressionNode>();
		else
			decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());

		LogicalInformationSchemaColumn sg_pers_group_id = storageGenerationsTable.lookup("storage_group"); 
		LogicalInformationSchemaColumn pg_pers_group_id = persistentGroupsTable.lookup("id"); 
		LogicalInformationSchemaColumn ss_id = storageSitesTable.lookup("id");
		LogicalInformationSchemaColumn sg_gen_id = storageGenerationsTable.lookup("id"); 
		
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(sg_pers_group_id, sgTable),
				new ColumnInstance(pg_pers_group_id, pgTable)));
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(generationSitesTable.getSiteID(), gsTable),
				new ColumnInstance(ss_id, ssTable)));
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(generationSitesTable.getGenerationID(), gsTable),
				new ColumnInstance(sg_gen_id,sgTable)));
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		
		// forward columns
		ColumnReplacementTraversal.replace(in,forwarding);
		for(Iterator<TableKey> iter = in.getDerivedInfo().getLocalTableKeys().iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (tk.getTable() == this)
				iter.remove();
		}
		
		return new LogicalCatalogQuery(lq,in);
	}

	private static class GenerationSitesTable extends LogicalInformationSchemaTable {

		private GenerationSitesColumn generation_id;
		private GenerationSitesColumn site_id;
		
		public GenerationSitesTable(DBNative dbn) {
			super(new UnqualifiedName("generation_site")); 
			// this has two columns - generation_id and site_id
			generation_id = new GenerationSitesColumn(new UnqualifiedName("generation_id"),"generation_id",dbn);
			site_id = new GenerationSitesColumn(new UnqualifiedName("site_id"), "site_id",dbn);
			addColumn(null,generation_id);
			addColumn(null,site_id);
		}
		
		GenerationSitesColumn getGenerationID() {
			return generation_id;
		}
		
		GenerationSitesColumn getSiteID() {
			return site_id;
		}

		@Override
		public String getTableName() {
			return "generation_sites";
		}
		
	}
	
	private static class GenerationSitesColumn extends LogicalInformationSchemaColumn {

		private String columnName;
		
		public GenerationSitesColumn(UnqualifiedName logicalName,
				String columnName,
				DBNative dbn) {
			super(logicalName, BasicType.buildType(java.sql.Types.INTEGER, 0, dbn));
			this.columnName = columnName;
		}
		
		@Override
		public String getColumnName() {
			return columnName;
		}
	}

	
}
