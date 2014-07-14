package com.tesora.dve.sql.infoschema;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.variables.KnownVariables;

public abstract class SchemaView implements
		Schema<InformationSchemaTableView> {

	private boolean frozen;
	private List<InformationSchemaTableView> tables;
	protected Lookup<InformationSchemaTableView> lookup;

	protected final InfoView view;
	
	// the backing logical view.
	protected LogicalInformationSchema logical;
	
	// reverse lookup for injection purposes
	protected Map<LogicalInformationSchemaTable, InformationSchemaTableView> reverse;
	
	protected SchemaEdge<DatabaseView> db;
	
	public SchemaView(LogicalInformationSchema basedOn, InfoView servicing,
			UnaryFunction<Name[], InformationSchemaTableView> getNamesFunc) {
		super();
		frozen = false;
		tables = new ArrayList<InformationSchemaTableView>();
		lookup = new Lookup<InformationSchemaTableView>(tables, getNamesFunc, false, false);
		reverse = new HashMap<LogicalInformationSchemaTable, InformationSchemaTableView>();
		view = servicing;
		logical = basedOn;
	}
	
	public InfoView getView() {
		return view;
	}
	
	public void freeze(DBNative dbn) {
		for(InformationSchemaTableView istv : tables)
			istv.prepare(this,dbn);
		for(InformationSchemaTableView istv : tables) 
			istv.inject(this, dbn);
		for(InformationSchemaTableView istv : tables)
			istv.freeze();
	}
	
	@Override
	public InformationSchemaTableView addTable(SchemaContext sc, InformationSchemaTableView t) {
		if (frozen)
			throw new InformationSchemaException("Information schema for " + getView() + " is frozen, cannot add table");
		InformationSchemaTableView already = lookup.lookup(t.getName());
		if (already != null)
			return already;
		tables.add(t);
		lookup.refreshBacking(tables);
		if (t.getLogicalTable() != null)
			reverse.put(t.getLogicalTable(), t);
		return t;
	}

	@Override
	public Collection<InformationSchemaTableView> getTables(SchemaContext sc) {
		return tables;
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored, boolean domtchecks) {
		InformationSchemaTableView istv = lookup.lookup(n); 
		if (istv == null) return null;
		return new TableInstance(istv,false);
	}
	
	@Override 
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored) {
		InformationSchemaTableView istv = lookup.lookup(n);
		if (istv == null) return null;
		return new TableInstance(istv, false);
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return new UnqualifiedName(view.getUserDatabaseName());
	}
	
	public InformationSchemaTableView lookup(LogicalInformationSchemaTable list) {
		return reverse.get(list);
	}
	
	public InformationSchemaTableView lookup(String s) {
		return lookup.lookup(s);
	}
	
	public void buildEntities(CatalogSchema schema, int groupid, int modelid, String charSet, String collation, List<PersistedEntity> acc) throws PEException {		
		CatalogDatabaseEntity cde = new CatalogDatabaseEntity(schema, view.getUserDatabaseName(), groupid, charSet,collation);
		acc.add(cde);
		for(InformationSchemaTableView t : tables) {
			t.buildTableEntity(schema, cde, modelid, groupid, acc);
		}
	}
	
	protected boolean useExtensions(SchemaContext sc) {
		return 	KnownVariables.SHOW_METADATA_EXTENSIONS.getValue(sc.getConnection().getVariableSource()).booleanValue();
	}

	protected boolean hasPriviledge(SchemaContext sc) {
		return sc.getPolicyContext().isRoot();
	}
	
}
