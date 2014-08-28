package com.tesora.dve.sql.infoschema.direct;

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
import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.UnaryFunction;

public class DirectInformationSchemaTableView implements InformationSchemaTableView<DirectInformationSchemaColumnView> {

	// due to type pressures, we have to copy all the columns
	
	private final PEViewTable backing;
	private final boolean privileged;
	private final boolean extension;
	private final InfoView view;

	private final Name name; // might be different than what is declared (i.e. case)
	private final Name pluralName;
	
	protected List<DirectInformationSchemaColumnView> columns;
	protected Lookup<DirectInformationSchemaColumnView> lookup;

	
	private final DirectInformationSchemaColumnView orderByColumn;
	private final DirectInformationSchemaColumnView identColumn;
	
	// we maintain a separate lookup in the view columns - this handles the case sensitivity, etc
	
	
	public DirectInformationSchemaTableView(SchemaContext sc, InfoView view, PEViewTable viewTab,
			boolean privileged, boolean extension,
			String orderByColumn, String identColumn) {
		this.backing = viewTab;
		this.privileged = privileged;
		this.extension = extension;
		this.view = view;
		// load up our columns
		columns = new ArrayList<DirectInformationSchemaColumnView>();
		this.lookup = new Lookup<DirectInformationSchemaColumnView>(columns, 
				new UnaryFunction<Name[], DirectInformationSchemaColumnView>() {

					@Override
					public Name[] evaluate(DirectInformationSchemaColumnView object) {
						return new Name[] { object.getName() };
					}
			
				},				
				false, view.isLookupCaseSensitive()); 
		for(PEColumn pec : backing.getColumns(sc)) {
			addColumn(sc,new DirectInformationSchemaColumnView(view,pec.getName().getUnqualified(),pec));
		}
		Name viewName = viewTab.getName();
		this.name = (view.isCapitalizeNames() ? viewName.getCapitalized().getUnqualified() : viewName);
		this.pluralName = null;
//		if (pluralViewName == null) this.pluralName = pluralViewName;
//		else this.pluralName = (view.isCapitalizeNames() ? pluralViewName.getCapitalized().getUnqualified() : pluralViewName); 
		this.orderByColumn = (orderByColumn == null ? null : lookup.lookup(orderByColumn));
		this.identColumn = (identColumn == null ? null : lookup.lookup(identColumn));
	}
	
	@Override
	public DirectInformationSchemaColumnView addColumn(SchemaContext sc, DirectInformationSchemaColumnView cv) {
		cv.setPosition(columns.size());
		columns.add(cv);
		lookup.refreshBacking(columns);
		cv.setTable(this);
		return cv;
	}

	@Override
	public List<DirectInformationSchemaColumnView> getColumns(SchemaContext sc) {
		return columns;
	}

	@Override
	public DirectInformationSchemaColumnView lookup(SchemaContext sc, Name n) {
		return lookup.lookup(n);
	}

	@Override
	public Name getName(SchemaContext sc) {
		return name;
	}

	@Override
	public Name getPluralName() {
		return pluralName;
	}
	
	@Override
	public boolean isInfoSchema() {
		return true;
	}

	@Override
	public boolean isTempTable() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return ((Database<?>)sc.getSource().find(sc, view.getCacheKey()));
	}

	
	@Override
	public Name getName() {
		return backing.getName();
	}

	@Override
	public InfoView getView() {
		return view;
	}

	@Override
	public void prepare(SchemaView view, DBNative dbn) {
		// does nothing
	}

	@Override
	public void inject(SchemaView view, DBNative dbn) {
		// does nothing
	}

	@Override
	public void freeze() {
		// already frozen
	}

	@Override
	public LogicalInformationSchemaTable getLogicalTable() {
		// no backing table
		return null;
	}

	public PEViewTable getBackingView() {
		return backing;
	}
	
	@Override
	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db,
			int dmid, int storageid, List<PersistedEntity> acc)
			throws PEException {
		buildTableEntity(cs,db,dmid,storageid,acc,backing);
	}

	@Override
	public DirectInformationSchemaColumnView getOrderByColumn() {
		return orderByColumn;
	}

	@Override
	public DirectInformationSchemaColumnView getIdentColumn() {
		return identColumn;
	}

	@Override
	public boolean requiresPriviledge() {
		return privileged;
	}

	@Override
	public boolean isExtension() {
		return extension;
	}

	public static void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, 
			List<PersistedEntity> acc, PEAbstractTable<?> table) throws PEException {
		CatalogTableEntity cte = new CatalogTableEntity(cs,db,table.getName().get(),dmid,storageid,"MEMORY");
		acc.add(cte);
		int counter = 0;
		for(PEColumn pec : table.getColumns(null)) {
			CatalogColumnEntity cce = new CatalogColumnEntity(cs,cte);
			cce.setName(pec.getName().get());
			cce.setNullable(pec.isNullable());
			cce.setType(pec.getType());
			cce.setPosition(counter);
			acc.add(cce);
		}
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in,
			TableKey onTK) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isVariablesTable() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public void assertPermissions(SchemaContext sc) {
		if (!privileged) return;
		if (!sc.getPolicyContext().isRoot())
			throw new InformationSchemaException("You do not have permissions to query " + getName().get());	
	}

	@Override
	public boolean isView() {
		return true;
	}

	
}
