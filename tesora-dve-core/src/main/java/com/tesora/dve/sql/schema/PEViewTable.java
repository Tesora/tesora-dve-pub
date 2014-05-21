// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

import java.util.List;

import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class PEViewTable extends PEAbstractTable<PEViewTable> {

	private SchemaEdge<PEView> view;
	private boolean loaded;
	private Boolean hasCardInfo;
	
	@SuppressWarnings("unchecked")
	public PEViewTable(SchemaContext pc, Name name, 
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv,  
			PEPersistentGroup defStorage, PEDatabase db,
			TableState theState,
			PEView theView) {
		super(pc,name,fieldsAndKeys,dv,defStorage,db,theState);
		this.view = StructuralUtils.buildEdge(pc, theView, false);
		loaded = true;
	}
	
	@SuppressWarnings("unchecked")
	protected PEViewTable(UserTable table, SchemaContext lc) {
		super(table,lc);
		loaded = false;
		lc.startLoading(this, table);
		loadPersistent(table,lc);
		checkLoaded(lc);
		view = StructuralUtils.buildEdge(lc, PEView.load(table.getView(), lc), true);
		lc.finishedLoading(this, table); 
	}
	
	@Override
	public PETable asTable() {
		throw new IllegalStateException("Cannot cast a view to a table");
	}

	@Override
	public boolean isView() {
		return true;
	}
	
	public PEView getView(SchemaContext sc) {
		return view.get(sc);
	}
	
	@Override
	public PEAbstractTable<?> recreate(SchemaContext sc, String decl, LockInfo li) {
		// this is the backing table declaration
		PETable backing = super.recreate(sc, decl, li).asTable();
		PEViewTable npvt = new PEViewTable(sc,backing.getName(),
				Functional.apply(backing.getColumns(sc), new UnaryFunction<TableComponent<?>, PEColumn>(){

					@Override
					public TableComponent<?> evaluate(PEColumn object) {
						return object;
					}
					
				}),
				backing.getDistributionVector(sc),
				backing.getPersistentStorage(sc), backing.getPEDatabase(sc), backing.getState(),
				new PEView(sc,view.get(sc),this));
		return npvt;
	}

	@Override
	protected void populateNew(SchemaContext sc, UserTable p)
			throws PEException {
		super.populateNew(sc, p);
		UserView uv = view.get(sc).persistTree(sc);
		p.setView(uv);
		uv.setTable(p);
	}

	@Override
	protected Persistable<PEViewTable, UserTable> load(SchemaContext sc,
			UserTable p) throws PEException {
		return new PEViewTable(p,sc);
	}

	@Override
	protected String getDiffTag() {
		return "View";
	}

	@Override
	public PEViewTable asView() {
		return this;
	}

	@Override
	protected boolean isLoaded() {
		return loaded;
	}

	@Override
	protected void setLoaded() {
		loaded = true;
	}

	@Override
	public String getTableType() {
		return "VIEW";
	}

	@Override
	public boolean hasCardinalityInfo(SchemaContext sc) {
		if (hasCardInfo == null) {
			ListSet<TableKey> tabs = view.get(sc).getViewDefinition(sc, this, false).getAllTableKeys();
			boolean has = true;
			for(TableKey tk : tabs) {
				if (!tk.getAbstractTable().hasCardinalityInfo(sc)) {
					has = false;
					break;
				}
			}
			hasCardInfo = has;
		}
		return hasCardInfo.booleanValue();
	}


}
