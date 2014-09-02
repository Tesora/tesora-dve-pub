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

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class InformationSchemaColumn implements Column<InformationSchemaTable> {

	protected InfoView view;
	
	protected UnqualifiedName name;
	protected InformationSchemaTable table;
	protected int position = -1;
	
	protected InformationSchemaColumnAdapter adapter;
	protected InformationSchemaTable returnType;
	
	public InformationSchemaColumn(InfoView view, UnqualifiedName nameInView, InformationSchemaColumnAdapter columnAdapter) {
		super();
		this.view = view;
		if (this.view == null)
			this.name = nameInView; // temporary
		else
			this.name =	(view.isCapitalizeNames() ? nameInView.getCapitalized().getUnqualified() : nameInView);
		this.adapter = (columnAdapter == null ? new InformationSchemaColumnAdapter() : columnAdapter);
	}
	
	protected InformationSchemaColumn(InformationSchemaColumn copy) {
		super();
		view = copy.view;
		name = copy.name;
		adapter = copy.adapter;
		returnType = copy.returnType;
	}
	
	public abstract InformationSchemaColumn copy(InformationSchemaColumnAdapter newAdapter);
	
	@Override
	public InformationSchemaTable getTable() {
		return table;
	}

	@Override
	public void setTable(InformationSchemaTable t) {
		table = t;
	}

	public void validate(AbstractInformationSchema ofView, InformationSchemaTable ofTable) {
		
	}
	
	public void prepare(AbstractInformationSchema ofView, InformationSchemaTable ofTable, DBNative dbn) {
		if (getAdapter().getLogicalColumn() != null &&
				getAdapter().getLogicalColumn().getReturnType() != null) {
			for(InformationSchemaTable istv : ofView.getTables(null)) {
				if (istv.getLogicalTable() == getAdapter().getLogicalColumn().getReturnType()) {
					returnType = istv;
					break;
				}
			}
			if (returnType == null)
				throw new InformationSchemaException("No view table in view " + view + " for return type " + getAdapter().getLogicalColumn().getReturnType().getName() + ", needed for column " + getName() + " in table " + ofTable.getName());
		}
	}
	
	@Override
	public Name getName() {
		return this.name;
	}

	@Override
	public boolean isTenantColumn() {
		return false;
	}

	public boolean isOrderByColumn() {
		return false;
	}
	
	public boolean isIdentColumn() {
		return false;
	}
	
	public boolean requiresPrivilege() {
		return false;
	}

	public boolean isExtension() {
		return false;
	}
	
	public boolean isVisible() {
		return true;
	}
	
	public boolean isInjected() {
		return false;
	}
	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int v) {
		position = v;
	}

	public ColumnInstance buildNameTest(ColumnInstance ci) {
		return new ScopedColumnInstance(returnType.getIdentColumn(),ci);
	}

	public ColumnInstance buildNameTest(TableInstance in) {
		return buildNameTest(new ColumnInstance(null,this,in));
	}
		
	public InformationSchemaTable getReturnType() {
		return returnType;
	}
	
	public boolean isBacked() {
		return isSynthetic() ? false : adapter.isBacked(); 
	}
	
	public boolean isSynthetic() {
		return false;
	}
	
	@Override
	public String toString() {
		Object backing = null;
		if (getAdapter() != null) {
			backing = getAdapter().getLogicalColumn();
			if (backing == null)
				backing = getAdapter().getDirectColumn();
		}
		return this.getClass().getSimpleName() + "{name=" + getName() + ", type=" + getType() + ", backing=" + backing + "}";
	}

	public InformationSchemaColumnAdapter getAdapter() {
		return adapter;
	}
	
	// sucks...well, this will go away with time
	public void setAdapter(InformationSchemaColumnAdapter adapter) {
		this.adapter = adapter;
	}
	
	public LogicalInformationSchemaColumn getLogicalColumn() {
		return getAdapter().getLogicalColumn();
	}
	
	// probably don't need this to be abstract
	public abstract void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int ordinal_position, List<PersistedEntity> acc) throws PEException;
	
}
