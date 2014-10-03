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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class InformationSchemaColumn implements Column<InformationSchemaTable> {

	protected InfoView view;
	
	protected UnqualifiedName name;
	protected InformationSchemaTable table;
	protected int position = -1;
	
	protected PEColumn backedBy;
	protected InformationSchemaTable returnType;
	
	public InformationSchemaColumn(InfoView view, UnqualifiedName nameInView, PEColumn pec) {
		super();
		this.view = view;
		if (this.view == null)
			this.name = nameInView; // temporary
		else
			this.name =	(view.isCapitalizeNames() ? nameInView.getCapitalized().getUnqualified() : nameInView);
		this.backedBy = pec;
	}
	
	protected InformationSchemaColumn(InformationSchemaColumn copy) {
		super();
		view = copy.view;
		name = copy.name;
		returnType = copy.returnType;
	}
	
	@Override
	public InformationSchemaTable getTable() {
		return table;
	}

	@Override
	public void setTable(InformationSchemaTable t) {
		table = t;
	}

	public PEColumn getColumn() {
		return backedBy;
	}
	
	public void validate(AbstractInformationSchema ofView, InformationSchemaTable ofTable) {
		
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

	public InformationSchemaTable getReturnType() {
		return returnType;
	}
	
	public boolean isBacked() {
		return true;
	}
		
	@Override
	public String toString() {
		Object backing = backedBy;
		return this.getClass().getSimpleName() + "{name=" + getName() + ", type=" + getType() + ", backing=" + backing + "}";
	}

	// probably don't need this to be abstract
	public abstract void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int ordinal_position, List<PersistedEntity> acc) throws PEException;
	
}
