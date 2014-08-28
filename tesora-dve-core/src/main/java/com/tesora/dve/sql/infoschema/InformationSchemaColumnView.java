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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class InformationSchemaColumnView extends AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn> {

	protected LogicalInformationSchemaColumn backing;
	
	public InformationSchemaColumnView(InfoView view, LogicalInformationSchemaColumn basedOn,
			UnqualifiedName nameInView) {
		super(view,nameInView);
		backing = basedOn;
	}

	public InformationSchemaColumnView(InformationSchemaColumnView copy) {
		super(copy);
		backing = copy.backing;
	}
	
	@Override
	public AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn> copy() {
		return new InformationSchemaColumnView(this);
	}

	@Override
	public void prepare(SchemaView ofView, InformationSchemaTableView ofTable, DBNative dbn) {
		if (backing == null) return;
		if (backing.getReturnType() != null) {
			for(InformationSchemaTableView istv : ofView.getTables(null)) {
				if (istv.getLogicalTable() == backing.getReturnType()) {
					returnType = istv;
					break;
				}
			}
			if (returnType == null)
				throw new InformationSchemaException("No view table in view " + view + " for return type " + backing.getReturnType().getName() + ", needed for column " + getName() + " in table " + ofTable.getName());
		}
	}

	public final void freeze() {
		frozen = true;
	}
	
	
	@Override
	public Type getType() {
		return backing.getType();
	}

	@Override
	public LogicalInformationSchemaColumn getLogicalColumn() {
		return backing;
	}
		
	public UserColumn persist(CatalogDAO c, UserTable parent, DBNative dbn) {
		return backing.persist(c,parent,getName(),dbn);
	}
	
	@Override
	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int offset, List<PersistedEntity> acc) throws PEException {
		acc.add(backing.buildColumnEntity(schema, cte, offset, getName().get()));
	}
	
}
