package com.tesora.dve.sql.infoschema.computed;

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
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnAdapter;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class BackedComputedInformationSchemaColumn extends
		ComputedInformationSchemaColumn {

	public BackedComputedInformationSchemaColumn(InfoView view, LogicalInformationSchemaColumn backing, UnqualifiedName nameInView) {
		super(view,nameInView,new BackedComputedInformationSchemaColumnAdapter(backing));
	}
	
	public BackedComputedInformationSchemaColumn(
			BackedComputedInformationSchemaColumn copy) {
		super(copy);
	}
	
	private BackedComputedInformationSchemaColumnAdapter getMyAdapter() {
		return (BackedComputedInformationSchemaColumnAdapter) getAdapter();
	}
	
	@Override
	public Type getType() {
		return getMyAdapter().getType();
	}

	@Override
	public InformationSchemaColumn copy(InformationSchemaColumnAdapter adapter) {
		return new BackedComputedInformationSchemaColumn(this.view,
				(adapter == null ? this.adapter : adapter).getLogicalColumn(),
				getName().getUnqualified());
	}

	@Override
	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte,
			int ordinal_position, List<PersistedEntity> acc) throws PEException {
		acc.add(getMyAdapter().buildColumnEntity(schema, cte, ordinal_position, getName().get()));
	}

	public static class BackedComputedInformationSchemaColumnAdapter extends InformationSchemaColumnAdapter {

		private final LogicalInformationSchemaColumn backing;
		
		public BackedComputedInformationSchemaColumnAdapter(LogicalInformationSchemaColumn backing) {
			this.backing = backing;
		}
		
		@Override
		public boolean isBacked() {
			return true;
		}

		@Override
		public LogicalInformationSchemaColumn getLogicalColumn() {
			return backing;
		}
		
		public LogicalInformationSchemaTable getReturnType() {
			return backing.getReturnType();
		}

		public Type getType() {
			return backing.getType();
		}
		
		public CatalogColumnEntity buildColumnEntity(CatalogSchema schema, CatalogTableEntity parent, int offset, String nameInView) throws PEException {
			return backing.buildColumnEntity(schema, parent, offset, nameInView);
		}
		
	}

}
