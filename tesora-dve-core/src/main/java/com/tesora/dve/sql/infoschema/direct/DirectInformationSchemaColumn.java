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

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnAdapter;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class DirectInformationSchemaColumn extends InformationSchemaColumn {
	
	private static final byte IDENT = 1;
	private static final byte ORDERBY = 2;
	private static final byte EXTENSION = 4;
	private static final byte PRIVILEGE = 8;
	private static final byte FULL = 16;
	
	private final byte flags;
	
	public DirectInformationSchemaColumn(InfoView view,
			UnqualifiedName nameInView,
			PEColumn backedBy,
			boolean isIdent,
			boolean isOrderBy,
			boolean isExtension,
			boolean isPrivilege,
			boolean isFull) {
		super(view, nameInView, new DirectInformationSchemaColumnAdapter(backedBy));
		byte f = 0;
		if (isIdent) f |= IDENT;
		if (isOrderBy) f |= ORDERBY;
		if (isExtension) f |= EXTENSION;
		if (isPrivilege) f |= PRIVILEGE;
		if (isFull) f |= FULL;
		flags = f;
	}

	@Override
	public Type getType() {
		return getAdapter().getDirectColumn().getType();
	}

	@Override
	public InformationSchemaColumn copy(InformationSchemaColumnAdapter adapter) {
		return new DirectInformationSchemaColumn(view,getName().getUnqualified(),
				(adapter == null ? getAdapter().getDirectColumn() : adapter.getDirectColumn()),
				isIdentColumn(),isOrderByColumn(),isExtension(),requiresPrivilege(), isFull()); 
	}

	DirectInformationSchemaColumnAdapter getMyAdapter() {
		return (DirectInformationSchemaColumnAdapter) getAdapter();
	}
	
	public PEColumn getColumn() {
		return getMyAdapter().getDirectColumn();
	}
	
	@Override
	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte,
			int ordinal_position, List<PersistedEntity> acc) throws PEException {
		// TODO Auto-generated method stub

	}

	private boolean isSet(byte flag) {
		return (flags & flag) == flag;
	}
	
	public boolean isOrderByColumn() {
		return isSet(ORDERBY);
	}
	
	public boolean isIdentColumn() {
		return isSet(IDENT);
	}
	
	public boolean requiresPrivilege() {
		return isSet(PRIVILEGE);
	}

	public boolean isExtension() {
		return isSet(EXTENSION);
	}

	public boolean isFull() {
		return isSet(FULL);
	}
	
	public static class DirectInformationSchemaColumnAdapter extends InformationSchemaColumnAdapter {

		// eventually we will have more than one here, or else a lookup into the actual 
		private final PEColumn backing;
		
		public DirectInformationSchemaColumnAdapter(PEColumn pec) {
			this.backing = pec;
		}
		
		@Override
		public boolean isBacked() {
			return true;
		}
		
		@Override
		public PEColumn getDirectColumn() {
			return backing;
		}
	}
	
}
