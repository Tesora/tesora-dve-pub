// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

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

import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.transform.CopyContext;

// specifically for handling hibernate path expressions, i.e. ut.database.name
public class ScopedColumnInstance extends ColumnInstance {

	private ColumnInstance relativeTo;
	
	public ScopedColumnInstance(Column<?> c, ColumnInstance relativeTo) {
		super(c, relativeTo.getTableInstance());
		this.relativeTo = relativeTo;
	}

	protected ScopedColumnInstance(ScopedColumnInstance oth) {
		this(oth.getColumn(),oth.relativeTo);
	}
	
	
	public ColumnInstance getRelativeTo() {
		return relativeTo;
	}

	@Override
	protected ColumnInstance copySelf(CopyContext cc) {
		if (cc == null) 
			return new ScopedColumnInstance(this);
		throw new InformationSchemaException("ScopedColumnInstance cannot be copied with copy context");
	}

	
}
