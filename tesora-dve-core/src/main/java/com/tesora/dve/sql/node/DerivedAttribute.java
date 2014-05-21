// OS_STATUS: public
package com.tesora.dve.sql.node;

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

import com.tesora.dve.sql.schema.SchemaContext;

public abstract class DerivedAttribute<T> {

	public abstract boolean isApplicableSubject(LanguageNode ln);
	public abstract T computeValue(SchemaContext sc, LanguageNode ln);
	
	public T getValue(LanguageNode in, SchemaContext sc) {
		return in.getBlock().getValue(this, sc);
	}
	
	public boolean hasValue(LanguageNode in, SchemaContext sc) {
		return in.getBlock().hasValue(this, sc);
	}
	
}
