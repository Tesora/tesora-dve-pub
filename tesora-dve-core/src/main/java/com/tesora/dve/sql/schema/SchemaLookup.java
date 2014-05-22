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

import java.util.Collection;

import com.tesora.dve.sql.util.UnaryFunction;

public class SchemaLookup<SchemaObject extends HasName> extends Lookup<SchemaObject> {

	public SchemaLookup(Collection<SchemaObject> declOrder, boolean exactMatch, boolean isCaseSensitive) {
		super(declOrder, new UnaryFunction<Name[], SchemaObject>() {

			@Override
			public Name[] evaluate(SchemaObject object) {
				return new Name[] { object.getName() };
			}
			
		}
				,exactMatch,isCaseSensitive);
	}
	
	public void refresh(Collection<SchemaObject> declOrder) {
		refreshBacking(declOrder);
	}
	
}
