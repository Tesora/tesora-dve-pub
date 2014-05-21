// OS_STATUS: public
package com.tesora.dve.sql.raw;

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
import java.util.HashMap;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.UnaryFunction;

public class RawSchema implements Schema<PEAbstractTable<?>> {

	protected Lookup<TempTable> tempTables;
	protected HashMap<TempTable,UnqualifiedName> actualNames;
	protected RawDB db;
	
	public RawSchema(RawDB rdb) {
		db = rdb;
		actualNames = new HashMap<TempTable,UnqualifiedName>();
		tempTables = new Lookup<TempTable>(true,true,new UnaryFunction<Name[],TempTable>() {

			@Override
			public Name[] evaluate(TempTable object) {
				return new Name[] { actualNames.get(object) };
			}
			
		}); 
	}
	
	public TempTable addTempTable(SchemaContext sc, TempTable tt, String declaredAs) {
		actualNames.put(tt, new UnqualifiedName(declaredAs));
		TempTable already = tempTables.lookup(declaredAs);
		if (already != null) return already;
		tempTables.add(tt);
		return tt;
	}
	
	@Override
	public PEAbstractTable<?> addTable(SchemaContext sc, PEAbstractTable<?> t) {
		throw new SchemaException(Pass.PLANNER, "Illegal call to RawSchema.addTable");
	}

	@Override
	public Collection<PEAbstractTable<?>> getTables(SchemaContext sc) {
		return null;
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored,
			boolean domtchecks) {
		if (domtchecks)
			throw new SchemaException(Pass.PLANNER, "no mt checks on raw schema");
		PEAbstractTable<?> candidate = tempTables.lookup(n);
		if (candidate != null)
			return new TableInstance(candidate, sc.getOptions().isResolve());
		return db.getBaseDatabase().getSchema().buildInstance(sc, n, ignored, domtchecks);
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored) {
		return buildInstance(sc,n,ignored,false);
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return new UnqualifiedName("rawplandb");
	}

}
