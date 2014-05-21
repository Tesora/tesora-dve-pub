// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Accessor;
import com.tesora.dve.sql.util.Cast;

public class ColumnInstance extends ExpressionNode {

	public static final Cast<ColumnInstance, ExpressionNode> castTo = new Cast<ColumnInstance, ExpressionNode>();
	public static final Accessor<TableInstance, ColumnInstance> getTableInstance = new Accessor<TableInstance, ColumnInstance>() {

		@Override
		public TableInstance evaluate(ColumnInstance object) {
			return object.getTableInstance();
		}
		
	};
	public static final Accessor<Column<?>, ColumnInstance> getColumn = new Accessor<Column<?>, ColumnInstance>() {

		@Override
		public Column<?> evaluate(ColumnInstance object) {
			return object.getColumn();
		}
		
	};
	public static final Accessor<PEColumn, ColumnInstance> getPEColumn = new Accessor<PEColumn, ColumnInstance>() {

		@Override
		public PEColumn evaluate(ColumnInstance object) {
			return object.getPEColumn();
		}

		
	};

	public static final Accessor<ColumnKey, ColumnInstance> getColumnKey = new Accessor<ColumnKey, ColumnInstance>() {

		@Override
		public ColumnKey evaluate(ColumnInstance object) {
			return object.getColumnKey();
		}

		
	};
	
	private Column<?> schemaColumn;
	private TableInstance ofTable;
	private Name specifiedAs;
	
	public ColumnInstance(Name origName, Column<?> column, TableInstance table) {
		super(origName == null ? null : origName.getOrig());
		ofTable = table;
		schemaColumn = column;
		specifiedAs = origName;
	}
	
	public ColumnInstance(Column<?> c, TableInstance ti) {
		this(null,c,ti);
	}
	
	public TableInstance getTableInstance() { return ofTable; }
	public Column<?> getColumn() { return schemaColumn; }
	
	public PEColumn getPEColumn() { return (PEColumn)schemaColumn; }
	
	public Name getSpecifiedAs() { 
		return specifiedAs; 
	}
		
	public Name getReferenceName(SchemaContext sc) {
		if (schemaColumn == null)
			return specifiedAs;
		if (specifiedAs == null || !specifiedAs.isQualified()) 
			return schemaColumn.getName().getUnqualified().postfix(ofTable.getReferenceName(sc));
		else
			return specifiedAs;
	}

	@Override
	protected ColumnInstance copySelf(CopyContext cc) {
		if (cc == null) 
			return new ColumnInstance(specifiedAs, schemaColumn, ofTable);
		ColumnInstance out = cc.getColumnInstance(this);
		if (out != null) return out;
		TableInstance cti = cc.getTableInstance(ofTable);
		if (cti == null) cti = (TableInstance) ofTable.copy(cc);
		out = new ColumnInstance(specifiedAs, schemaColumn, cti);
		return cc.put(this, out);
	}
	
	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		String fl = schemaColumn.getName().getUnquotedName().getUnqualified().get().trim().substring(0,1);
		return new NameAlias(new UnqualifiedName(ofTable.buildAlias(sc).get() + fl + schemaColumn.getPosition()));
	}

	public ColumnKey getColumnKey() {
		return new ColumnKey(this);
	}
	
	public boolean rewriteEqual(ColumnInstance other) {
		return getColumnKey().equals(other.getColumnKey());
	}

	@Override
	public RewriteKey getRewriteKey() {
		return getColumnKey();
	}

	public ColumnInstance postFlipAdapt(TableInstance repl) {
		return new ColumnInstance(specifiedAs,schemaColumn,repl);
	}
	
	public void reload(SchemaContext sc) {
		ofTable.reload(sc);
		schemaColumn = ofTable.getTable().lookup(sc, schemaColumn.getName());
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		ColumnInstance oci = (ColumnInstance) other;
		return getColumnKey().equals(oci.getColumnKey());
	}

	@Override
	protected int selfHashCode() {
		return getColumnKey().hashCode();
	}
}
