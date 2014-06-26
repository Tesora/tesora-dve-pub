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


import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.Traversable;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Cast;
import com.tesora.dve.sql.util.IsInstance;
import com.tesora.dve.sql.util.ListSet;

public class TableInstance extends ExpressionNode {

	public static final IsInstance<Traversable> instanceTest = new IsInstance<Traversable>(TableInstance.class);
	public static final Cast<TableInstance, Traversable> castTo = new Cast<TableInstance, Traversable>();
	
	protected Table<?> schemaTable;
	protected UnqualifiedName alias;
	protected Name specifiedAs;
	
	protected long node;
	protected List<IndexHint> indexHints = null;
	
	public TableInstance(Table<?> schemaTable, Name origName, UnqualifiedName tableAlias, boolean checknull) {
		this(schemaTable, origName, tableAlias, 0, checknull);
	}
	
	public TableInstance(Table<?> schemaTable, Name origName, UnqualifiedName tableAlias, long n, boolean checknull) {
		super((origName == null) ? null : origName.getOrig());
		// sometimes checknull is false for good reason - parameterization, for instance
		if (checknull && schemaTable == null)
			throw new SchemaException(Pass.SECOND, "Invalid table reference: no backing table");
		this.schemaTable = schemaTable;
		this.specifiedAs = origName;
		this.alias = tableAlias;
		node = n;
	}

	public TableInstance(Table<?> schemaTable, boolean checknull) {
		this(schemaTable,null,null, checknull);
	}
	
	public Table<?> getTable() { return this.schemaTable; }
	public UnqualifiedName getAlias() { return alias; }
	public void setAlias(UnqualifiedName un) { setAlias(un,true); }
	public void setAlias(UnqualifiedName un, boolean except) {
		if (except && alias != null)
			throw new SchemaException(Pass.SECOND, "Alias for table already specified");
		alias = un;
	}
	
	public Name getSpecifiedAs(SchemaContext sc) { return specifiedAs; }
	
	public PEAbstractTable<?> getAbstractTable() { return (PEAbstractTable<?>)this.schemaTable; }

	public void setTable(Table<?> tab) {
		if (schemaTable != null) throw new SchemaException(Pass.SECOND, "Attempt to re-resolve resolved table");
		schemaTable = tab;
	}
	
	public void setHints(List<IndexHint> ih) {
		indexHints = ih;
	}
	
	public List<IndexHint> getHints() {
		return indexHints;
	}
	
	public UnqualifiedName getReferenceName(SchemaContext sc) {
		if (alias != null)
			return alias;
		return schemaTable.getName(sc).getUnqualified();
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null) 
			return withHints(new TableInstance(schemaTable, specifiedAs, alias, node, false));
		TableInstance out = cc.getTableInstance(this);
		if (out != null) return out;
		out = withHints(new TableInstance(schemaTable, specifiedAs, alias, node, false));
		return cc.put(this, out);
	}

	private TableInstance withHints(TableInstance copy) {
		copy.setHints(getHints());
		return copy;
	}
	
	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		if (alias == null) {
			String fl = schemaTable.getName(sc).getUnquotedName().get().substring(0,1);
			return new NameAlias(new UnqualifiedName(fl + node));
		}
		return new NameAlias(alias);
	}
	
	public long getNode() {
		return node;
	}
	
	// for view support ONLY.  DO NOT use elsewhere.
	public void setNode(long n) {
		node = n;
	}
	
	public TableKey getTableKey() {
		return TableKey.make(this);
	}
	
	@Override
	public RewriteKey getRewriteKey() {
		return getTableKey();
	}
	
	public TableInstance adapt(Name tableName, UnqualifiedName alias, long node, boolean checknull) {
		return new TableInstance(schemaTable,tableName,alias,node, checknull);
	}
	
	public static ListSet<TableKey> makeKeySet(TableInstance in) {
		ListSet<TableKey> out = new ListSet<TableKey>();
		out.add(in.getTableKey());
		return out;
	}
	
	public void reload(SchemaContext sc) {
		if (schemaTable instanceof PETable) {
			schemaTable = (Table<?>) sc.getSource().find(sc, ((PETable)schemaTable).getCacheKey());
		}
	}
	
	public boolean isMT() {
		return false;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		TableInstance oti = (TableInstance) other;
		return getTableKey().equals(oti.getTableKey());
	}

	@Override
	protected int selfHashCode() {
		return getTableKey().hashCode();
	}
	
}
