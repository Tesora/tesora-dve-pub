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

import java.util.LinkedHashSet;
import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.TriggerTableKey;
import com.tesora.dve.sql.node.AbstractTraversal.ExecStyle;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TriggerTime;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.CopyContext;

public class TriggerTableInstance extends TableInstance {

	private final TriggerTime when;

	public static final class EarlyTriggerTableCollector {
		
		private final Traversal collector = new Traversal(Order.POSTORDER, ExecStyle.ONCE) {

			@Override
			public LanguageNode action(LanguageNode in) {
				if (in instanceof NameInstance) {
					final Name name = ((NameInstance) in).getName();
					if (name.isQualified()) {
						final UnqualifiedName namespace = ((QualifiedName) name).getNamespace();
						if (namespace.equals(TriggerTime.BEFORE.getAlias())) {
							EarlyTriggerTableCollector.this.beforeColumns.add(in);
						} else if (namespace.equals(TriggerTime.AFTER.getAlias())) {
							EarlyTriggerTableCollector.this.afterColumns.add(in);
						}
					}
				}

				return in;
			}
		};
		
		private final Set<LanguageNode> beforeColumns = new LinkedHashSet<LanguageNode>();
		private final Set<LanguageNode> afterColumns = new LinkedHashSet<LanguageNode>();

		public boolean hasBeforeColumns() {
			return !this.beforeColumns.isEmpty();
		}

		public boolean hasAfterColumns() {
			return !this.afterColumns.isEmpty();
		}

		protected void traverse(final LanguageNode ln) {
			this.collector.traverse(ln);
		}
	}

	public static EarlyTriggerTableCollector collectTriggerTableReferences(final Statement stmt) {
		final EarlyTriggerTableCollector collector = new EarlyTriggerTableCollector();
		collector.traverse(stmt);
		return collector;
	}

	public TriggerTableInstance(Table<?> schemaTable, long node, TriggerTime when) {
		super(schemaTable, schemaTable.getName(), when.getAlias(), node, false);
		this.when = when;
	}

	public TriggerTime getTime() {
		return when;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null)
			return withHints(new TriggerTableInstance(schemaTable,node,when));
		TriggerTableInstance out = (TriggerTableInstance) cc.getTableInstance(this);
		if (out != null) return out;
		out = withHints(new TriggerTableInstance(schemaTable, node, when));
		return cc.put(this, out);
	}


	public TableKey getTableKey() {
		return new TriggerTableKey(this);
	}

	
}
