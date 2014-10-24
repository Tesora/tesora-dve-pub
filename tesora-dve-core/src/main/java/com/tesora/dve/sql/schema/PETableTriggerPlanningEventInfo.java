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
import java.util.LinkedHashMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TriggerTableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class PETableTriggerPlanningEventInfo extends PETableTriggerEventInfo {

	// the key iteration order defines the temp table result set
	private LinkedHashMap<ColumnKey,Integer> connValueOffsets;

	// the before body, with trigger cols replaced by runtime constants
	private Statement beforeBody;
	// the feature step that represents the before body
	private FeatureStep beforeStep;
	// the after body, with trigger cols replaced by runtime constants
	private Statement afterBody;
	// the after step that represents the after body
	private FeatureStep afterStep;
	
	public PETableTriggerPlanningEventInfo() {
		super();
		connValueOffsets = null;
	}

	private void ensureRuntime(SchemaContext sc) throws PEException {
		if (connValueOffsets == null) {
			TriggerColumnTraversal trav = new TriggerColumnTraversal();
			Statement beforeStmt = null;
			Statement afterStmt = null;
			FeatureStep beforeStep = null;
			FeatureStep afterStep = null;
			if (getBefore() != null) {
				beforeStmt = getBefore().getBody(sc);
				trav.traverse(beforeStmt);
				beforeStep = beforeStmt.plan(sc, sc.getBehaviorConfiguration());
			}
			if (getAfter() != null) {
				afterStmt = getAfter().getBody(sc);
				trav.traverse(afterStmt);
				afterStep = afterStmt.plan(sc, sc.getBehaviorConfiguration());
			}
			if (connValueOffsets == null) {
				synchronized(this) {
					if (connValueOffsets == null) {
						connValueOffsets = trav.getTriggerColumnOffsets();
						beforeBody = beforeStmt;
						afterBody = afterStmt;
						this.beforeStep = beforeStep;
						this.afterStep = afterStep;
					}
				}
			}
		}
	}
	
	@Override
	public Collection<ColumnKey> getTriggerBodyColumns(SchemaContext sc) throws PEException {
		ensureRuntime(sc);
		return connValueOffsets.keySet();
	}

	@Override
	public FeatureStep getBeforeStep(SchemaContext sc) throws PEException {
		ensureRuntime(sc);
		return beforeStep;
	}

	@Override
	public FeatureStep getAfterStep(SchemaContext sc) throws PEException {
		ensureRuntime(sc);
		return afterStep;
	}

	
	private static class TriggerColumnTraversal extends Traversal {

		LinkedHashMap<ColumnKey,Integer> triggerColumnOffsets;
		
		public TriggerColumnTraversal() {
			super(Order.POSTORDER,ExecStyle.ONCE);
			triggerColumnOffsets = new LinkedHashMap<ColumnKey,Integer>();
		}
		
		public LinkedHashMap<ColumnKey,Integer> getTriggerColumnOffsets() {
			return triggerColumnOffsets;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				TableInstance ti = ci.getTableInstance();
				if (ti instanceof TriggerTableInstance) {
					ColumnKey ck = ci.getColumnKey();
					Integer any = triggerColumnOffsets.get(ck);
					if (any == null) {
						any = triggerColumnOffsets.size();
						triggerColumnOffsets.put(ck, any);
					}
					return new LateBindingConstantExpression(any.intValue());
				}
			}
			return in;
		}
		
	}
		
}
