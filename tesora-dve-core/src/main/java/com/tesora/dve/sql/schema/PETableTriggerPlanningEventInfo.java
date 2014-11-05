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
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.CastFunctionCall;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TriggerTableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListSet;

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
			TriggerEvent event = (getBefore() != null ? getBefore().getEvent() : getAfter().getEvent());
			TriggerColumnTraversal trav = new TriggerColumnTraversal(event);
			Statement beforeStmt = null;
			Statement afterStmt = null;
			FeatureStep beforeStep = null;
			FeatureStep afterStep = null;
			ParserOptions was = sc.getOptions();
			ParserOptions now = was.setTriggerPlanning();
			try {
				sc.setOptions(now);
				if (getBefore() != null) {
					beforeStmt = getBefore().getBody(sc);
					trav.setTime(TriggerTime.BEFORE);
					trav.traverse(beforeStmt);
					beforeStep = beforeStmt.plan(sc, sc.getBehaviorConfiguration());
				}
				if (getAfter() != null) {
					afterStmt = getAfter().getBody(sc);
					trav.setTime(TriggerTime.AFTER);
					trav.traverse(afterStmt);
					afterStep = afterStmt.plan(sc, sc.getBehaviorConfiguration());
				}
			} finally {
				sc.setOptions(was);
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
		private final TriggerEvent event;
		private TriggerTime time;
		
		public TriggerColumnTraversal(TriggerEvent event) {
			super(Order.POSTORDER,ExecStyle.ONCE);
			triggerColumnOffsets = new LinkedHashMap<ColumnKey,Integer>();
			this.event = event;
		}
		
		public void setTime(TriggerTime time) {
			this.time = time;
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
					final TriggerTableInstance tti = (TriggerTableInstance) ti;
					ColumnKey ck = ci.getColumnKey();
					// if this is a before insert trigger, we're going to replace the NEW.autoinc columns with OLD.autoinc
					// for after insert triggers, we leave it alone.
					boolean wrap = false;
					if (event == TriggerEvent.INSERT && ck.getPEColumn().isAutoIncrement()) {
						if (time == TriggerTime.BEFORE) {
							TriggerTableInstance oti = new TriggerTableInstance(tti.getTable(),-1,TriggerTime.BEFORE);
							ColumnInstance oci = new ColumnInstance(ck.getPEColumn(),oti);
							ck = oci.getColumnKey();
							wrap = true;
						}
					}
					Integer any = triggerColumnOffsets.get(ck);
					if (any == null) {
						any = triggerColumnOffsets.size();
						triggerColumnOffsets.put(ck, any);
					}

					final LateBindingConstantExpression value = new LateBindingConstantExpression(any.intValue(),ci.getColumn().getType());
					if (wrap) {
						final ExpressionNode zeroLiteral = new ActualLiteralExpression(0, TokenTypes.Unsigned_Integer, null, null);
						return new FunctionCall(FunctionName.makeIfNull(), value, zeroLiteral);						
					}
					return value;
				}
			}
			return in;
		}
		
	}
	
	public static class LateBindingConstantCollector extends GeneralCollectingTraversal {

		public LateBindingConstantCollector() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}

		@Override
		public boolean is(LanguageNode ln) {
			return (ln instanceof LateBindingConstantExpression);
		}
		
	}
	
	public static ListSet<LateBindingConstantExpression> findLateBindingExprs(LanguageNode ln) {

		LateBindingConstantCollector collector = new LateBindingConstantCollector();
		collector.traverse(ln);
		
		ListSet<LateBindingConstantExpression> out = new ListSet<LateBindingConstantExpression>();
		for(LanguageNode iln : collector.getCollected())
			out.add((LateBindingConstantExpression) iln);
		
		return out;
	}
	
	public static class LateBindingCastingTraversal extends Traversal {

		public LateBindingCastingTraversal() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof LateBindingConstantExpression) {
				LateBindingConstantExpression lbce = (LateBindingConstantExpression) in;
				if (lbce.getType().isStringType()) {
					// cast to the string type
					return new CastFunctionCall(lbce,new UnqualifiedName(String.format("char(%d)",lbce.getType().getSize())));
				}
			}
			return in;
		}
		
	}
	
	public static void forceConstantTypes(ProjectingStatement src) {
		
		if (src instanceof SelectStatement) {
			SelectStatement ss = (SelectStatement) src;
			new LateBindingCastingTraversal().traverse(ss.getProjectionEdge());
		} else {
			// union statement
			UnionStatement us = (UnionStatement) src;
			forceConstantTypes(us.getFromEdge().get());
			forceConstantTypes(us.getToEdge().get());
		}
		
	}
	
}
