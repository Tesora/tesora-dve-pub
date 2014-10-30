package com.tesora.dve.sql.statement.dml;

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
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.modifiers.InsertModifier;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;

public abstract class InsertStatement extends UnaryTableDMLStatement {

	protected MultiEdge<InsertStatement, ExpressionNode> columnSpec =
			new MultiEdge<InsertStatement, ExpressionNode>(InsertStatement.class, this, EdgeName.INSERT_COLUMN_SPEC);
	protected MultiEdge<InsertStatement,ExpressionNode> onDuplicateKey =
			new MultiEdge<InsertStatement,ExpressionNode>(InsertStatement.class, this, EdgeName.INSERT_DUPKEY);

	protected Boolean cacheable = null;
	protected TransactionStatement.Kind txnFlag = null;
	protected boolean ignore = false;
	protected InsertModifier insertModifier = null;

	protected InsertStatement(TableInstance table,
			List<ExpressionNode> columns,
			List<ExpressionNode> onDupKey,
			AliasInformation aliasInfo,
			SourceLocation loc) {
		super(loc);
		this.intoTable.set(table);
		this.columnSpec.set(columns);
		this.onDuplicateKey.set(onDupKey);
		setAliases(aliasInfo);
	}

	public List<ExpressionNode> getColumnSpecification() { return columnSpec.getMulti(); }
	public MultiEdge<InsertStatement,ExpressionNode> getColumnSpecificationEdge() { return columnSpec; }
	public List<ExpressionNode> getOnDuplicateKey() { return this.onDuplicateKey.getMulti(); }
	public MultiEdge<InsertStatement,ExpressionNode> getOnDuplicateKeyEdge() { return onDuplicateKey; }
	
	
	public void setColumnSpecification(List<ExpressionNode> spec) {
		columnSpec.set(spec);
	}
	
	public void setOnDuplicateKey(List<ExpressionNode> dk) {
		this.onDuplicateKey.set(dk);
	}
	
	public void setTxnFlag(TransactionStatement.Kind k) {
		txnFlag = k;
	}
	
	public TransactionStatement.Kind getTxnFlag() {
		return txnFlag;
	}
	
	public boolean isReplace() {
		return false;
	}
	
	public Boolean isCacheable() {
		return cacheable;
	}
	
	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.INSERT;
	}

	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tk, DistributionKey kv,
			DMLStatement sql) {
		throw new SchemaException(Pass.PLANNER, "InsertStatement does not plan single key steps");
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return DistKeyOpType.INSERT;
	}

	protected void assertValidDupKey(SchemaContext sc) {
		// we're only going to support key = key for now, and only for nonrandom tables
		// when the user says key = key, they mean that the inserted key value should win
		if (this.onDuplicateKey.size()<1)
			return;
		DistributionVector dv = intoTable.get().getAbstractTable().getDistributionVector(sc);
		if (dv.isRandom())
			throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key for random distributed tables");
		// we used to have a restriction that there couldn't be more than one tuple, but I don't believe we need that
		// since we only accept basically id=id and the use of on dup key means we don't set the update count anyhow,
		// it looks like that is a silly restriction
		// if (nvals > 1)
		//	throw new PEException("No support for multivalue insert with on duplicate key");
		// last test - we only support id = id and VALUES(id).  check for that
		for(ExpressionNode expr : this.onDuplicateKey.getMulti()) {
			if (EngineConstant.FUNCTION.has(expr, EngineConstant.EQUALS)) {
				FunctionCall eq = (FunctionCall) onDuplicateKey.get();
				ExpressionNode lhs = eq.getParametersEdge().get(0);
				ExpressionNode rhs = eq.getParametersEdge().get(1);
				
				if (!EngineConstant.COLUMN.has(lhs)) {
					throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key update of a non-column");
				}
				
				if (dv.contains(sc, ((ColumnInstance) lhs).getPEColumn()) && !((ColumnInstance) lhs).isSchemaEqual(rhs)) {
					throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key update of distribution column");
				}
				
				if (!EngineConstant.COLUMN.has(rhs) && !EngineConstant.FUNCTION.has(rhs) && !EngineConstant.CONSTANT.has(rhs)) {
					throw new SchemaException(Pass.NORMALIZE,"Unsupported right-hand side of on duplicate key update");
				}
				
				return;
			} else {
				// not an equals function
				break;
			}
			
		}
		// something more complex
		throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key insert value expression not of form id = id");
	}

	public boolean getIgnore() {
		return ignore;
	}
	
	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}

	public InsertModifier getModifier() {
		return insertModifier;
	}

	public void setModifier(InsertModifier im) {
		insertModifier = im;
	}
	
	@Override
	public TriggerEvent getTriggerEvent() {
		return TriggerEvent.INSERT;
	}

}
