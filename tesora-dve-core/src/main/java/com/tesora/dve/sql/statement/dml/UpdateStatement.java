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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.UpdateExecutionStep;

public class UpdateStatement extends MultiTableDMLStatement {
	
	private MultiEdge<UpdateStatement, SortingSpecification> orderBys =
		new MultiEdge<UpdateStatement, SortingSpecification>(UpdateStatement.class, this, EdgeName.ORDERBY);
	private SingleEdge<UpdateStatement, LimitSpecification> limitExpression =
		new SingleEdge<UpdateStatement, LimitSpecification>(UpdateStatement.class, this, EdgeName.LIMIT);
	private MultiEdge<UpdateStatement, ExpressionNode> updateClauses =
		new MultiEdge<UpdateStatement, ExpressionNode>(UpdateStatement.class, this, EdgeName.UPDATE_EXPRS);
	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { tableReferences, updateClauses, whereClause, orderBys, limitExpression });
	
	protected boolean ignore = false;

	public UpdateStatement(List<FromTableReference> tableRefs,
			List<ExpressionNode> updateExprs,
			ExpressionNode whereClause,
			List<SortingSpecification> orderbys,
			LimitSpecification limit,
			AliasInformation ai,
			SourceLocation sloc) {
		super(sloc);
		setTables(tableRefs);
		setOrderBys(orderbys);
		setUpdateExpressions(updateExprs);
		this.whereClause.set(whereClause);
		this.limitExpression.set(limit);
		setAliases(ai);
	}
	
	public UpdateStatement() {
		super(null);
	}
	
	public List<ExpressionNode> getUpdateExpressions() { return updateClauses.getMulti(); }
	public MultiEdge<UpdateStatement, ExpressionNode> getUpdateExpressionsEdge() { return updateClauses; }
	public UpdateStatement setUpdateExpressions(List<ExpressionNode> oth) { 
		updateClauses.set(oth);
		return this;
	}
	
	public List<SortingSpecification> getOrderBys() { return orderBys.getMulti(); }
	public UpdateStatement setOrderBys(List<SortingSpecification> oth) { 
		orderBys.set(oth);
		SortingSpecification.setOrdering(oth, Boolean.TRUE);
		return this;
	}
	
	public LimitSpecification getLimit() { return limitExpression.get(); }
	public UpdateStatement setLimit(LimitSpecification ls) {
		limitExpression.set(ls);
		return this;
	}
	
	@Override
	public void normalize(SchemaContext sc) {
				
	}
	
	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tk, DistributionKey kv, DMLStatement sql) throws PEException {
		return UpdateExecutionStep.build(sc,getDatabase(sc), getStorageGroup(sc), tk.getAbstractTable().asTable(), kv, sql,
				getDerivedInfo().doSetTimestampVariable(), distKeyExplain);
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return DistKeyOpType.UPDATE;
	}

	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.UPDATE;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T extends Edge<?,?>> List<T> getNaturalOrderEdges() {
		// natural order edges are the emission order
		ArrayList out = new ArrayList();
		out.add(tableReferences);
		out.add(updateClauses);
		out.add(whereClause);
		out.add(orderBys);
		out.add(limitExpression);
		return out;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.UPDATE;
	}

	public boolean getIgnore() {
		return ignore;
	}

	public void setIgnore(final boolean ignore) {
		this.ignore = ignore;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		UpdateStatement us = (UpdateStatement) other;
		return (us.ignore == ignore);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(1,ignore);
	}

	@Override
	public TriggerEvent getTriggerEvent() {
		return TriggerEvent.UPDATE;
	}
}
