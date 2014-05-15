// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.Arrays;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.DeleteExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.strategy.ContainerBaseTableRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DegenerateExecuteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DeleteRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DistributionKeyExecuteTransformFactory;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.NestedQueryBroadcastTransformFactory;
import com.tesora.dve.sql.transform.strategy.SessionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.nested.NestedQueryRewriteTransformFactory;

public class DeleteStatement extends MultiTableDMLStatement {

	// when we support multitable delete, this will need to be a multiedge
	private MultiEdge<DeleteStatement, TableInstance> deleteTableRef =
		new MultiEdge<DeleteStatement, TableInstance>(DeleteStatement.class, this, EdgeName.DELETE_TABLE);
	private MultiEdge<DeleteStatement, SortingSpecification> orderBys =
		new MultiEdge<DeleteStatement, SortingSpecification>(DeleteStatement.class, this, EdgeName.ORDERBY);
	private SingleEdge<DeleteStatement, LimitSpecification> limitExpression =
		new SingleEdge<DeleteStatement, LimitSpecification>(DeleteStatement.class, this, EdgeName.LIMIT);
	
	// true if this delete was originally a truncate statement.
	private boolean truncate;
	
	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { this.deleteTableRef, this.tableReferences, this.whereClause });

	public DeleteStatement(List<FromTableReference> tableRefs,
			ExpressionNode whereClause, boolean truncateTable, AliasInformation ai) {
		this(null, tableRefs, whereClause, null, null, truncateTable, ai, null);
	}
	
	public DeleteStatement(List<TableInstance> explicitDeleteTables, List<FromTableReference> tableRefs,
			ExpressionNode whereClause,	List<SortingSpecification> orderbys, 
			LimitSpecification limit, boolean truncateTable, AliasInformation ai,
			SourceLocation sloc) {
		super(sloc);
		setTargetDeletes(explicitDeleteTables);
		setTables(tableRefs);
		setWhereClause(whereClause);
		setOrderBy(orderbys);
		setLimit(limit);
		truncate = truncateTable;
		setAliases(ai);
	}

	public DeleteStatement() {
		super(null);
	}
	
	public List<SortingSpecification> getOrderBys() { return orderBys.getMulti(); }
	public MultiEdge<DeleteStatement, SortingSpecification> getOrderBysEdge() { return orderBys; }
	public DeleteStatement setOrderBy(List<SortingSpecification> order) { 
		orderBys.set(order);
		SortingSpecification.setOrdering(order, Boolean.TRUE);
		return this;
	}
		
	public LimitSpecification getLimit() { return limitExpression.get(); }
	public DeleteStatement setLimit(LimitSpecification ls) { limitExpression.set(ls); return this;}
	public Edge<DeleteStatement, LimitSpecification> getLimitEdge() { return limitExpression; }

	public boolean isTruncate() {
		return truncate;
	}
	
	public DeleteStatement setTruncate(boolean v) {
		truncate = v;
		return this;
	}
	
	public MultiEdge<DeleteStatement, TableInstance> getTargetDeleteEdge() {
		return this.deleteTableRef;
	}
	
	public List<TableInstance> getTargetDeletes() {
		return deleteTableRef.getMulti();
	}
	
	public void setTargetDeletes(List<TableInstance> ti) {
		deleteTableRef.set(ti);
	}
	
	@Override
	public void normalize(SchemaContext sc) {
		// the front end does not actually accept delete T from T where f(T)
		// but we're going to normalize to that
		// the emitter takes care of not emitting this if it's not allowed
		if (!deleteTableRef.has()) {
			FromTableReference first = tableReferences.get(0);
			TableInstance fti = first.getBaseTable();
			TableInstance oti = (TableInstance) fti.copy(null);
			deleteTableRef.set(oti);
		}
		for(TableInstance ti : deleteTableRef.getMulti()) {
			if (ti.getAbstractTable().isView())
				throw new SchemaException(Pass.NORMALIZE,"No support for updatable views");
		}
	}

	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tab, DistributionKey kv, DMLStatement sql) throws PEException {
		return DeleteExecutionStep.build(
				sc,
				getDatabase(sc),
				getStorageGroup(sc),
				tab,
				kv,
				sql,
				false,
				distKeyExplain);
	}
	
	@Override
	public TransformFactory[] getTransformers() {
		return new TransformFactory[] {
				new InformationSchemaRewriteTransformFactory(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new ContainerBaseTableRewriteTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
				new NestedQueryBroadcastTransformFactory(),
				new NestedQueryRewriteTransformFactory(),
				new DeleteRewriteTransformFactory(),
				new DistributionKeyExecuteTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return DistKeyOpType.UPDATE;
	}

	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.DELETE;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.DELETE;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		DeleteStatement ds = (DeleteStatement) other;
		return truncate == ds.truncate;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(1,truncate);
	}

	
	
}
