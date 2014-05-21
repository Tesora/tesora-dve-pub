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
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.UnionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.util.ListSet;

public class UnionStatement extends ProjectingStatement {

	private SingleEdge<UnionStatement, ProjectingStatement> lhs = 
		new SingleEdge<UnionStatement, ProjectingStatement>(UnionStatement.class, this, EdgeName.UNION_FROM);
	private SingleEdge<UnionStatement, ProjectingStatement> rhs = 
		new SingleEdge<UnionStatement, ProjectingStatement>(UnionStatement.class, this, EdgeName.UNION_TO);

	private boolean all;
	// lazily computed
	ProjectionInfo projectionInfo = null;
	
	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { lhs, rhs, orderBys, limitExpression });
	
	public UnionStatement(ProjectingStatement left, ProjectingStatement right, boolean unionAll, SourceLocation loc) {
		super(loc);
		lhs.set(left);
		rhs.set(right);
		all = unionAll;
	}
	
	public Edge<UnionStatement, ProjectingStatement> getFromEdge() { return lhs; }
	public Edge<UnionStatement, ProjectingStatement> getToEdge() { return rhs; }
	
	public boolean isUnionAll() {
		return all;
	}
	
	@Override
	public List<TableInstance> getBaseTables() {
		ListSet<TableInstance> out = new ListSet<TableInstance>();
		out.addAll(lhs.get().getBaseTables());
		out.addAll(rhs.get().getBaseTables());
		return out;
	}

	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.UNION;
	}

	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tk, DistributionKey kv,
			DMLStatement sql) throws PEException {
		// so, this is used when we determine that the entire thing is on a single site - so I guess this is just
		// a regular select?
		throw new PEException("union buildSingleKeyStep?");
	}

	@Override
	public TransformFactory[] getTransformers() {
		return new TransformFactory[] {
			new InformationSchemaRewriteTransformFactory(),
			new ViewRewriteTransformFactory(),
			new SingleSiteStorageGroupTransformFactory(),
			new UnionRewriteTransformFactory()	
		};
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return DistKeyOpType.QUERY;
	}

	@Override
	public void normalize(SchemaContext sc) {
		pullUpOrderbyLimit(sc);
		// normalize the two sides, I guess.
		// really should verify that they have the same number of columns
		lhs.get().normalize(sc);
		rhs.get().normalize(sc);
		// now verify that we have the same number of columns
		List<List<ExpressionNode>> projs = getProjections();
		int width = -1;
		for(List<ExpressionNode> l : projs) {
			if (width == -1) 
				width = l.size();
			else if (width != l.size())
				throw new SchemaException(Pass.NORMALIZE, "Invalid union, found differing numbers of columns");
		}
	}

	private ProjectingStatement findOrderByLimitHost() {
		if (isGrouped())
			return null;
		ProjectingStatement rhp = rhs.get();
		if (rhp.isGrouped())
			return null;
		else if (rhp instanceof UnionStatement) 
			return ((UnionStatement)rhp).findOrderByLimitHost();
		else if (rhp.getOrderBysEdge().has() || rhp.getLimitEdge().has())
			return rhp;
		else
			return null;
	}

	private List<SortingSpecification> convertOrderBys(SchemaContext sc, List<SortingSpecification> sorts, ProjectingStatement h) {
		// since this is before normalization - we're going to make a copy of the statement, then normalize
		List<ExpressionNode> unnormalized = h.getProjections().get(0);
		List<ExpressionNode> normalized = h.normalize(sc,unnormalized);
		ArrayList<SortingSpecification> out = new ArrayList<SortingSpecification>();
		for(SortingSpecification ss : sorts) {
			SortingSpecification oss = null;
			ExpressionNode targ = ss.getTarget();
			ExpressionNode actual = null;
			if (targ instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) targ;
				actual = ai.getTarget();
			} else if (targ instanceof LiteralExpression) {
				// copy it over
				LiteralExpression litex = (LiteralExpression) targ;
				oss = new SortingSpecification(litex,ss.isAscending());				
			} else {
				actual = targ;
			}
			if (oss == null) {
				int offset = -1;
				for(int i = 0; i < normalized.size(); i++) {
					if (actual instanceof ExpressionAlias) {
						ExpressionAlias ea = (ExpressionAlias) actual;
						if (ea.isSchemaEqual(normalized.get(i))) {
							offset = i;
							break;
						}
					} else {
						ExpressionNode en = ExpressionUtils.getTarget(normalized.get(i));
						if (actual.isSchemaEqual(en)) {
							offset = i;
							break;
						}
					}
				}
				if (offset == -1)
					throw new SchemaException(Pass.NORMALIZE, "Unable to determine column offset for order by clause " + ss.getTarget().toString(sc));
				oss = new SortingSpecification(LiteralExpression.makeLongLiteral(offset+1),ss.isAscending());
			}
			oss.setOrdering(ss.isOrdering());
			out.add(oss);
		}
		return out;
	}
	
	@Override
	protected SelectStatement findLeftmostSelect() {
		return lhs.get().findLeftmostSelect();
	}
	
	private void pullUpOrderbyLimit(SchemaContext sc) {
		if (getOrderBysEdge().has()) {
			setOrderBy(convertOrderBys(sc, getOrderBys(),findLeftmostSelect()));
		} else {
			ProjectingStatement hosted = findOrderByLimitHost();
			if (hosted != null) {
				if (hosted.getOrderBysEdge().has())
					setOrderBy(convertOrderBys(sc, hosted.getOrderBys(),hosted));
				setLimit(hosted.getLimit());
				hosted.getOrderBysEdge().clear();
				hosted.getLimitEdge().clear();
				hosted.setGrouped(true);
			}
		}		
	}
	
	@Override
	public List<ExpressionNode> normalize(SchemaContext sc, List<ExpressionNode> in) {
		throw new SchemaException(Pass.NORMALIZE, "Independent projection normalize unsupported on union statements");
	}
	
	@Override
	public List<List<ExpressionNode>> getProjections() {
		ArrayList<List<ExpressionNode>> out = new ArrayList<List<ExpressionNode>>();
		ProjectingStatement lps = (ProjectingStatement) lhs.get();
		ProjectingStatement rps = (ProjectingStatement) rhs.get();
		out.addAll(lps.getProjections());
		out.addAll(rps.getProjections());
		return out;
	}


	@Override
	public ProjectionInfo getProjectionMetadata(SchemaContext sc) {
		if (projectionInfo == null) {
			DMLStatement lps = (DMLStatement) lhs.get();
			projectionInfo = lps.getProjectionMetadata(sc);
			projectionInfo.unionize();
			pullUpOrderbyLimit(sc);
		}
		return projectionInfo;
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.UNION;
	}

	@Override
	public boolean supportsPartitions() {
		return false;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		UnionStatement ous = (UnionStatement) other;
		return all == ous.all;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(1,all);
	}
	
}
