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

import java.util.Arrays;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.InsertIntoTransformFactory;
import com.tesora.dve.sql.transform.strategy.SessionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class InsertIntoSelectStatement extends InsertStatement {

	private SingleEdge<InsertIntoSelectStatement, ProjectingStatement> sourceSelect =
		new SingleEdge<InsertIntoSelectStatement, ProjectingStatement>(InsertIntoSelectStatement.class, this, EdgeName.SUBQUERY);
	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { intoTable, columnSpec, sourceSelect, onDuplicateKey });
	
	private boolean nestedGrouped;
	
	public InsertIntoSelectStatement( 
			TableInstance tab, 
			List<ExpressionNode> columns,
			ProjectingStatement ss,
			boolean nestedGrouped,
			List<ExpressionNode> onDupKey,
			AliasInformation aliasInfo,
			SourceLocation loc) {
		super(tab,columns,onDupKey,aliasInfo, loc);
		this.sourceSelect.set(ss);
		this.nestedGrouped = nestedGrouped;
	}
	
	public boolean isNestedGrouped() {
		return nestedGrouped;
	}
	
	public ProjectingStatement getSource() {
		return sourceSelect.get();
	}
	
	public SingleEdge<InsertIntoSelectStatement, ProjectingStatement> getSourceEdge() {
		return sourceSelect;
	}
	
	@Override
	public TransformFactory[] getTransformers() {
		return new TransformFactory[] {
				new InformationSchemaRewriteTransformFactory(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new InsertIntoTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
		};
	}

	@Override
	public void normalize(SchemaContext sc) {
		sourceSelect.get().normalize(sc);
		if (intoTable.get().getAbstractTable().isView())
			throw new SchemaException(Pass.NORMALIZE, "No support for updatable views");
		PETable tab = intoTable.get().getAbstractTable().asTable();
		if (columnSpec.size() > tab.getColumns(sc).size())
			throw new SchemaException(Pass.NORMALIZE, "More columns specified than exist in target table");
		List<ExpressionNode> proj = sourceSelect.get().getProjections().get(0);
		if (proj.size() > tab.getColumns(sc).size()) 
			throw new SchemaException(Pass.NORMALIZE, "More columns in projection than in table");
		if (!columnSpec.isEmpty() && columnSpec.size() != proj.size()) {
			throw new SchemaException(Pass.NORMALIZE, "Column list not same size as projection");
		} 

		boolean isMT = sc.getPolicyContext().requiresMTRewrites(ExecutionType.INSERT);
		if (columnSpec.isEmpty()) {
			// nothing specified.  if the number of values specified matches the number of columns (up to mt stuff), then assume
			// all the columns were specified.
			int visibleColumns = tab.getColumns(sc).size();
			if (isMT) visibleColumns--;
			if (proj.size() == visibleColumns) {
				for(PEColumn c : tab.getColumns(sc)) {
					if (c.isTenantColumn()) continue;
					columnSpec.add(new ColumnInstance(null, c, intoTable.get()));
				}
			}
		}

		// missing columns
		// figure out what was specified first
		ListSet<Column<?>> specColumns = new ListSet<Column<?>>();
		Functional.apply(columnSpec.getMulti(), specColumns, new UnaryFunction<Column<?>,ExpressionNode>() {

			@Override
			public Column<?> evaluate(ExpressionNode object) {
				ColumnInstance ci = (ColumnInstance) object;
				return ci.getColumn();
			}
			
		});

		SQLMode sqlMode = SchemaVariables.getSQLMode(sc);
		
		SelectStatement src = null;
		if (sourceSelect.get() instanceof SelectStatement) {
			src = (SelectStatement) sourceSelect.get();
		}
		
		ListSet<PEColumn> autoincs = new ListSet<PEColumn>();
		boolean added = false;
		for(PEColumn c : tab.getColumns(sc)) {
			if (specColumns.contains(c))
				continue;			
			if (c.isAutoIncrement()) {
				autoincs.add(c);
				continue;
			}		
			columnSpec.add(new ColumnInstance(c,intoTable.get()));
			if (src == null) throw new SchemaException(Pass.NORMALIZE, "Unsupported: insert into select ... union ... where subquery normalization is required");
			if (isMT && c.isTenantColumn()) {
				src.getProjectionEdge().add(sc.getPolicyContext().getTenantIDLiteral(true));
				added = true;
			} else {
				ExpressionNode defaultValue = c.getDefaultValue();
				if (defaultValue == null) {
					if (c.isNullable()) {
						src.getProjectionEdge().add(LiteralExpression.makeNullLiteral());
						added = true;
					} else if (!sqlMode.isStrictMode()) {
						src.getProjectionEdge().add(c.getType().getZeroValueLiteral());
						added = true;
					} else {
						// this is an error
						throw new SchemaException(Pass.NORMALIZE, "Missing projection value for non-nullable column " + c);
					}
				} else {
					src.getProjectionEdge().add((ExpressionNode) ((LiteralExpression)defaultValue).copy(null));
					added = true;
				}
			}
		}
		// for autoincs, we place them at the end of the insert list, but not in the select
		for(PEColumn c : autoincs) {
			columnSpec.add(new ColumnInstance(c, intoTable.get()));
		}
		if (added)
			sourceSelect.get().normalize(sc);
		assertValidDupKey(sc);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.INSERT_INTO_SELECT;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		InsertIntoSelectStatement oth = (InsertIntoSelectStatement) other;
		return nestedGrouped == oth.nestedGrouped;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(1,nestedGrouped);
	}

}
