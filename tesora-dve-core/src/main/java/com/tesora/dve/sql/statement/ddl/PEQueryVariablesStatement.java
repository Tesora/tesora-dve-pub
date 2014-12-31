package com.tesora.dve.sql.statement.ddl;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.direct.DirectShowVariablesTable;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TempColumn;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.CreateTempTableExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.strategy.TempGroupManager;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * So I guess the strategy is - build an memory table using the original column defs; do a big insert, execute the original wc
 * against the table, returning the results.
 */
public class PEQueryVariablesStatement extends DDLStatement {

	private final VariableScope scope;
	private final SelectStatement query;
	
	public PEQueryVariablesStatement(SelectStatement query,
			VariableScope scope) {
		super(true);
		this.scope = scope;
		this.query = query;
	}

	@Override
	public ProjectionInfo getProjectionMetadata(SchemaContext pc) {
		// we only ever do Variable_Name, Value - so just do those two
		ProjectionInfo pi = new ProjectionInfo(2);
		pi.addColumn(1,"Variable_name","Variable_name");
		pi.addColumn(2,"Value","Value");
		return pi;
	}


	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		TempGroupManager tgm = new TempGroupManager(); 
		PEStorageGroup aggSite = tgm.getGroup(true);
		List<PEColumn> columns = new ArrayList<PEColumn>();
		DBNative dbn = Singletons.require(DBNative.class);
		// we're going to hard code the definition, since this is all bespoke anyhow
		columns.add(new TempColumn(pc,new UnqualifiedName("variable_name"),BasicType.buildType(java.sql.Types.VARCHAR,64, dbn)));
		columns.add(new TempColumn(pc,new UnqualifiedName("variable_value"),BasicType.buildType(java.sql.Types.VARCHAR,1024,dbn)));
		// crap, we need a database, any database, if the current database doesn't exist
		// well, almost any database - we need one that is not in information_schema.
		PEDatabase cdb = getDatabase(pc);
		if (cdb == null) {
			cdb = pc.getAnyNonSchemaDatabase();
			if (cdb == null) {
				throw new PEException("No user-defined database present");
			}
		}
		TempTable tt = TempTable.buildAdHoc(pc, cdb, columns, DistributionVector.Model.BROADCAST, Collections.<PEColumn> emptyList(), aggSite, false);
		es.append(new CreateTempTableExecutionStep(cdb,aggSite,tt));
		List<List<String>> rawValues = pc.getConnection().getVariables(scope);
		final TableInstance ti = new TempTableInstance(pc,tt,new UnqualifiedName("tt"));
		List<ExpressionNode> colspec = Functional.apply(columns,new UnaryFunction<ExpressionNode,PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				return new ColumnInstance(object,ti);
			}
			
		});
		List<List<ExpressionNode>> values = Functional.apply(rawValues,new UnaryFunction<List<ExpressionNode>,List<String>>() {

			@Override
			public List<ExpressionNode> evaluate(List<String> object) {
				ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
				out.add(LiteralExpression.makeStringLiteral(object.get(1)));
				out.add(LiteralExpression.makeStringLiteral(object.get(2)));
				return out;
			}
			
		});
		
		InsertIntoValuesStatement populator = new InsertIntoValuesStatement(ti,
				colspec, values, null, null, null);
		populator.plan(pc, es, config);
		mapQuery(pc,query,ti);
		Emitter emitter = dbn.getEmitter();
		es.append(ProjectingExecutionStep.build(getDatabase(pc), aggSite,
				query.getGenericSQL(pc, emitter, EmitOptions.GENERIC_SQL)));
		
		tgm.plan(pc);
	}

	private void mapQuery(SchemaContext sc, SelectStatement in, TableInstance toTable) {
		// before we write this sucker out, we have to forward the table refs in the query
		Table<?> fromTable = in.getTablesEdge().get(0).getBaseTable().getTable(); 
		Map<Column<?>,Column<?>> mapping = new HashMap<Column<?>,Column<?>>();
		
		mapping.put(fromTable.lookup(sc, new UnqualifiedName(DirectShowVariablesTable.NAME_COL_NAME)),
				toTable.getAbstractTable().lookup(sc, "variable_name"));
		mapping.put(fromTable.lookup(sc, new UnqualifiedName(DirectShowVariablesTable.VALUE_COL_NAME)),
				toTable.getAbstractTable().lookup(sc, "variable_value"));		
		new TableForwarder(query.getDerivedInfo().getLocalTableKeys().get(0).getTable(),toTable,mapping).traverse(in);
		
	}
	
	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
	}


	@Override
	public Action getAction() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Persistable<?, ?> getRoot() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static class TableForwarder extends Traversal {
		
		private final Table<?> from;
		private final TableInstance to;
		
		private final Map<Column<?>,Column<?>> columnForwarding;
		
		public TableForwarder(Table<?> fromTab, TableInstance toTab, Map<Column<?>,Column<?>> columns) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.from = fromTab;
			this.to = toTab;
			this.columnForwarding = columns;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) in;
				TableInstance current = ci.getTableInstance();
				TableInstance next = map(current);
				return new ColumnInstance(ci.getSpecifiedAs(),map(ci.getColumn()),next);
			} else if (in instanceof TableInstance) {
				return map((TableInstance)in);
			} else {
				return in;
			}
		}
		
		private Column<?> map(Column<?> in) {
			Column<?> out = columnForwarding.get(in);
			if (out == null) return in;
			return out;
		}
		
		private TableInstance map(TableInstance in) {
			Table<?> theTab = in.getTable();
			if (theTab == from)
				return to;
			return in;
		}
	}
	
}
