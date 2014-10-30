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
import java.util.LinkedHashMap;
import java.util.List;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.TransformException;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.MultiMultiEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.Default;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.AutoIncrementBlock;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.strategy.featureplan.InsertValuesFeatureStep.UpdateCountAdjuster;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.KnownVariables;

public class InsertIntoValuesStatement extends InsertStatement implements UpdateCountAdjuster {

	public static final boolean batchInserts = true;
	
	protected MultiMultiEdge<InsertIntoValuesStatement, ExpressionNode> values =
		new MultiMultiEdge<InsertIntoValuesStatement, ExpressionNode>(InsertIntoValuesStatement.class, this, EdgeName.INSERT_MULTIVALUE);
		
	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { intoTable, columnSpec, values, onDuplicateKey });
	
	// we now do some inserts behind the scenes - their update count does not count
	protected boolean hiddenUpdateCount = false;
	
	public InsertIntoValuesStatement(TableInstance table,
			List<ExpressionNode> columns,
			List<List<ExpressionNode>> values,
			List<ExpressionNode> onDupKey,
			AliasInformation aliasInfo,
			SourceLocation loc) {
		super(table,columns,onDupKey,aliasInfo, loc);
		this.values.setMultiMulti(values);
	}		
			
	public List<List<ExpressionNode>> getValues() { return values.getMultiMulti(); }
	public MultiMultiEdge<?, ExpressionNode> getValuesEdge() { return values; }
	
	public void setValues(List<List<ExpressionNode>> in) {
		values.clear();
		values.setMultiMulti(in);
	}
	
	public void setHiddenUpdateCount() {
		hiddenUpdateCount = true;
	}
	
	@Override
	public void normalize(SchemaContext sc) {
		try {
			// first, validate:
			// [I] if the number of row constructors is more than 1, make sure they all have the same number of parameters.
			// [II] if the number of columns specified is not zero, and the number of values specified is not zero, make sure they
			// have the same number of parameters.  carve out the case where all the values are specified, but the column is a wildcard.
			// also in the carveout, handle missing tenant column if in mt mode
			// then, augment:
			// [III] add missing columns (trailing), and missing values (trailing)
			// I
			int size = -1;
			for(MultiEdge<InsertIntoValuesStatement,ExpressionNode> rvc : values.getMultiEdges()) {
				if (size == -1)
					size = rvc.size();
				else if (rvc.size() != size) {
					throw new TransformException(Pass.NORMALIZE, "Differing sizes for row value constructor.  Found sizes " + size + ", " + rvc.size());
				}
			}
			List<PEColumn> columns = getTableInstance().getAbstractTable().getColumns(sc);
			int columnSize = columnSpec.size();
			boolean haveWildcard = false;
			if (columnSize == 1 && columnSpec.get(0) instanceof Wildcard)
				haveWildcard = true;
			// II
			if (!haveWildcard && columnSize > 0 && columnSize != size) {
				throw new TransformException(Pass.NORMALIZE, "Differing numbers of columns and values found.  Found " + columnSpec.size() + " columns and " + size + " values");
			}
			if (haveWildcard && size != columns.size()) {
				// can't tell what values were specified
				throw new TransformException(Pass.NORMALIZE, "Not enough values specified");
			}
			if (haveWildcard)
				// just get rid of it - the case of () and (*) is the same
				columnSpec.remove(0);
			// find the missing columns by finding what we have
			// III
			List<PEColumn> foundColumns =
				Functional.apply(columnSpec.getMulti(), new UnaryFunction<PEColumn, ExpressionNode>() {
	
					@Override
					public PEColumn evaluate(ExpressionNode object) {
						ColumnInstance cr = (ColumnInstance)object;
						return cr.getPEColumn();
					}
	
				});
	
			if (getTableInstance().getAbstractTable().isView())
				throw new SchemaException(Pass.NORMALIZE, "No support for updatable views");
			
			if (getTableInstance().getAbstractTable().asTable().hasAutoInc()) {
				sc.getValueManager().allocateAutoIncBlock(sc,getTableInstance().getTableKey());
			}
			
			cacheable = sc.getPolicyContext().isCacheableInsert(this);

			LinkedHashMap<UnqualifiedName, PEColumn> copy = new LinkedHashMap<UnqualifiedName, PEColumn>();
			for(PEColumn pec : columns)
				copy.put(pec.getName().getUnqualified(), pec);
			for(PEColumn pec : foundColumns)
				copy.remove(pec.getName().getUnqualified());
			
			// copy now contains all columns not specified in the column spec.
			// we already know that the column spec matches the values - so copy contains
			// the missing columns.  use defaults to add them to the end of each row value.
			// there is one edge case here to be aware of - if this is mt mode and the user did
			// something like insert into foo values ( a,b,c ) where they specified all the columns
			// we should just add the tenant column value
			
			// switching to a single pass through all the row constructors since we now delegate 
			// autoinc creation to the value manager for plan caching purposes.  For each row constructor:
			// for the columns that are specified, if any of them are default tokens, replace with the default
			// (either the specified default, an autoinc, or null).  For the columns that aren't specified,
			// do the same thing.  we're actually going to do this as a vector of value handlers
			
			// unfortunately for inserts into a container base table - we need to do a catalog txn to allocate the
			// new tenant - but in order to do that we need the rest of the tuple
			// fortunately for us the tenant column should be specified last
			
			SQLMode sqlMode = 
					KnownVariables.SQL_MODE.getSessionValue(sc.getConnection().getVariableSource()); 

			// we used to turn off caching here, but it turns out there is no need
			// strict_trans_tables, strict_all_tables:
			// consider a tuple (1,2,3).  once cached this will only match other literal
			// combos that are integrals, so it won't match (1,'2',3) (because RegularCachedPlan.isValid will return false).
			// for this setting, we can do some error checking.  if for a particular column it is not nullable
			// and no default value is specified - we can fail if the value is specified as null, or if it is not
			// specified at all.  both of these are structural checks (because null or lack of value is encoded in
			// the plan cache key).  likewise, if it is initially specified but then set to null (i.e. we started with
			// (1,2,3) and then saw (2,null,5) - that won't hit this plan's cache key because null is not literalized.
			// also, if we then saw (2,'null',5) - also won't hit this plan's cache key because the literal types
			// check will fail.  so - strict_trans_tables, strict_all_tables checking is completely structural.
			//
			// no_auto_on_zero:
			// when set do not allocate autoincs on the value 0, only on null.  this is handled in the autoinc block
			// so it is effectively structural as well.
			
			List<ValueHandler> handlers = new ArrayList<ValueHandler>();
			for(PEColumn pec : foundColumns) {
				if (pec.isAutoIncrement())
					handlers.add(new AutoincrementValueHandler(this,pec));
				else if (pec.isTenantColumn()) {
					handlers.add(sc.getPolicyContext().handleTenantColumnUponInsert(this, pec));
				} else 
					handlers.add(DefaultValueHandler.build(this, pec, sqlMode.isStrictMode()));
			}
			for(PEColumn pec : copy.values()) {
				ValueHandler vh = null;
				if (pec.isAutoIncrement())
					vh = new AutoincrementValueHandler(this,pec);
				else if (pec.isTenantColumn()) {
					vh = sc.getPolicyContext().handleTenantColumnUponInsert(this, pec);
				}
				else  
					vh = DefaultValueHandler.build(this,pec, sqlMode.isStrictMode());
				if (vh == null)  
					continue;
				handlers.add(vh);
				ColumnInstance ci = new ColumnInstance(pec.getName(), pec, getTableInstance());
				columnSpec.add(ci);
			}

			for(MultiEdge<InsertIntoValuesStatement,ExpressionNode> rc : values.getMultiEdges()) {
				ArrayList<ExpressionNode> trailing = new ArrayList<ExpressionNode>();
				for(int i = 0; i < handlers.size(); i++) {
					ValueHandler vh = handlers.get(i);
					if (i < rc.size()) {
						ExpressionNode current = rc.get(i);
						ExpressionNode actual = current;
						if (current instanceof VariableInstance) {
							cacheable = false;
							VariableInstance vi = (VariableInstance) current;
							AbstractVariableAccessor va = vi.buildAccessor(sc);
							try {
								String value = va.getValue(sc.getConnection().getVariableSource()); 
								actual = LiteralExpression.makeStringLiteral(value);
							} catch (PEException pe) {
								throw new SchemaException(Pass.SECOND, "Unable to sub in value for variable " + vi);
							}
						}
						ExpressionNode fixed = vh.handle(sc, sqlMode, columnSpec,rc,trailing,actual);
						if (current != fixed) {
							Edge<?,ExpressionNode> edge = current.getParentEdge();
							edge.set(fixed);
						}
					} else {
						ExpressionNode value = vh.handle(sc, sqlMode, columnSpec,rc,trailing,null);
						trailing.add(value);						
					}
				}
				if (!trailing.isEmpty())
					rc.addAll(trailing);
			}
			
		} finally {
		}
	}

	public TableInstance getPrimaryTable() {
		return intoTable.get();
	}
	
	public Long adjustUpdateCount(int in) {
		if (onDuplicateKey.size()>0 || ignore || hiddenUpdateCount)
			return null;
		return new Long(in);
	}
	

	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}
	

	public abstract static class ValueHandler {
		
		protected InsertIntoValuesStatement parent;
		protected PEColumn column;
		public ValueHandler(InsertIntoValuesStatement stmt, PEColumn col) {
			parent = stmt;
			column = col;
		}
		
		public ExpressionNode handle(SchemaContext sc, SQLMode mode, MultiEdge<InsertStatement,ExpressionNode> spec, 
				MultiEdge<InsertIntoValuesStatement,ExpressionNode> existing, 
				List<ExpressionNode> prior, ExpressionNode v) {
			if (v instanceof Default)
				return handleMissing(sc);
			else if (v == null)
				return handleMissing(sc);
			else 
				return v;
		}
		
		public abstract ExpressionNode handleMissing(SchemaContext sc);
		
	}	

	public static class AutoincrementValueHandler extends ValueHandler {
		
		public AutoincrementValueHandler(InsertIntoValuesStatement stmt, PEColumn col) {
			super(stmt,col);
		}

		@Override
		public ExpressionNode handle(SchemaContext sc, SQLMode mode, MultiEdge<InsertStatement,ExpressionNode> spec, 
				MultiEdge<InsertIntoValuesStatement,ExpressionNode> existing, 
				List<ExpressionNode> prior, ExpressionNode v) {
			if (v == null || v instanceof Default)
				return handleMissing(sc);
			else {
				ConstantExpression ce = (ConstantExpression)v;
				if (ce instanceof LiteralExpression) {
					LiteralExpression litex = (LiteralExpression) ce;
					if (AutoIncrementBlock.requiresAllocation(litex,sc, mode))
						return handleMissing(sc);
				}
				sc.getValueManager().registerSpecifiedAutoinc(sc,ce);
				return v;
			}
		}
		
		@Override
		public ExpressionNode handleMissing(SchemaContext sc) {
			return sc.getValueManager().allocateAutoInc(sc);
		}
	}
	
	public static class DefaultValueHandler extends ValueHandler {
		
		protected ExpressionNode fillInValue;
		protected boolean missingIsError;
		
		public static DefaultValueHandler build(InsertIntoValuesStatement stmt, PEColumn col, boolean strict) {
			ExpressionNode defaultValue = col.getDefaultValue();
			ExpressionNode defVal = null;
			if (defaultValue == null) {
				if (col.isAutoIncrement())
					throw new SchemaException(Pass.NORMALIZE, "Invalid insert value handler: has auto increment but using default value handler");
				if (col.isNullable())
					defVal = LiteralExpression.makeNullLiteral();
				else if (col.getType().isTimestampType())
					// omit
					return null;
				else if (!strict)
					defVal = col.getType().getZeroValueLiteral();
			} else {
				defVal = defaultValue;
			}
			return new DefaultValueHandler(stmt, col, defVal);
		}
		
		private DefaultValueHandler(InsertIntoValuesStatement stmt, PEColumn col, ExpressionNode defVal) {
			super(stmt, col);
			fillInValue = defVal;
		}
		
		@Override
		public ExpressionNode handleMissing(SchemaContext sc) {
			if (fillInValue != null)
				return (ExpressionNode) fillInValue.copy(null);
			else  
				throw new SchemaException(Pass.NORMALIZE, "No value found for required " + column.getName() + " and no default specified");
		}
		
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.INSERT;
	}

	// we don't use the same planner
	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return illegalSchemaSelf(other);
	}

	@Override
	protected int selfHashCode() {
		return illegalSchemaHash();
	}


}