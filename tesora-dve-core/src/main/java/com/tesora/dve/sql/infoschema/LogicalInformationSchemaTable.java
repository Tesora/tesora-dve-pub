package com.tesora.dve.sql.infoschema;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;


public class LogicalInformationSchemaTable implements Table<LogicalInformationSchemaColumn>{

	protected List<LogicalInformationSchemaColumn> columns;
	protected Lookup<LogicalInformationSchemaColumn> lookup;

	protected LogicalInformationSchemaColumn idcol;

	protected List<LogicalInformationSchemaColumn> injected;
	
	protected UnqualifiedName name;

	protected boolean frozen;
	
	public LogicalInformationSchemaTable(UnqualifiedName name) {
		columns = new ArrayList<LogicalInformationSchemaColumn>();
		lookup = new Lookup<LogicalInformationSchemaColumn>(columns,new UnaryFunction<Name[], LogicalInformationSchemaColumn>() {

			@Override
			public Name[] evaluate(LogicalInformationSchemaColumn object) {
				return new Name[] { object.getName() };
			}
			
		}, false, false);
		frozen = false;
		idcol = null;
		injected = new ArrayList<LogicalInformationSchemaColumn>();
		this.name = name;
	}
	
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		for(LogicalInformationSchemaColumn isc : columns)
			isc.prepare(schema, dbn);
	}

	protected void collectInjected(List<LogicalInformationSchemaColumn> currentPath, List<List<LogicalInformationSchemaColumn>> acc) {
		for(LogicalInformationSchemaColumn isc : injected) {
			if (isc.getReturnType() == null) continue;
			List<LogicalInformationSchemaColumn> path = new ArrayList<LogicalInformationSchemaColumn>(currentPath);
			path.add(isc);
			isc.getReturnType().collectInjected(path, acc);
			acc.add(path);
		}
	}
	
	
	/**
	 * @param schema
	 */
	protected void inject(LogicalInformationSchema schema) {
		ArrayList<LogicalInformationSchemaColumn> currentPath = new ArrayList<LogicalInformationSchemaColumn>();
		List<List<LogicalInformationSchemaColumn>> acc = new ArrayList<List<LogicalInformationSchemaColumn>>();
		collectInjected(currentPath, acc);
		for(List<LogicalInformationSchemaColumn> p : acc) {
			// the injected column is already present if any path contains only that column.
			if (p.size() == 1) continue;
			DelegatingInformationSchemaColumn delegate = new DelegatingInformationSchemaColumn(p,p.get(p.size() - 1).getName().getUnqualified());
			addColumn(null,delegate);
		}
	}
	
	protected final void freeze() {
		for(LogicalInformationSchemaColumn isc : columns)
			isc.freeze();
		frozen = true;
	}
	
	@Override
	public LogicalInformationSchemaColumn addColumn(SchemaContext sc, LogicalInformationSchemaColumn c) {
		LogicalInformationSchemaColumn already = lookup(sc,c.getName());
		if (already != null)
			throw new InformationSchemaException("Column " + c.getName() + " already exists in " + getName());
		c.setPosition(columns.size());
		columns.add(c);
		lookup.refreshBacking(columns);
		c.setTable(this);
		if (c.isID())
			idcol = c;
		if (c.isInjected())
			injected.add(c);
		return c;
	}

	public LogicalInformationSchemaColumn addExplicitColumn(String rawName, Type t) {
		return addColumn(null, new LogicalInformationSchemaColumn(new UnqualifiedName(rawName),t));
	}
	
	@Override
	public Name getName() {
		return name;
	}
	
	@Override
	public Name getName(SchemaContext sc) {
		return getName();
	}
	
	@Override
	public LogicalInformationSchemaColumn lookup(SchemaContext sc, Name n) {
		return lookup.lookup(n);
	}

	public LogicalInformationSchemaColumn lookup(String s) {
		LogicalInformationSchemaColumn lisc = lookup(null, new UnqualifiedName(s));
		if (lisc == null)
			throw new InformationSchemaException("Unable to find column " + s + " in logical table " + getName());
		return lisc;
	}
	
	@Override
	public List<LogicalInformationSchemaColumn> getColumns(SchemaContext sc) {
		return columns;
	}
	
	public Class<?> getEntityClass() {
		return null;
	}
	
	public String getEntityName() {
		return null;
	}
	
	public String getTableName() {
		return null;
	}
	
	public LogicalInformationSchemaColumn getNameColumn() {
		return null;
	}
	
	/**
	 * This is for synthetic access, override if has ident/orderBy column but
	 * no name column (this may be unnecessary, but getNameColumn is used by
	 * more than just synthetic access)
	 * 
	 * @return
	 */
	public LogicalInformationSchemaColumn getIdentOrderByColumn() {
		return getNameColumn();
	}
	
	// some tables require raw execution because they don't exist as entities
	public boolean requiresRawExecution() {
		return false;
	}
	
	// a layered table is actually a composite - it needs further rewrites before we can convert it
	public boolean isLayered() {
		return false;
	}
	
	/**
	 * @param sc
	 * @param lq
	 * @return
	 */
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		return lq;
	}
	
	@Override
	public boolean isInfoSchema() {
		return true;
	}

	@Override
	public boolean isTempTable() {
		return false;
	}
		
	public LogicalInformationSchemaColumn getIDColumn() {
		if (idcol == null)
			throw new InformationSchemaException("Logical table " + getName() + " has no id column");
		return idcol;
	}
	
	public static final UnaryFunction<Name[], LogicalInformationSchemaTable> getNameFunc = new UnaryFunction<Name[], LogicalInformationSchemaTable>() {

		@Override
		public Name[] evaluate(LogicalInformationSchemaTable object) {
			return new Name[] { object.getName() };
		}
		
	};
	
	public static ExpressionNode makeEquijoin(TableInstance leftTab, LogicalInformationSchemaColumn leftCol, 
			TableInstance rightTab, LogicalInformationSchemaColumn rightCol) {
		ColumnInstance lci = new ColumnInstance(leftCol,leftTab);
		ColumnInstance rci = new ColumnInstance(rightCol,rightTab);
		return new FunctionCall(FunctionName.makeEquals(),lci,rci);
	}
	
	public static class ColumnReplacementTraversal extends Traversal {

		Map<LogicalInformationSchemaTable, TableInstance> forwarding;
		
		public static void replace(LanguageNode in, Map<LogicalInformationSchemaTable,TableInstance> forwardTo) {
			new ColumnReplacementTraversal(forwardTo).traverse(in);
		}
		
		
		private ColumnReplacementTraversal(Map<LogicalInformationSchemaTable,TableInstance> forwardTo) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			forwarding = forwardTo;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				// some columns will already be converted (because we already rewrote the where clause), so check for that
				if (ci.getColumn() instanceof DelegatingInformationSchemaColumn) {
					DelegatingInformationSchemaColumn isc = (DelegatingInformationSchemaColumn) ci.getColumn();
					LogicalInformationSchemaTable ofTable = isc.getActualTable();
					TableInstance ti = forwarding.get(ofTable);
					if (ti != null)
						// guard against multiple such tables
						return isc.rewriteToActual(ti);
				}
			}
			return in;
		}
		
	}

	public static class ComplexReplacementTraversal extends Traversal {
	
		private LogicalInformationSchemaColumn lookFor;
		private ExpressionNode replaceWith;
		
		public static void replace(LanguageNode in, LogicalInformationSchemaColumn col, ExpressionNode repl) {
			new ComplexReplacementTraversal(col,repl).traverse(in);
		}
		
		private ComplexReplacementTraversal(LogicalInformationSchemaColumn col, ExpressionNode repl) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			lookFor = col;
			replaceWith = repl;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				if (ci.getColumn() == lookFor) {
					return (LanguageNode) replaceWith.copy(null);
				}
			}
			return in;
		}
		
	}

	public static class SyntheticReplacementTraversal extends Traversal {
		
		private final Set<SyntheticLogicalInformationSchemaColumn> synthetics;
		
		public SyntheticReplacementTraversal(Set<SyntheticLogicalInformationSchemaColumn> synthetics) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.synthetics = synthetics;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				for(SyntheticLogicalInformationSchemaColumn s : synthetics) {
					if (s.matches(ci))
						return s.explode(ci);
				}
			}
			return in;
		}
		
	}
	
	// looks for references to the lookFor column, and using the the coalesceWith column, build
	// coalesce(coalesceWith, lookFor)
	public static class BuildCoalesceColumnTraversal extends Traversal {
	
		private LogicalInformationSchemaColumn lookFor;
		private ColumnInstance coalesceWith;
		private boolean existingLeading;
		
		public BuildCoalesceColumnTraversal(LogicalInformationSchemaColumn lookFor, ColumnInstance coalesceWith, boolean preferExisting) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.lookFor = lookFor;
			this.coalesceWith = coalesceWith;
			this.existingLeading = preferExisting;
		}
		
		public BuildCoalesceColumnTraversal(LogicalInformationSchemaColumn lookFor, ColumnInstance coalesceWith) {
			this(lookFor, coalesceWith, false);
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				if (ci.getColumn() == lookFor) {
					List<ExpressionNode> params = new ArrayList<ExpressionNode>();
					ExpressionNode existingCopy = (ExpressionNode)ci.copy(null);
					ExpressionNode withCopy = (ExpressionNode)coalesceWith.copy(null);
					if (existingLeading) {
						params.add(existingCopy);
						params.add(withCopy);
					} else {
						params.add(withCopy);
						params.add(existingCopy);
					}
					FunctionCall fc = new FunctionCall(FunctionName.makeCoalesce(), params);
					return fc;
				}
			}
			return in;
		}
		
		
	}

	public static class PathExtender extends Traversal {
	
		private TableInstance match;
		private ColumnInstance extendBy;
		
		public PathExtender(TableInstance m, ColumnInstance e) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			match = m;
			extendBy = e;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof ColumnInstance) {
				return map((ColumnInstance)in);
			}
			return in;
		}
		
		private ColumnInstance map(ColumnInstance ci) {
			if (ci instanceof ScopedColumnInstance) {
				ScopedColumnInstance sci = (ScopedColumnInstance) ci;
				ColumnInstance extended = map(sci.getRelativeTo());
				if (extended != sci.getRelativeTo())
					return new ScopedColumnInstance(sci.getColumn(),extended);
				return sci;
			}
			if (ci.getTableInstance().getTableKey().equals(match.getTableKey())) {
				ColumnInstance eb = (ColumnInstance) extendBy.copy(null);
				return new ScopedColumnInstance(ci.getColumn(),eb);
			}
			return ci;
		}
	}

	public static class NameRefCollector extends GeneralCollectingTraversal {
	
		private LogicalInformationSchemaColumn nameCol;
		
		public NameRefCollector(LogicalInformationSchemaColumn nc) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			nameCol = nc;
		}
		
		
		@Override
		public boolean is(LanguageNode ln) {
			if (ln instanceof ScopedColumnInstance) {
				ScopedColumnInstance sci = (ScopedColumnInstance) ln;
				if (sci.getColumn() == nameCol)
					return true;
			} else if (ln instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) ln;
				if (ci.getColumn() == nameCol)
					return true;
			}
			return false;
		}
		
		public static ListSet<ColumnInstance> collect(LanguageNode in, LogicalInformationSchemaColumn nc) {
			return GeneralCollectingTraversal.collect(in, new NameRefCollector(nc));
		}
		
	}

	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return sc.getCurrentDatabase();
	}

	public static Type buildStringType(DBNative dbn, int width) {
		return BasicType.buildType(java.sql.Types.VARCHAR, width, dbn);
	}
	
	protected static Type buildClobType(DBNative dbn, int width) {
		return BasicType.buildType(java.sql.Types.LONGVARCHAR, width, dbn);
	}
	
	protected static Type buildLongType(DBNative dbn) {
		return BasicType.buildType(java.sql.Types.BIGINT, 0, dbn);
	}
	
	protected static Type buildDateTimeType(DBNative dbn) {
		return BasicType.buildType(java.sql.Types.TIMESTAMP, 0, dbn);
	}
}
