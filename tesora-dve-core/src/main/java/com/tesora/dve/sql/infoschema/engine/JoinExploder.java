package com.tesora.dve.sql.infoschema.engine;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;

public class JoinExploder {

	public static void explodeJoins(final SchemaContext sc, SelectStatement in, ListSet<ScopedColumnInstance> scoped) {
		MultiMap<ScopedPath,ScopedColumnInstance> origPaths = new MultiMap<ScopedPath,ScopedColumnInstance>();
		HashMap<ScopedColumnInstance,TableKey> relativeTables = new HashMap<ScopedColumnInstance,TableKey>();
		MultiMap<ScopedPath,ScopedPath> stems = new MultiMap<ScopedPath,ScopedPath>();
		HashMap<ScopedPath,TableKey> tabKeys = new HashMap<ScopedPath,TableKey>();
		for(ScopedColumnInstance sci : scoped) {
			ScopedPath sp = new ScopedPath(sci);
			origPaths.put(sp,sci);
			boolean done = false;
			while(!done) {
				if (sp.isRoot()) {
					done = true;
					// initial set
					tabKeys.put(sp,sp.getBaseTable());
				} else {
					ScopedPath relativeTo = sp.buildRelativeTo();
					stems.put(relativeTo,sp);
					sp = relativeTo;					
				}
			}
		}

		AliasInformation ai = in.getAliases();
		UnqualifiedName kern = new UnqualifiedName("jexp");
		
		// for a.b.c.d, a.b.c.e, a.b.f, stems now contains {a.b.c,[a.b.c.d,a.b.c.e]}, {a.b,[a.b.c,a.b.f]}, {a,[a.b]}
		// now we traverse the multimap looking for keys which have no entry in tabKeys.  if it has an entry in tabKeys, build
		// the join
		LinkedHashMap<ScopedPath,JoinRecord> joinOrder = new LinkedHashMap<ScopedPath,JoinRecord>();
		while(!stems.isEmpty()) {
			// make a copy in case we modify
			ArrayList<ScopedPath> keys = new ArrayList<ScopedPath>(stems.keySet());
			for(ScopedPath sp : keys) {				
				TableKey base = tabKeys.get(sp);
				if (base != null) {
					// we have the table key for this scoped path, build new table keys for each of the subs
					Collection<ScopedPath> subs = stems.get(sp);
					if (subs != null && !subs.isEmpty()) {
						for(ScopedPath dp : subs) {
							if (dp.getLeafProperty().getReturnType() != null) {
								// requires further joins - build a join record
								JoinRecord jr = new JoinRecord(sc,base,dp.getLeafProperty(),ai.buildNewAlias(kern));
								tabKeys.put(dp,jr.getRHS());
								joinOrder.put(dp, jr);
							} else {
								// this leaf is an atomic type - so record the base for it
								Collection<ScopedColumnInstance> origInstance = origPaths.get(dp);
								if (origInstance == null || origInstance.isEmpty())
									throw new IllegalStateException("lost orig scoped column instance");
								for(ScopedColumnInstance psci : origInstance)
									relativeTables.put(psci, base);
							}
						}
					}
					stems.remove(sp);
				}
			}
		}
		// now we have a join order based on scoped path, which always has a table instance in it
		// we just need to sort our existing table refs by contained table instances, and then we can walk down the
		// join order and add joins for each join record.
		HashMap<TableKey,FromTableReference> byContainedTables = new HashMap<TableKey,FromTableReference>();
		for(FromTableReference ftr : in.getTables()) {
			byContainedTables.put(ftr.getBaseTable().getTableKey(), ftr);
			for(JoinedTable jt : ftr.getTableJoins()) 
				byContainedTables.put(jt.getJoinedToTable().getTableKey(), ftr);
		}
		for(JoinRecord jr : joinOrder.values()) {
			TableKey lhs = jr.getLHS();
			FromTableReference onBranch = byContainedTables.get(lhs);
			JoinedTable jt = jr.buildJoin(sc);
			onBranch.addJoinedTable(jt);
			byContainedTables.put(jr.getRHS(), onBranch);
		}
		// finally, we can replace the scoped column instances with actual instances		
		new ScopedColumnReplacementTraversal(relativeTables).traverse(in);
	}
	
	private static class JoinRecord {
		
		TableKey lhs;
		TableKey rhs;
		LogicalInformationSchemaColumn property;
		
		public JoinRecord(SchemaContext sc, TableKey base, LogicalInformationSchemaColumn onProperty, UnqualifiedName newAlias) {
			lhs = base;
			property = onProperty;
			LogicalInformationSchemaTable nextTab = property.getReturnType();
			TableInstance nti = new TableInstance(nextTab,null,newAlias,sc.getNextTable(),false);
			rhs = new TableKey(nti);
		}
		
		public TableKey getLHS() {
			return lhs;
		}
		
		public TableKey getRHS() {
			return rhs;
		}
		
		/**
		 * @param sc
		 * @return
		 */
		public JoinedTable buildJoin(SchemaContext sc) {
			// build the join condition, i.e. lhs.property_column_name = rhs.id_column
			ColumnInstance leftColumn = new ColumnInstance(property,lhs.toInstance());
			ColumnInstance rightColumn = new ColumnInstance(property.getReturnType().getIDColumn(),rhs.toInstance());
			FunctionCall joinex = new FunctionCall(FunctionName.makeEquals(),leftColumn,rightColumn);
			// if the property is nullable, use an outer join; otherwise use an inner join.
			JoinedTable jt = new JoinedTable(rhs.toInstance(),joinex,(property.isNullable() ? JoinSpecification.LEFT_OUTER_JOIN : JoinSpecification.INNER_JOIN));
			return jt;
		}
	}
	
	
	private static class ScopedPath {
		
		private TableKey base;
		private LogicalInformationSchemaColumn[] path;
		
		private ScopedPath(TableKey tab, LogicalInformationSchemaColumn[] computedPath) {
			base = tab;
			path = computedPath;
		}
		
		public ScopedPath(ScopedColumnInstance sci) {
			LinkedList<LogicalInformationSchemaColumn> pathBuf = new LinkedList<LogicalInformationSchemaColumn>();
			ColumnInstance p = sci;
			while(p != null) {
				pathBuf.addFirst((LogicalInformationSchemaColumn)p.getColumn());
				if (p instanceof ScopedColumnInstance) {
					ScopedColumnInstance psci = (ScopedColumnInstance) p;
					p = psci.getRelativeTo();
				} else {
					base = p.getTableInstance().getTableKey();
					p = null;
				}
			}
			path = pathBuf.toArray(new LogicalInformationSchemaColumn[0]);
		}

		public TableKey getBaseTable() { return base; }
		
		public boolean isRoot() {
			return path.length == 0;
		}

		public LogicalInformationSchemaColumn getLeafProperty() {
			if (isRoot()) return null;
			return path[path.length - 1];
		}
		
		public ScopedPath buildRelativeTo() {
			if (path.length <= 1)
				return new ScopedPath(base,new LogicalInformationSchemaColumn[] {});
			LogicalInformationSchemaColumn[] newPath = new LogicalInformationSchemaColumn[path.length - 1];
			System.arraycopy(path,0,newPath,0,path.length - 1);
			return new ScopedPath(base,newPath);
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((base == null) ? 0 : base.hashCode());
			result = prime * result + Arrays.hashCode(path);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ScopedPath other = (ScopedPath) obj;
			if (base == null) {
				if (other.base != null)
					return false;
			} else if (!base.equals(other.base))
				return false;
			if (!Arrays.equals(path, other.path))
				return false;
			return true;
		}
		
		
	}
	
	private static class ScopedColumnReplacementTraversal extends Traversal {

		private Map<ScopedColumnInstance, TableKey> forwarding;
		
		public ScopedColumnReplacementTraversal(Map<ScopedColumnInstance,TableKey> f) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			forwarding = f;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof ScopedColumnInstance) {
				ScopedColumnInstance sci = (ScopedColumnInstance) in;
				TableKey any = forwarding.get(sci);
				if (any == null) throw new IllegalStateException("Missing mapping for scoped column instance " + sci);
				return new ColumnInstance(sci.getColumn(),any.toInstance());
			}
			return in;
		}
		
	}
	

}
