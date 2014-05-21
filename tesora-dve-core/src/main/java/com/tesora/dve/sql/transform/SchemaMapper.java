// OS_STATUS: public
package com.tesora.dve.sql.transform;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

public class SchemaMapper {

	protected CopyContext copyContext;
	
	protected ListSet<DMLStatement> orig = new ListSet<DMLStatement>();
	protected DMLStatement copy;
	
	// construct a mapper with the given originals, copy, and copy context
	public SchemaMapper(Collection<DMLStatement> originals, DMLStatement copyStatement, CopyContext cc) {
		orig.addAll(originals);
		copy = copyStatement;
		copyContext = cc;
		copyContext.setFixed(true);
	}
	
	public CopyContext getCopyContext() {
		return copyContext;
	}
	
	public ListSet<DMLStatement> getOriginals() {
		return orig;
	}
	
	// make sure we share a source eventually
	public boolean hasSameRoot(SchemaMapper other) {
		HashSet<DMLStatement> ts = new HashSet<DMLStatement>();
		HashSet<DMLStatement> os = new HashSet<DMLStatement>();
		collectStatements(ts);
		other.collectStatements(os);
		ts.retainAll(os);
		return !ts.isEmpty();
	}
	
	protected void collectStatements(Set<DMLStatement> into) {
		into.addAll(orig);
		for(DMLStatement dmls : orig) {
			SchemaMapper sm = dmls.getMapper();
			if (sm != null) sm.collectStatements(into);
		}
	}

	public void remove(PETable pet) {
		copyContext.removeTable(pet);
		for(DMLStatement source : orig) {
			SchemaMapper mapper = source.getMapper();
			if (mapper == null) continue;
			mapper.remove(pet);
		}
	}
	
	public void remove(TableKey tt) {
		copyContext.removeTable(tt);
		// the goal is to make the table invisible - remove it from elsewhere too
		for(DMLStatement source : orig) {
			SchemaMapper mapper = source.getMapper();
			if (mapper == null) continue;
			mapper.remove(tt);
		}
	}
	
	// we have only a few actual operations we can do - given a current statement and a source statement,
	// and an expression within that source, we can produce an expression using the current statement table/column
	// instances

	public ColumnKey copyColumnKeyForward(ColumnKey in) {
		ColumnKey out = copyContext.getColumnKey(in);
		if (out != null) return out;
		for(DMLStatement source : orig) {
			SchemaMapper mapper = source.getMapper();
			if (mapper == null) continue;
			out = mapper.copyColumnKeyForward(in);
			if (out == null) continue;
			out = copyContext.getColumnKey(out);
			if (out == null) continue;
			return out;
		}
		return null;
	}
	
	public TableKey copyTableKeyForward(TableKey in) {
		TableKey out = copyContext.getTableKey(in);
		if (out != null) return out;
		for(DMLStatement source : orig) {
			SchemaMapper mapper = source.getMapper();
			if (mapper == null) continue;
			out = mapper.copyTableKeyForward(in);
			if (out == null) continue;
			out = copyContext.getTableKey(out);
			if (out == null) continue;
			return out;
		}
		return null;
	}

	public ColumnKey mapExpressionToColumn(ExpressionNode in) {
		if (in instanceof ColumnInstance)
			return copyColumnKeyForward(((ColumnInstance)in).getColumnKey());
		// special mapper - we have a noncolumn input, which we are going to search backwards through contexts
		// for a match and then map forward as a column key
		ExpressionKey ek = new ExpressionKey(in);
		return mapExpression(ek);
	}
	
	public boolean isOf(ExpressionNode in) {
		ListSet<ColumnInstance> cis = ColumnInstanceCollector.getColumnInstances(in);
		ListSet<TableKey> ofTables = new ListSet<TableKey>();
		for(ColumnInstance ci : cis) {
			ofTables.add(ci.getColumnKey().getTableKey());
		}
		// we're of this mapper if all of our table keys are amongst mapped to tables in the copy context
		return copyContext.getMappedToTables().containsAll(ofTables);
	}
	
	private ColumnKey mapExpression(ExpressionKey ek) {
		ColumnKey ck = copyContext.getColumnKey(ek);
		if (ck != null) return ck;
		for(DMLStatement source : orig) {
			SchemaMapper mapper = source.getMapper();
			if (mapper == null) continue;
			ck = mapper.mapExpression(ek);
			if (ck == null) continue;
			ck = copyContext.getColumnKey((ColumnKey)ck);
			if (ck == null) continue;
			return ck;
		}
		return null;
	}
	
	public <T extends LanguageNode> T copyForward(T in) {
		UpdatingContext uc = new UpdatingContext(this);
		return CopyVisitor.copy(in, uc);
	}
	
	public <T extends LanguageNode> List<T> copyForward(List<T> in) throws PEException {
		ArrayList<T> out = new ArrayList<T>();
		for(T c : in)
			out.add(copyForward(c));
		return out;
	}

	// we have our own copy context, which is an updating context - it does the search backwards
	private static class UpdatingContext extends CopyContext {
	
		private final SchemaMapper last;
		
		public UpdatingContext(SchemaMapper l) {
			super("UpdatingContext");
			last = l;
		}

		@Override
		public TableKey getTableKey(TableKey tk) {
			return last.copyTableKeyForward(tk);
		}
		
		@Override
		public ColumnKey getColumnKey(ColumnKey ck) {
			ColumnKey out = last.copyColumnKeyForward(ck);
			if (out == null && last.getCopyContext().isTargetTable(ck.getTableKey()))
				return ck;
			return out;
		}
		
		@Override
		public TableInstance getTableInstance(TableInstance in) {
			TableKey out = last.copyTableKeyForward(in.getTableKey());
			if (out == null) 
				throw new SchemaException(Pass.REWRITER, "Unable to map table " + in + " forward");
			return out.toInstance();
		}
		
		@Override
		public ColumnInstance getColumnInstance(ColumnInstance in) {
			ColumnKey ick = in.getColumnKey();
			ColumnKey out = getColumnKey(ick); 
			if (out == null) {
//				System.out.println("Can't find: " + in.getColumnKey());
//				System.out.println(last.dumpChain(""));
//				getColumnKey(ick);
				throw new SchemaException(Pass.REWRITER, "Unable to map column " + in + " forward");
			}
			return out.toInstance();
		}

		@Override
		public boolean isFixed() {
			// always true
			return true;
		}
		
	}
	
	public String dumpChain(String offset) {
		StringBuffer buf = new StringBuffer();
		buf.append(offset).append("c: ").append(copy.toString()).append(PEConstants.LINE_SEPARATOR);
		int oc = 0;
		for(DMLStatement dmls : orig) {
			buf.append(offset).append("o[").append(oc).append("]: ").append(dmls.toString()).append(PEConstants.LINE_SEPARATOR);
			oc++;
			if (dmls.getMapper() != null)
				buf.append(dmls.getMapper().dumpChain(offset + "  "));
		}
		return buf.toString();
	}
	
}
