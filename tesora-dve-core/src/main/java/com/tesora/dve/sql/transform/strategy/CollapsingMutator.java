package com.tesora.dve.sql.transform.strategy;

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
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;

// removes repeated columns from the projection - we still create a column mutator for each original column
// but those for repeats other than the first of each will point back to the first offset
public class CollapsingMutator extends ProjectionMutator {

	public CollapsingMutator(SchemaContext sc) {
		super(sc);
	}
	
	@Override
	public List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj) {
		MultiMap<RewriteKey,Integer> offsets = new MultiMap<RewriteKey,Integer>();
		for(int i = 0; i < proj.size(); i++) {
			ExpressionNode targ = ColumnMutator.getProjectionEntry(proj, i);
			offsets.put(targ.getRewriteKey(), new Integer(i));
			OffsetMutator om = new OffsetMutator();
			om.setBeforeOffset(i);
			columns.add(om);
		}
		for(RewriteKey rk : offsets.keySet()) {
			List<Integer> matching = (List<Integer>) offsets.get(rk);
			if (matching == null || matching.isEmpty()) continue;
			if (matching.size() == 1) continue;
			OffsetMutator canonVersion = (OffsetMutator) columns.get(matching.get(0));
			for(int i = 1; i < matching.size(); i++) {
				Integer oi = matching.get(i);
				OffsetMutator om = (OffsetMutator) columns.get(oi);
				om.setCanonical(canonVersion);
			}
		}
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ColumnMutator cm : columns) {
			cm.setAfterOffsetBegin(out.size());
			List<ExpressionNode> any = cm.adapt(context,proj,ms);
			if (any != null)
				out.addAll(any);
			cm.setAfterOffsetEnd(out.size());
		}
		return out;
	}


	
	private static class OffsetMutator extends ColumnMutator {

		// if null, we are the canonical version
		protected OffsetMutator canonical;
		
		public OffsetMutator() {
			super();
			canonical = null;
		}
		
		public void setCanonical(OffsetMutator canon) {
			canonical = canon;
		}
		
		@Override
		public int getAfterOffsetBegin() {
			if (canonical != null) return canonical.getAfterOffsetBegin();
			return super.getAfterOffsetBegin();
		}
		

		
		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ignored) {
			if (canonical == null) return getSingleColumn(proj,getBeforeOffset());
			return null;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption ignored) {
			ExpressionNode en = ColumnMutator.getProjectionEntry(proj, getAfterOffsetBegin());
			return Collections.singletonList((ExpressionNode)en.copy(null));
		}
		
	}

	
}
