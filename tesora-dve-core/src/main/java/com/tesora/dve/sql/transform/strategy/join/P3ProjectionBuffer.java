// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

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

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;

class P3ProjectionBuffer extends FinalBuffer {

	// map from P2 entries to P3 entries
	// there may be multiple p2 entries for a single p3 entry
	private HashMap<BufferEntry, BufferEntry> forwarding;
	
	public P3ProjectionBuffer(Buffer bef, PartitionLookup pl) {
		super(BufferKind.P3, bef,pl);
		forwarding = new HashMap<BufferEntry, BufferEntry>();
	}

	@Override
	public void adapt(SchemaContext sc, SelectStatement stmt) {
		HashMap<RewriteKey, BufferEntry> firstCols = new HashMap<RewriteKey,BufferEntry>();
		HashMap<RewriteKey, BufferEntry> firstCompounds = new HashMap<RewriteKey,BufferEntry>();
		P2ProjectionBuffer p2proj = (P2ProjectionBuffer) getBuffer(BufferKind.P2);
		for(BufferEntry be : p2proj.getEntries()) {
			ExpressionNode t = be.getTarget();
			RewriteKey rk = t.getRewriteKey();
			boolean compoundUse = false;
			if (t instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) t;
				compoundUse = ci.getColumn().getType().isFloatType() && be.hasCompoundExpressionDependent();
			}
			Map<RewriteKey,BufferEntry> version = (compoundUse ? firstCompounds : firstCols);
			BufferEntry firstBE = version.get(rk);
			if (firstBE == null) {
				firstBE = new BufferEntry(t);
				add(firstBE);
				partitionInfo.add(firstBE, ColumnInstanceCollector.getTableKeys(t));
				version.put(rk, firstBE);
			}
			be.addDependency(firstBE);
			forwarding.put(be,firstBE);
		}	
	}	
	
	public BufferEntry getP3ForP2(BufferEntry be) {
		return forwarding.get(be);
	}
}