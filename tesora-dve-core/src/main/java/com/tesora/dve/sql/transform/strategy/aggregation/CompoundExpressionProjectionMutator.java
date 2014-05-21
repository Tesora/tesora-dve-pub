// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

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

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.CompoundExpressionColumnMutator;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;

public class CompoundExpressionProjectionMutator extends ProjectionMutator {

	public CompoundExpressionProjectionMutator(SchemaContext sc) {
		super(sc);
	}

	@Override
	public List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj) {
		ProjectionCharacterization pc = ProjectionCharacterization.getProjectionCharacterization(ms.getStatement());
		List<ColumnCharacterization> ccs = pc.getColumns();
		for(int i = 0; i < ccs.size(); i++) {
			ColumnCharacterization cc = ccs.get(i);
			ColumnMutator cm = null;
			if (!cc.hasAnyAggFuns() || (!cc.hasAnyNonAggFunColumns() && EngineConstant.AGGFUN.has(cc.getEntry().getTarget())))
				cm = new PassThroughMutator();
			else
				cm = new CompoundExpressionColumnMutator();
			cm.setBeforeOffset(i);
			columns.add(cm);
		}
		return applyAdapted(proj,ms);
	}
	
}
