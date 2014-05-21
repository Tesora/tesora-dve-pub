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


import java.util.Arrays;

import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

public final class TableInstanceCollector extends GeneralCollectingTraversal {

	private TableInstanceCollector() {
		super(Order.POSTORDER, ExecStyle.ONCE);
	}

	public static ListSet<TableInstance> getInstances(DMLStatement... dmls) {
		return GeneralCollectingTraversal.collect(Arrays.asList(dmls), new TableInstanceCollector());
	}

	public static ListSet<TableInstance> getInstances(ExpressionNode en) {
		return GeneralCollectingTraversal.collect(en, new TableInstanceCollector());
	}
	
	@Override
	public boolean is(LanguageNode ln) {
		return EngineConstant.TABLE.has(ln);
	}	
}
