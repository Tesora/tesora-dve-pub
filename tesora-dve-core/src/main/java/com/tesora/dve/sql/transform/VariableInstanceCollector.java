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

import java.util.List;

import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;

public class VariableInstanceCollector extends GeneralCollectingTraversal {

	public VariableInstanceCollector() {
		super(Order.POSTORDER, ExecStyle.ONCE);
	}
	
	@Override
	public boolean is(LanguageNode ln) {
		return EngineConstant.VARIABLE.has(ln);
	}

	public static List<VariableInstance> getVariables(LanguageNode ln) {
		return GeneralCollectingTraversal.collect(ln, new VariableInstanceCollector());
	}
	
}