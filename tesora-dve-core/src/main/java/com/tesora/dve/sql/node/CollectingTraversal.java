// OS_STATUS: public
package com.tesora.dve.sql.node;

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

import com.tesora.dve.sql.node.test.NodeTest;

public class CollectingTraversal extends GeneralCollectingTraversal {

	private final NodeTest<LanguageNode> t;
	
	public CollectingTraversal(Order order, NodeTest<LanguageNode> test) {
		super(order,ExecStyle.ONCE);
		t = test;
	}
	
	@Override
	public boolean is(LanguageNode ln) {
		if (t.has(ln))
			return true;
		return false;
	}

}
