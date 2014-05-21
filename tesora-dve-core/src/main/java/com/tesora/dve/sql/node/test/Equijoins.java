package com.tesora.dve.sql.node.test;

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

import com.tesora.dve.sql.node.AbstractTraversal.ExecStyle;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class Equijoins extends DerivedAttribute<ListSet<FunctionCall>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return true;
	}

	@Override
	public ListSet<FunctionCall> computeValue(final SchemaContext sc, LanguageNode ln) {
		return GeneralCollectingTraversal.collect(ln, new GeneralCollectingTraversal(Order.PREORDER,ExecStyle.ONCE) {

			@Override
			public boolean is(LanguageNode ln) {
				Boolean value = ln.getDerivedAttribute(EngineConstant.EQUIJOIN, sc);
				if (value == null) return false;
				return value.booleanValue();
			}
		});
	}

}
