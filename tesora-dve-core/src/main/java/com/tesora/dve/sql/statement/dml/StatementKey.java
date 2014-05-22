package com.tesora.dve.sql.statement.dml;

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

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.LanguageNode;

public class StatementKey extends RewriteKey {

	private final DMLStatement stmt;
	
	public StatementKey(DMLStatement dmls) {
		super();
		this.stmt = dmls;
	}
	
	@Override
	protected int computeHashCode() {
		return stmt.getSchemaHashCode();
	}

	@Override
	public LanguageNode toInstance() {
		return stmt;
	}

}
