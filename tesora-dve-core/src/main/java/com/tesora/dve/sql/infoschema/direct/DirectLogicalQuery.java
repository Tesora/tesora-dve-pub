package com.tesora.dve.sql.infoschema.direct;

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

import java.util.Map;

import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class DirectLogicalQuery extends LogicalQuery {

	public DirectLogicalQuery(ViewQuery orig, SelectStatement xlated,
			Map<String, Object> p) {
		super(orig, xlated, p);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean isDirect() {
		return true;
	}

}
