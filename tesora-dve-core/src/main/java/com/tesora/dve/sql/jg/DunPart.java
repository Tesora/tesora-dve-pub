package com.tesora.dve.sql.jg;

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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class DunPart extends DPart {

	protected ListSet<TableKey> table;

	public DunPart(TableKey tk, int id) {
		super(id);
		table = new ListSet<TableKey>();
		table.add(tk);
	}
	
	@Override
	public boolean isUnary() {
		return true;
	}

	@Override
	public ListSet<TableKey> getTables() {
		return table;
	}

	@Override
	public String getGraphRole() {
		return "UnaryPartition";
	}

	@Override
	public List<JoinEdge> getEmbeddedJoins() {
		return Collections.emptyList();
	}

	@Override
	public List<DunPart> getUnaryParts() {
		return Collections.singletonList(this);
	}
	
	@Override
	public DistributionVector getGoverningVector(SchemaContext sc, TableKey forTable) {
		return forTable.getAbstractTable().getDistributionVector(sc);
	}

}
