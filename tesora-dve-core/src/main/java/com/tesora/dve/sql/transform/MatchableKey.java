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

import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.ListOfPairs;

public interface MatchableKey {

	public boolean isComplete(SchemaContext sc, Set<PEColumn> have, boolean partialOk);
	
	public List<PEColumn> getColumns(SchemaContext sc);

	public PlanningConstraint buildEmptyConstraint(SchemaContext sc, TableKey tk, ListOfPairs<PEColumn,ConstantExpression> values);

	public long getCardRatio(SchemaContext sc);
}
