// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.Pair;

public class PEDropGroupProviderStatement extends PEGroupProviderDDLStatement {

	public PEDropGroupProviderStatement(PEProvider pep,
			List<Pair<Name, LiteralExpression>> opts) {
		super(pep, opts);
	}

	@Override
	public Action getAction() {
		return Action.DROP;
	}

	// set up my delete objects
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		return Collections.singletonList((CatalogEntity)getBackingProvider(pc));
	}

	@Override
	public Command getCommand() {
		return Command.DROP;
	}

}
