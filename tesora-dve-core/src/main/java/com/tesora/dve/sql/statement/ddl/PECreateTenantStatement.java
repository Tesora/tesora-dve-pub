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

import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.statement.StatementType;

public class PECreateTenantStatement extends PECreateStatement<PETenant, Tenant> {

	private final StatementType logicalStatementType;
	
	public PECreateTenantStatement(Persistable<PETenant, Tenant> targ,  boolean exists, StatementType stmtType) {
		this(targ, null, exists, stmtType);
	}
	
	public PECreateTenantStatement(Persistable<PETenant, Tenant> targ, Boolean ine, boolean exists, StatementType stmtType) {
		super(targ,true,ine, "TENANT",exists);
		logicalStatementType = stmtType;
	}
	
	@Override
	public StatementType getStatementType() {
		return logicalStatementType;
	}

}
