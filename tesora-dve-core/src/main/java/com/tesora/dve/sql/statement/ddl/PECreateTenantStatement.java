// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

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
