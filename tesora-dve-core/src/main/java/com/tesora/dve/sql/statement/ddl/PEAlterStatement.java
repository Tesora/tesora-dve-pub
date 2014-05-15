// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;


import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;

public abstract class PEAlterStatement<S extends Persistable<?,?>> extends AlterStatement {

	protected S backing;
	
	public PEAlterStatement(S target, boolean peOnly) {
		super(peOnly);
		backing = target;
	}

	public S getTarget() {
		return backing;
	}
	
	public void setTarget(S nt) {
		backing = nt;
	}
	
	protected abstract S modify(SchemaContext pc, S backing) throws PEException;

	public List<CatalogEntity> getCatalogEntries(SchemaContext pc) throws PEException {
		pc.beginSaveContext(true);
		try {
			backing.persistTree(pc,true);
			backing = modify(pc,backing);
		} finally {
			pc.endSaveContext();
		}
		pc.beginSaveContext(true);
		try {
			backing.persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}
		
	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return getCatalogEntries(pc);
	}

	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return getTarget();
	}

}
