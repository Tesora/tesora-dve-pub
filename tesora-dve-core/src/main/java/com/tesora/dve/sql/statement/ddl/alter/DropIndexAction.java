// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class DropIndexAction extends AlterTableAction {

	PEKey key;
	
	public DropIndexAction(PEKey pek) {
		key = pek;
	}
	
	public Name getIndexName() {
		if (key.getSymbol() != null) {
			if (key.isForeign()) {
				PEForeignKey pefk = (PEForeignKey) key;
				return pefk.getPhysicalSymbol();
			} else {
				return key.getSymbol();
			}
		}
		return key.getName();
	}

	public ConstraintType getConstraintType() {
		return key.getConstraint();
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEKey peckish = key.getIn(sc, tab);
		tab.removeKey(sc, peckish);
		key = peckish;
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new DropIndexAction(key);
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		PEKey c = (PEKey) key.getIn(sc,tab);
		if (c == null)
			return "Cannot drop nonexistent key " + key.getName() + " in table " + tab.getName();
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		PEKey c = (PEKey) key.getIn(sc, tab);
		return (c == null);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.INDEX;
	}

	@Override
	public Action getActionKind() {
		return Action.DROP;
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext sc) throws PEException {
		return Collections.singletonList((CatalogEntity)key.getPersistent(sc));
	}

	@Override
	public boolean hasSQL(SchemaContext sc, PETable pet) {
		if (this.isTransientOnly())
			return true;
		PEKey c = (PEKey) key.getIn(sc, pet);
		if (c == null) return false;
		if (!c.isForeign()) return true;
		PEForeignKey pefk = (PEForeignKey) c;
		return pefk.isPersisted();
	}

}
