// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;


import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class AddIndexAction extends AlterTableAction {

	private PEKey index;

	public AddIndexAction(PEKey ind) {
		index = ind;
	}
	
	public PEKey getNewIndex() {
		return index;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void setTarget(SchemaContext sc, PETable targ) {
		index.setTable(StructuralUtils.buildEdge(sc, targ, false));
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEKey copy = index.copy(sc, tab);
		PEKey invalidated = tab.addKey(sc, copy, true);
		if (invalidated != null) 			
			return new DropIndexAction(invalidated);
		return null;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		if (index.getName() == null) return null;
		PEKey already = index.getIn(sc,tab);
		if (already != null)
			return "Table " + tab.getName() + " already has " + already.toString(); 
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return index.getIn(sc, tab) != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		PEKey theKey = index.copy(sc, actual);
		theKey.setTable(StructuralUtils.buildEdge(sc,  actual,  false));
		return new AddIndexAction(theKey);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.INDEX;
	}

	@Override
	public Action getActionKind() {
		return Action.CREATE;
	}

}
