// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.SchemaContext;

public class PEDropContainerStatement extends
		PEDropStatement<PEContainer, Container> {

	private List<CatalogEntity> updates;
	private List<CatalogEntity> deletes;
	
	public PEDropContainerStatement(PEContainer cont) {
		super(PEContainer.class, null, true, cont, "CONTAINER");
		updates = null;
		deletes = null;
	}
	
	@SuppressWarnings("unchecked")
	private void compute(SchemaContext sc) {
		if (deletes != null) return;
		try {
			PEContainer cont = getTarget().get();
			if (sc.getCatalog().findContainerMembers(cont.getName().get()).isEmpty()) {
				sc.beginSaveContext();
				try {
					deletes = new ArrayList<CatalogEntity>();
					deletes.add(cont.getPersistent(sc));
					updates = Collections.EMPTY_LIST;
				} finally {
					sc.endSaveContext();
				}
			} else {
				throw new SchemaException(Pass.PLANNER, "Unable to drop container " + cont.getName().getSQL() + " due to existing tables");
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute drop set for drop container",pe);
		}
	}
	
	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext sc) throws PEException {
		compute(sc);
		return updates;
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext sc) throws PEException {
		compute(sc);
		return deletes;
	}
	
}
