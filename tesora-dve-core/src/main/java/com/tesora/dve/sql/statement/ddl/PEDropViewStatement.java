// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;

public class PEDropViewStatement extends PEDropStatement<PEViewTable,UserTable> {

	protected List<CatalogEntity> ents = null;
	
	public PEDropViewStatement(PEViewTable theView, Boolean ifExists) {
		super(PEViewTable.class, ifExists, false, theView, "VIEW");
	}
	
	public PEDropViewStatement(Name targ, Boolean ifExists) {
		super(PEViewTable.class, ifExists, false, targ, "VIEW");
	}

	@Override
	public PEDatabase getDatabase(SchemaContext pc) {
		if (getRoot() != null)
			return ((PEViewTable)getRoot()).getPEDatabase(pc);
		return super.getDatabase(pc);
	}

	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		if (ents == null) {
			pc.beginSaveContext();
			try {
				getRoot().persistTree(pc);
				ents = Functional.toList(pc.getSaveContext().getObjects());
			} finally {
				pc.endSaveContext();
			}
		}
		return ents;
	}

}
