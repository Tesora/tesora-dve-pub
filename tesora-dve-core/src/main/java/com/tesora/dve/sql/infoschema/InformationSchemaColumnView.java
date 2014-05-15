// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class InformationSchemaColumnView extends AbstractInformationSchemaColumnView {

	protected LogicalInformationSchemaColumn backing;
	
	public InformationSchemaColumnView(InfoView view, LogicalInformationSchemaColumn basedOn,
			UnqualifiedName nameInView) {
		super(view,nameInView);
		backing = basedOn;
		if (backing == null)
			throw new InformationSchemaException("No backing logical table column for " + view + " column " + nameInView);
	}

	public InformationSchemaColumnView(InformationSchemaColumnView copy) {
		super(copy);
		backing = copy.backing;
	}
	
	@Override
	public AbstractInformationSchemaColumnView copy() {
		return new InformationSchemaColumnView(this);
	}

	@Override
	public void prepare(SchemaView ofView, InformationSchemaTableView ofTable, DBNative dbn) {
		if (backing.getReturnType() != null) {
			for(InformationSchemaTableView istv : ofView.getTables(null)) {
				if (istv.getLogicalTable() == backing.getReturnType()) {
					returnType = istv;
					break;
				}
			}
			if (returnType == null)
				throw new InformationSchemaException("No view table in view " + view + " for return type " + backing.getReturnType().getName() + ", needed for column " + getName() + " in table " + ofTable.getName());
		}
	}

	public final void freeze() {
		frozen = true;
	}
	
	
	@Override
	public Type getType() {
		return backing.getType();
	}

	@Override
	public LogicalInformationSchemaColumn getLogicalColumn() {
		return backing;
	}
		
	public UserColumn persist(CatalogDAO c, UserTable parent, DBNative dbn) {
		return backing.persist(c,parent,getName(),dbn);
	}
	
	@Override
	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int offset, List<PersistedEntity> acc) throws PEException {
		acc.add(backing.buildColumnEntity(schema, cte, offset, getName()));
	}
	
}
