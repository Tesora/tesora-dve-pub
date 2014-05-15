// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.UnaryFunction;

public class ShowView extends SchemaView {

	public ShowView(LogicalInformationSchema lis) {
		super(lis,InfoView.SHOW,
				new UnaryFunction<Name[], InformationSchemaTableView>() {

			@Override
			public Name[] evaluate(InformationSchemaTableView object) {
				return new Name[] { object.getName(), object.getPluralName() };
			}
			
		});

	}

	@Override
	public void buildEntities(CatalogSchema schema, int groupid, int modelid, 
			String charSet, String collation, List<PersistedEntity> acc) throws PEException {	
	}
	
	public ShowInformationSchemaTable lookupTable(UnqualifiedName unq) {
		return (ShowInformationSchemaTable)lookup.lookup(unq);
	}
}
