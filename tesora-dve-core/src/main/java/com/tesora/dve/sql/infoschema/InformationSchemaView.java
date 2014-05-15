// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.util.UnaryFunction;

public class InformationSchemaView extends SchemaView {

	protected InformationSchemaView(LogicalInformationSchema lis) {
		super(lis,InfoView.INFORMATION, new UnaryFunction<Name[], InformationSchemaTableView>() {

			@Override
			public Name[] evaluate(InformationSchemaTableView object) {
				return new Name[] { object.getName() };
			}
			
		});
	}

}
