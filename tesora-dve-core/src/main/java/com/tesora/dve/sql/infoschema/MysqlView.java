// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.util.UnaryFunction;

public class MysqlView extends SchemaView {

	protected MysqlView(LogicalInformationSchema lis) {
		super(lis, InfoView.MYSQL,
				new UnaryFunction<Name[], InformationSchemaTableView>() {

			@Override
			public Name[] evaluate(InformationSchemaTableView object) {
				return new Name[] { object.getName() };
			}
			
		});
	}

}
