// OS_STATUS: public
package com.tesora.dve.sql.infoschema.mysql;

import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class MysqlDBInformationSchemaTable extends InformationSchemaTableView {

	public MysqlDBInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.MYSQL, basedOn, new UnqualifiedName("db"), null, false, false);
		orderByColumn = new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("db"), new UnqualifiedName("Db")); 
		addColumn(null,new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("host"), new UnqualifiedName("Host")));
		addColumn(null,orderByColumn);
		addColumn(null,new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("user"), new UnqualifiedName("User")));
	}


	@Override
	protected void validate(SchemaView ofView) {
	}

}
