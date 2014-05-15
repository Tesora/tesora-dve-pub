// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.show.ShowView;

public interface InformationSchemaBuilder {

	public void populate(LogicalInformationSchema logicalSchema, 
			InformationSchemaView infoSchema,
			ShowView showSchema,
			MysqlView mysqlSchema, DBNative dbn) throws PEException;
		
}
