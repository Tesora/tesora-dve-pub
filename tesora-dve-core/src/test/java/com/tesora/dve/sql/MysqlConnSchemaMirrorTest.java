// OS_STATUS: public
package com.tesora.dve.sql;

import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MysqlConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;

public class MysqlConnSchemaMirrorTest extends SchemaMirrorTest {

	boolean useUTF8 = true;
	
	public void setUseUTF8(boolean useUTF8) {
		this.useUTF8 = useUTF8;
	}

	// For tests that are dependent on the MysqlConnectionResource implementation
	@Override
	protected ConnectionResource createConnection(ProjectDDL p)	throws Throwable {
		ConnectionResource cr = null;
		if (p == getNativeDDL()) 
			return new DBHelperConnectionResource(useUTF8); 
		if (p == getMultiDDL() || p == getSingleDDL())
			cr = new MysqlConnectionResource();

		return cr;
	}
}
