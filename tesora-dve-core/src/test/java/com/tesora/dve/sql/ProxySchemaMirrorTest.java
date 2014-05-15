// OS_STATUS: public
package com.tesora.dve.sql;

import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;

public abstract class ProxySchemaMirrorTest extends SchemaMirrorTest {

	// For tests that are dependent on the ProxyConnectionResource implementation
	@Override
	protected ConnectionResource createConnection(ProjectDDL p) throws Throwable {
		ConnectionResource cr = super.createConnection(p);
		if (p == getMultiDDL() || p == getSingleDDL()) 
			cr = new ProxyConnectionResource();
		
		return cr;
	}
}
