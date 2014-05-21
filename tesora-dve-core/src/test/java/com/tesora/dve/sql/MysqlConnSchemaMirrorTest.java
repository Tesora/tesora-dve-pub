// OS_STATUS: public
package com.tesora.dve.sql;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
