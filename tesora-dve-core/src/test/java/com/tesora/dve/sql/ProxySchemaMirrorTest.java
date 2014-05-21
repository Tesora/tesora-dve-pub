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
