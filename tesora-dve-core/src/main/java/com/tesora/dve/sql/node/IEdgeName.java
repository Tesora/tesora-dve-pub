// OS_STATUS: public
package com.tesora.dve.sql.node;

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

import java.util.EnumSet;

public interface IEdgeName {

	public String getName();
	
	public boolean matches(IEdgeName in);
	
	// not applicable for offset edges
	public boolean any(EnumSet<EdgeName> set);
	
	public boolean baseMatches(IEdgeName in);
	
	public boolean isOffset();
	
	public OffsetEdgeName makeOffset(int i);
	
	public IEdgeName getBase();
	
}
