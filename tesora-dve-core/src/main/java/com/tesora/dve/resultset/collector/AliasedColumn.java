// OS_STATUS: public
package com.tesora.dve.resultset.collector;

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

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;


public class AliasedColumn extends UserColumn {

	private static final long serialVersionUID = 1L;
	
	protected String aliasName;
	
	public AliasedColumn() {
		super();
	}
	
	public AliasedColumn(UserColumn uc, String alias) {
		super(uc);
		aliasName = alias;
	}

	public AliasedColumn(ColumnMetadata cm) throws PEException {
		super(cm);
		aliasName = cm.getAliasName();
	}

	public AliasedColumn(String name, int sQLtype, String nativeType) {
		super(name, sQLtype, nativeType);
		aliasName = name;
	}

	@Override
	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	
	@Override
	public String getQueryName() {
		return getAliasName();
	}
}
