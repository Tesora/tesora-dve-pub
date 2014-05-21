package com.tesora.dve.db.mysql.libmy;

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

import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PEException;

public class MyParameter {

	MyFieldType type;
	Object value;
	
	MyParameter(MyFieldType type, Object value) {
		this.type = type;
		this.value = value;
	}

	public MyParameter(MyFieldType type) {
		this.type = type;
		this.value = null;
	}

	public MyFieldType getType() {
		return type;
	}

	public void setType(MyFieldType type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	public String getValueForQuery() throws PEException {
		return DBTypeBasedUtils.getMysqlTypeFunc(type).getParamReplacement(value,true);
	}
}
