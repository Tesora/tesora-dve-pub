// OS_STATUS: public
package com.tesora.dve.variable.status;

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

import java.sql.Types;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.variable.VariableHandler;


public abstract class StatusVariableHandler extends VariableHandler {

	String defaultValue;
	String variableName;
	
	public void initialise(String name, String defaultValue) {
		this.defaultValue = defaultValue;
		this.variableName = name;
	}

	public abstract String getValue(CatalogDAO c, String name) throws PEException;

	public abstract void reset(CatalogDAO c, String name) throws PEException;
	
	public ResultCollector getValueAsResult(CatalogDAO catalogDAO, String variableName) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, getValue(catalogDAO, variableName));
	}
}
