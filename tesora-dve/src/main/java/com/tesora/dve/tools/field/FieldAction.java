package com.tesora.dve.tools.field;

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

import java.io.PrintStream;
import java.util.Properties;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.upgrade.CatalogSchemaVersion;

public interface FieldAction {

	public String getName();
	
	public String getDescription();
	
	public boolean isValid(CatalogSchemaVersion currentVersion);
	
	public void report(Properties connectionProperties, PrintStream reportWriter) throws PEException;
	
	public void execute(Properties connectionProperties, PrintStream reportWriter) throws PEException;
	
}
