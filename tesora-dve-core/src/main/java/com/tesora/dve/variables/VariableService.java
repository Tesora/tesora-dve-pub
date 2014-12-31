package com.tesora.dve.variables;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.variables.ScopedVariables;
import com.tesora.dve.variables.VariableManager;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface VariableService {
    Collection<String> getScopedVariableScopeNames();

    Map<String, String> getScopedVariables(String scopeName) throws PEException;

    void setScopedVariable(String scopeName, String variableName, String value) throws PEException;

    void addScopedConfig(String scopeName, ScopedVariables config);

    VariableManager getVariableManager();
}
