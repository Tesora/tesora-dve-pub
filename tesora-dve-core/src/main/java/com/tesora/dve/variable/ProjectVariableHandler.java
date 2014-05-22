package com.tesora.dve.variable;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class ProjectVariableHandler extends GlobalVariableHandler implements ProjectVariableConstants {

	@Override
	public void setValue(final CatalogDAO c, final String name, final String value)
			throws PEException {

        final Project p = c.findByKey(Project.class, Singletons.require(HostService.class).getProject().getId());
		try {
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					if (PE_DEFAULT_PERSISTENT_GROUP.equalsIgnoreCase(name))
						p.setDefaultStorageGroup(c.findPersistentGroup(value));
					else if (PE_DEFAULT_DYNAMIC_POLICY.equalsIgnoreCase(name))
						p.setDefaultPolicy(c.findDynamicPolicy(value));
					else
						throw new PEException("Variable " + name + " cannot be processed by " + getClass().getSimpleName());
					return p;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Unable to set Project variable \"" + name + "\"", e);
		}
	}

	@Override
	public String getValue(CatalogDAO c, String name)
			throws PEException {
        final Project p = c.findByKey(Project.class, Singletons.require(HostService.class).getProject().getId());
		String result;
		
		if (PE_DEFAULT_PERSISTENT_GROUP.equalsIgnoreCase(name)) {
			PersistentGroup sg = p.getDefaultStorageGroup();
			result = sg == null ? "Not defined" : sg.getName();
		} else if (PE_DEFAULT_DYNAMIC_POLICY.equalsIgnoreCase(name)) {
			DynamicPolicy policy = p.getDefaultPolicy();
			result = policy == null ? "Not defined" : policy.getName();
		} else
			throw new PENotFoundException("Variable " + name + " cannot be processed by " + getClass().getSimpleName());

		return result;
	}
}
