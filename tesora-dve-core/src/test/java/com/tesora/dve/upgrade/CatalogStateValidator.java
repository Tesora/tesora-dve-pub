package com.tesora.dve.upgrade;

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

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.upgrade.CatalogVersions.CatalogVersionNumber;

// the state validator has two parts:
// we have the pre part, where we populate the before catalog
// and then theres the after part, where we validate that the upgrade
// did the right stuff
public abstract class CatalogStateValidator {

	protected final CatalogVersionNumber testedUpgrade;
	
	public CatalogStateValidator(CatalogVersionNumber version) {
		this.testedUpgrade = version;
	}
	
	public CatalogVersionNumber getVersionNumber() {
		return testedUpgrade;
	}
	
	// most population will work just fine with a list of insert stmts
	protected String[] getPopulation() {
		return new String[] {};
	}
	
	public void populate(DBHelper helper) throws Throwable {
		for(String s : getPopulation()) try {
			helper.executeQuery(s);
		} catch (Throwable t) {
			throw new Throwable("Unable to prepare population, failed to execute '" + s + "'",t);
		}
	}
	
	public abstract String validate(DBHelper helper) throws Throwable;
	
}
