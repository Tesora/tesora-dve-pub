package com.tesora.dve.siteprovider;

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

import com.tesora.dve.worker.SiteManagerCommand;

public class ConfigSiteManagerCommand extends SiteManagerCommand {

	private static final long serialVersionUID = 1L;

	private String enabled;
	private String config;

	public ConfigSiteManagerCommand(SiteManagerCommand smc, String enabled, String config) {
		super(smc.getAction(), smc.getTarget(), smc.getOptions());

		this.enabled = enabled;
		this.config = config;
	}

	public String getConfig() {
		return config;
	}

	public String getEnabled() {
		return enabled;
	}
}
