// OS_STATUS: public
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

import com.tesora.dve.siteprovider.DynamicSiteStatus;
import com.tesora.dve.worker.SiteManagerCommand;

public class CmdSiteManagerCommand extends SiteManagerCommand {

	private static final long serialVersionUID = 1L;

	private String command;
	private DynamicSiteInfo site;
	private DynamicSiteStatus status;
	
	public CmdSiteManagerCommand(SiteManagerCommand smc, String command) {
		super(smc.getAction(), smc.getTarget(), smc.getOptions());
		
		this.command = command;
	}
	
	public String getCommand()
	{
		return command;
	}
	
	public DynamicSiteInfo getSite() {
		return site;
	}
	
	public void setSite(DynamicSiteInfo site) {
		this.site = site;
	}
	
	public DynamicSiteStatus getStatus() {
		return status;
	}
	
	public void setStatus(DynamicSiteStatus status) {
		this.status = status;
	}
}
