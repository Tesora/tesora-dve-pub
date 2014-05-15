// OS_STATUS: public
package com.tesora.dve.siteprovider;

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
