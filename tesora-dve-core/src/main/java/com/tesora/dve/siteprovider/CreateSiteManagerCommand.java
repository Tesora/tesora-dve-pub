// OS_STATUS: public
package com.tesora.dve.siteprovider;

import com.tesora.dve.worker.SiteManagerCommand;

public class CreateSiteManagerCommand extends SiteManagerCommand {

	private static final long serialVersionUID = 1L;

	private String enabled;
	private String config;

	public CreateSiteManagerCommand(SiteManagerCommand smc, String enabled, String config) {
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
