// OS_STATUS: public
package com.tesora.dve.siteprovider;

import com.tesora.dve.worker.SiteManagerCommand;

public class DropSiteManagerCommand extends SiteManagerCommand {

	private static final long serialVersionUID = 1L;

	public DropSiteManagerCommand(SiteManagerCommand smc) {
		super(smc.getAction(), smc.getTarget(), smc.getOptions());
	}
}
