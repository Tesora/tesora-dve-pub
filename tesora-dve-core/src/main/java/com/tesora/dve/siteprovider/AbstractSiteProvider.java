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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.transform.execution.AdhocCatalogEntity;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.SiteManagerCommand;

public abstract class AbstractSiteProvider implements SiteProviderPlugin {

	private String providerName;
	private boolean isEnabled;
	
	protected final static String OPTION_CMD = "cmd";
	protected final static String OPTION_ENABLED = "enabled";
	protected final static String OPTION_CONFIG = "config";

	protected final static String CMD_CONFIG = "config";
	protected final static String CMD_SITES = "sites";
	
	public AbstractSiteProvider() {
	}

	protected void initialize(String name, boolean isEnabled) {
		this.providerName = name;
		this.isEnabled = isEnabled;
	}

	@Override
	public String getProviderName() {
		return providerName;
	}

	@Override
	public boolean isEnabled() {
		return isEnabled;
	}

	@Override
	public void setEnabled(boolean enabled) {
		isEnabled = enabled;
	}
	
	// ------------------------------------------------------------------------
	// SHOW Commands
	//
	protected abstract List<CatalogEntity> onShowSites(SiteManagerCommand smc) throws PEException;

	protected List<CatalogEntity> onShowConfig(SiteManagerCommand smc) throws PEException {
		return Collections.singletonList((CatalogEntity) new AdhocCatalogEntity("Configuration", smc.getTarget()
				.getConfig()));
	}

	// ------------------------------------------------------------------------
	// ALTER DYNAMIC SITE PROVIDER <provider> cmd=<something>
	//
	protected abstract CmdSiteManagerCommand onPrepareCommand(SiteManagerCommand smc, String command)
			throws PEException;

	protected abstract int onUpdateCommand(CmdSiteManagerCommand smc) throws PEException;

	protected abstract void onRollbackCommand(CmdSiteManagerCommand smc) throws PEException;

	// ------------------------------------------------------------------------
	// ALTER DYNAMIC SITE PROVIDER <provider> [config=<something>] [enabled=true|false]
	//
	protected abstract ConfigSiteManagerCommand onPrepareConfig(SiteManagerCommand smc, String enabled, String config)
			throws PEException;

	protected abstract int onUpdateConfig(ConfigSiteManagerCommand smc) throws PEException;

	protected abstract void onRollbackConfig(ConfigSiteManagerCommand smc) throws PEException;

	// ------------------------------------------------------------------------

	protected DropSiteManagerCommand onPrepareDrop(SiteManagerCommand smc) throws PEException {
		return new DropSiteManagerCommand(smc);
	}

	protected int onUpdateDrop(DropSiteManagerCommand smc) throws PEException {
		return 1;
	}

	protected void onRollbackDrop(DropSiteManagerCommand smc) throws PEException {
	}

	// ------------------------------------------------------------------------

	@Override
	public List<CatalogEntity> show(SiteManagerCommand smc) throws PEException {

		// For SHOW commands we support the following:
		// SHOW DYNAMIC SITE PROVIDER <provider> cmd='sites'
		// SHOW DYNAMIC SITE PROVIDER <provider> cmd='config'

		String command = (String) findOption(smc, OPTION_CMD);

		if (command == null)
			throw new PEException("cmd not specified");

		if (command.equalsIgnoreCase(CMD_SITES)) {
			if (smc.getTarget().isEnabled())
				return onShowSites(smc);
			else
				return Collections.emptyList();
		} else if (command.equalsIgnoreCase(CMD_CONFIG))
			return onShowConfig(smc);

		throw new PEException("'" + command + "' isn't a valid SHOW command");
	}

	@Override
	public SiteManagerCommand prepareUpdate(SiteManagerCommand smc) throws PEException {

		if (smc.getAction() == Command.ALTER) {
			// For ALTER commands we support the following:
			// ALTER DYNAMIC SITE PROVIDER <provider> enabled=<YES or NO>
			// ALTER DYNAMIC SITE PROVIDER <provider> config=<config>
			// Further commands are supported by the specific providers

			String cmd = (String) findOption(smc, OPTION_CMD);
			if (cmd != null)
				return onPrepareCommand(smc, cmd);

			String config = (String) findOption(smc, OPTION_CONFIG);
			String enabled = (String) findOption(smc, OPTION_ENABLED);
			if (enabled != null || config != null)
				return onPrepareConfig(smc, enabled, config);

			throw new PEException("Unable to parse ALTER command");

		} else if (smc.getAction() == Command.CREATE) {
			String config = (String) findOption(smc, OPTION_CONFIG);
			String enabled = (String) findOption(smc, OPTION_ENABLED);

			return onPrepareConfig(smc, enabled, config);

		} else if (smc.getAction() == Command.DROP) {
			return onPrepareDrop(smc);
		}

		throw new PEException("Unable to parse update command");
	}

	@Override
	public int update(SiteManagerCommand smc) throws PEException {

		if (smc instanceof CmdSiteManagerCommand)
			return onUpdateCommand((CmdSiteManagerCommand) smc);

		else if (smc instanceof ConfigSiteManagerCommand)
			return onUpdateConfig((ConfigSiteManagerCommand) smc);

		else if (smc instanceof DropSiteManagerCommand)
			return onUpdateDrop((DropSiteManagerCommand) smc);

		return 0;
	}

	@Override
	public void rollback(SiteManagerCommand smc) throws PEException {

		if (smc instanceof CmdSiteManagerCommand)
			onRollbackCommand((CmdSiteManagerCommand) smc);

		else if (smc instanceof ConfigSiteManagerCommand)
			onRollbackConfig((ConfigSiteManagerCommand) smc);

		else if (smc instanceof DropSiteManagerCommand)
			onRollbackDrop((DropSiteManagerCommand) smc);
	}



	protected Object findOption(SiteManagerCommand smc, String key) {
		for (Pair<String, Object> pair : smc.getOptions()) {
			if (key.equalsIgnoreCase(pair.getFirst()))
				return pair.getSecond();
		}
		return null;
	}
}
