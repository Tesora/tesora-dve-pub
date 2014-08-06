package com.tesora.dve.groupmanager;

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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.variables.ServerGlobalVariableStore;
import com.tesora.dve.variables.VariableHandler;

public class OnGlobalConfigChangeMessage extends GroupMessage {

	private static final long serialVersionUID = 1L;

	static Logger logger = Logger.getLogger(OnGlobalConfigChangeMessage.class);

	final String variableName;
	final String newValue;
	final boolean newVariable;

	public OnGlobalConfigChangeMessage(String variableName, String newValue, boolean newVariable) {
		this.variableName = variableName;
		this.newValue = newValue;
		this.newVariable = newVariable;
	}

	@SuppressWarnings("rawtypes")
	@Override
	void execute(HostService hostService) {
		try {
			VariableHandler handler = Singletons.require(HostService.class).getVariableManager().lookup(variableName);
			if (handler == null) {
				if (newVariable)
					handler = Singletons.require(HostService.class).getVariableManager().postInitializationAddVariable(variableName);
			}
			if (handler == null) {
				logger.error("OnGlobalConfigChangeMessage handler not found for variable '" + variableName + "'");
				return;
			}
			ServerGlobalVariableStore.INSTANCE.invalidate(handler);
			handler.onGlobalValueChange(handler.toInternal(newValue));

			logger.info("OnGlobalConfigChangeMessage updated " + variableName + " = '" + newValue + "'");
		} catch (PEException e) {
			logger.error(
					"Exception in OnGlobalConfigChangeMessage " + variableName + " = '" + newValue + "' - "
							+ e.getMessage(), e);
		}
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}
