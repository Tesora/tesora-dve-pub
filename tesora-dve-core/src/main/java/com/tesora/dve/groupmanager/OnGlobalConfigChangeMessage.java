// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.variable.GlobalVariableHandler;

public class OnGlobalConfigChangeMessage extends GroupMessage {

	private static final long serialVersionUID = 1L;

	static Logger logger = Logger.getLogger(OnGlobalConfigChangeMessage.class);

	String variableName;
	String newValue;

	public OnGlobalConfigChangeMessage(String variableName, String newValue) {
		this.variableName = variableName;
		this.newValue = newValue;
	}

	@Override
	void execute(HostService hostService) {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
            GlobalVariableHandler handler = Singletons.require(HostService.class).getGlobalVariableHandler(variableName);

			if (handler == null) {
				logger.error("OnGlobalConfigChangeMessage handler not found for variable '" + variableName + "'");
			} else {
				handler.onValueChange(variableName, newValue);

				logger.info("OnGlobalConfigChangeMessage updated " + variableName + " = '" + newValue + "'");
			}
		} catch (PEException e) {
			logger.error(
					"Exception in OnGlobalConfigChangeMessage " + variableName + " = '" + newValue + "' - "
							+ e.getMessage(), e);
		} finally {
			c.close();
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
