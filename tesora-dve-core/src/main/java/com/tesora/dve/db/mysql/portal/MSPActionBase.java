// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MSPActionBase implements MSPAction {
    static Logger log = LoggerFactory.getLogger(MSPActionBase.class);

    protected <P> P castProtocolMessage(Class<P> clazz, MSPMessage protocolMessage) throws IllegalArgumentException {
        if (!clazz.isInstance(protocolMessage)){
            String errorMessage = String.format("%s handler received unexpected protocol message of type %s",
                    this.getClass().getSimpleName(),
                    (protocolMessage == null? null : protocolMessage.getClass().getSimpleName())
            );

            throw new IllegalArgumentException(errorMessage);
        }

        return clazz.cast(protocolMessage);
    }

}
