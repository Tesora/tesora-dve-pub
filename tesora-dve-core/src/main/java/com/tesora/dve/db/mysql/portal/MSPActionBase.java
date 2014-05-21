package com.tesora.dve.db.mysql.portal;

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
