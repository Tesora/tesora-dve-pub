package com.tesora.dve.mysqlapi.repl.messages;

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

import com.tesora.dve.exceptions.PEException;

/**
 *
 */
public interface ReplicationVisitorEvent {
    /**
     *  Utility call that allows an event to redispatch itself to the appropriate type-specific method in ReplicationVisitorTarget.
     *  Decoded messages should not perform any business logic in this method, since that would couples the message to
     *  specific application behavior, preventing reuse in alternate applications.
     *
     * @param visitorTarget
     * @throws PEException
     */
    void accept(ReplicationVisitorTarget visitorTarget) throws PEException;
}
