package com.tesora.dve.server.connectionmanager.log;

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

import org.apache.log4j.Logger;

/**
 *
 */
public class ShutdownLog {
    static final Logger logger = Logger.getLogger("com.tesora.dve.server.connectionmanager.BootstrapHost"); //dirty hack to break dependency cycling


    public static void logShutdownError(String message, Throwable t) {
        logger.warn(message, t);
    }

}
