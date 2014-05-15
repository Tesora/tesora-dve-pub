// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.log;

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
