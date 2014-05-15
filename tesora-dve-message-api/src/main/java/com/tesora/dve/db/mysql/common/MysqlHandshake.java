// OS_STATUS: public
package com.tesora.dve.db.mysql.common;

/**
 *
 */
public interface MysqlHandshake {

    int getConnectionId();

    String getSalt();

    String getPluginData();

    byte getServerCharSet();

    String getServerVersion();

    long getServerCapabilities();
}
