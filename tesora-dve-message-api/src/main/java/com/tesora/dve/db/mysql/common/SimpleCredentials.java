// OS_STATUS: public
package com.tesora.dve.db.mysql.common;

/**
 *
 */
public interface SimpleCredentials {
    String getName();

    String getPassword();

    boolean isCleartext();
}
