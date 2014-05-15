// OS_STATUS: public
package com.tesora.dve.db.mysql.common;

import java.nio.charset.Charset;

/**
 *
 */
public interface JavaCharsetCatalog {
    Charset findJavaCharsetById(int clientCharsetId);
}
