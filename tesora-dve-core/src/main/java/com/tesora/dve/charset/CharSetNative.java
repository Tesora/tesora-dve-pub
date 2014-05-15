// OS_STATUS: public
package com.tesora.dve.charset;

import java.io.Serializable;

import com.tesora.dve.charset.mysql.MysqlCharSetNative;
import com.tesora.dve.charset.standalone.StandaloneMysqlCharSetNative;
import com.tesora.dve.common.DBType;
import com.tesora.dve.exceptions.PEException;

public abstract class CharSetNative implements Serializable {

	private static final long serialVersionUID = 1L;

	private NativeCharSetCatalog charSetCatalog;

	public static class CharSetNativeFactory {
		public static CharSetNative newInstance(DBType dbType) throws PEException {
			return newInstance(dbType, true);
		}

		/**
		 * haveCatalog should be set to true if there is a catalog available to get the list of charsets
		 * 
		 * @param dbType
		 * @param haveCatalog
		 * @return
		 * @throws PEException
		 */
		public static CharSetNative newInstance(DBType dbType, boolean haveCatalog) throws PEException {
			CharSetNative charSetNative = null;

			switch (dbType) {
			case MYSQL:
			case MARIADB:
				charSetNative = haveCatalog ? new MysqlCharSetNative() : new StandaloneMysqlCharSetNative();
				break;
			default:
				throw new PEException("Attempt to create new character set native instance using invalid database type " + dbType);
			}
			return charSetNative;
		}

	}

	public NativeCharSetCatalog getCharSetCatalog() {
		return charSetCatalog;
	}

	public void setCharSetCatalog(NativeCharSetCatalog tc) throws PEException {
		this.charSetCatalog = tc;
		this.charSetCatalog.load();
	}

}
