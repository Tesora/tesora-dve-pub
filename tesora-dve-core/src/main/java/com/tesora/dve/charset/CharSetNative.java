package com.tesora.dve.charset;

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
