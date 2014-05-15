// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.common.DBType;
import com.tesora.dve.exceptions.PEException;

public class MariaDBNative extends MysqlNative {

	private static final long serialVersionUID = 1L;

//	public class PEMariaDBDataSource extends MySQLDataSource implements PEDataSource {
//
//		@Override
//		public void setEncoding(String encoding) {
//		}
//
//		@Override
//		public String getEncoding() {
//			return null;
//		}
//
//	}
	
	public MariaDBNative() throws PEException {
		super();
		setDbType(DBType.MARIADB);
	}

}
