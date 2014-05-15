// OS_STATUS: public
package com.tesora.dve.db;

import javax.sql.XADataSource;

public interface PEDataSource extends XADataSource {
	
	void setEncoding(String encoding);

	String getEncoding();

}
