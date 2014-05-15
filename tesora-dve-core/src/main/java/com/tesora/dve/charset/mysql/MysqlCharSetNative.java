// OS_STATUS: public
package com.tesora.dve.charset.mysql;

import com.tesora.dve.charset.CharSetNative;
import com.tesora.dve.exceptions.PEException;

public class MysqlCharSetNative extends CharSetNative {
    
	private static final long serialVersionUID = 1L;

	public MysqlCharSetNative() throws PEException {
		this(new MysqlNativeCharSetCatalog());
	}

	protected MysqlCharSetNative(MysqlNativeCharSetCatalog tc) throws PEException {
		setCharSetCatalog(tc);
	}

}
