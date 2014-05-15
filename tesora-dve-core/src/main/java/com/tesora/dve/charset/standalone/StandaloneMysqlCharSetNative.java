// OS_STATUS: public
package com.tesora.dve.charset.standalone;

import com.tesora.dve.charset.mysql.MysqlCharSetNative;
import com.tesora.dve.exceptions.PEException;

public class StandaloneMysqlCharSetNative extends MysqlCharSetNative {

	private static final long serialVersionUID = 1L;

	public StandaloneMysqlCharSetNative() throws PEException {
		this(new StandaloneMysqlNativeCharSetCatalog());
	}

	protected StandaloneMysqlCharSetNative(StandaloneMysqlNativeCharSetCatalog tc) throws PEException {
		super(tc);
	}

}
