// OS_STATUS: public
package com.tesora.dve.distribution;

import com.tesora.dve.exceptions.PEException;

public class PELockedException extends PEException {

	private static final long serialVersionUID = -6668674306540804836L;

	protected PELockedException() {
		super();
	}
	
	public PELockedException(String m) {
		super(m);
	}

}
