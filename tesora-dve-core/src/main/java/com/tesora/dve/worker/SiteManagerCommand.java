// OS_STATUS: public
package com.tesora.dve.worker;

import java.io.Serializable;

import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.sql.transform.execution.PassThroughCommand;
import com.tesora.dve.sql.util.ListOfPairs;

public class SiteManagerCommand extends PassThroughCommand<Provider, String, Object> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public SiteManagerCommand(
			com.tesora.dve.sql.transform.execution.PassThroughCommand.Command a,
			Provider target, ListOfPairs<String, Object> opts) {
		super(a, target, opts);
	}
	
}
