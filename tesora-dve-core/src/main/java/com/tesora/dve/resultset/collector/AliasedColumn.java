// OS_STATUS: public
package com.tesora.dve.resultset.collector;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;


public class AliasedColumn extends UserColumn {

	private static final long serialVersionUID = 1L;
	
	protected String aliasName;
	
	public AliasedColumn() {
		super();
	}
	
	public AliasedColumn(UserColumn uc, String alias) {
		super(uc);
		aliasName = alias;
	}

	public AliasedColumn(ColumnMetadata cm) throws PEException {
		super(cm);
		aliasName = cm.getAliasName();
	}

	public AliasedColumn(String name, int sQLtype, String nativeType) {
		super(name, sQLtype, nativeType);
		aliasName = name;
	}

	@Override
	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	
	@Override
	public String getQueryName() {
		return getAliasName();
	}
}
