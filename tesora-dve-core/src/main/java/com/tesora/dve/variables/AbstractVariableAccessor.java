package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;

public abstract class AbstractVariableAccessor {

	public abstract String getValue(VariableStoreSource conn) throws PEException;

	public abstract void setValue(VariableStoreSource conn, String v) throws Throwable;

	public abstract String getSQL();

	
}
