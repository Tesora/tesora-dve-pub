// OS_STATUS: public
package com.tesora.dve.tools.field;

import java.io.PrintStream;
import java.util.Properties;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.upgrade.CatalogSchemaVersion;

public interface FieldAction {

	public String getName();
	
	public String getDescription();
	
	public boolean isValid(CatalogSchemaVersion currentVersion);
	
	public void report(Properties connectionProperties, PrintStream reportWriter) throws PEException;
	
	public void execute(Properties connectionProperties, PrintStream reportWriter) throws PEException;
	
}
