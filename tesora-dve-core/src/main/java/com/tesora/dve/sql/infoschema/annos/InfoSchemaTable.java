// OS_STATUS: public
package com.tesora.dve.sql.infoschema.annos;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({java.lang.annotation.ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface InfoSchemaTable {

	/*
	 * Views on the table.
	 */
	public TableView[] views();

	/*
	 * Name of the logical table.
	 */
	public String logicalName();	
}
