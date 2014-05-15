// OS_STATUS: public
package com.tesora.dve.sql.infoschema.annos;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({java.lang.annotation.ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface InfoSchemaColumn {

	/*
	 * The name of the column in the logical table.
	 */
	public String logicalName();
	
	/*
	 * The field name for this column, used to look up whether this has a backing physical column.
	 * Use "" for no backing field.
	 */
	public String fieldName();
	
	/*
	 * The type of the column (as returned by a getter), as a java.sql.Type value.
	 */
	public int sqlType();

	/*
	 * The width, if applicable.
	 * Default: -1
	 */
	public int sqlWidth() default -1;

	/*
	 * The column views which expose this column.
	 */
	public ColumnView[] views();
	
	/*
	 * Should the injected columns of the return type also be put into the logical table.
	 * Default: false
	 */
	public boolean injected() default false;
	
	public String[] booleanStringTrueFalseValue() default {"YES", "NO"};

}
