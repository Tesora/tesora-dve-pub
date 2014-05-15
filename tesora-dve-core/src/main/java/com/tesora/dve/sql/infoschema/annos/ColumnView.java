// OS_STATUS: public
package com.tesora.dve.sql.infoschema.annos;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({java.lang.annotation.ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnView {

	/*
	 * The view this column is for
	 */
	public InfoView view();
	
	/*
	 * The name of this column in this view
	 */
	public String name();
	
	/*
	 * Is this column an extension?
	 * @default false
	 */
	public boolean extension() default false;
	
	/*
	 * Does this column require privileges in this view?
	 * @default false
	 */
	public boolean priviledged() default false;
	
	/*
	 * Is this column the order by column in this view?
	 * @default false
	 */
	public boolean orderBy() default false;
	
	/*
	 * Is this the ident column for this table in this view?
	 * @default false
	 */
	public boolean ident() default false;
	
	/*
	 * Is this column visible for this table in this view?
	 * @default true
	 */
	public boolean visible() default true;
	
	/*
	 * Is this column an injected column in this view?
	 * @default false
	 */
	public boolean injected() default false;
}
