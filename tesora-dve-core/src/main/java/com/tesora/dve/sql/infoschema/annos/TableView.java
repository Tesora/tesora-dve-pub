// OS_STATUS: public
package com.tesora.dve.sql.infoschema.annos;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({java.lang.annotation.ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableView {

	/*
	 * The owning infos chema view
	 */
	public InfoView view();

	/*
	 * The public name of the view.
	 */
	public String name();
	
	/*
	 * The plural name of the view.
	 */
	public String pluralName();
	
	/*
	 * Is the table an extension in the named view?
	 * Default: false
	 */
	public boolean extension() default false;
	
	/*
	 * Is the table privileged in the named view?
	 * Default: false
	 */
	public boolean priviledged() default false;
	
	/*
	 * Column order in the view
	 */
	public String[] columnOrder();
	
}
