package com.tesora.dve.sql.infoschema.annos;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
