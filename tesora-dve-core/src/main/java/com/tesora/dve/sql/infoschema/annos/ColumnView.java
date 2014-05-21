// OS_STATUS: public
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
