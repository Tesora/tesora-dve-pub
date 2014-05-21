// OS_STATUS: public
package com.tesora.dve.infomessage;

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

public class InformationMessage {

	private final Level level;
	private final String message;
	private final int code;
	
	public InformationMessage(Level l, String m) {
		level = l;
		message = m;
		code = 9999;
	}
	
	public boolean atLeast(Level l) {
		return level.ordinal() >= l.ordinal();
	}
	
	public boolean only(Level l) {
		return level == l;
	}
	
	public int getCode() {
		return code;
	}
	
	public String getMessage() {
		return message;
	}	
	
	public Level getLevel() {
		return level;
	}
}
