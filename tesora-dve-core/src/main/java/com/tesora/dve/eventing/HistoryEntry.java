package com.tesora.dve.eventing;

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

public class HistoryEntry {

	private final String s;
	private final int level;
	
	public HistoryEntry(String s, int level) {
		this.s = s;
		this.level = level;
	}
	
	public String toString() {
		return s;
	}
	
	private static final String indent = new String("                                                                                    ");
	
	public String getEntry() {
		return indent.substring(0, level+1) + s;
	}
	
	public int getLevel() {
		return level;
	}
}
