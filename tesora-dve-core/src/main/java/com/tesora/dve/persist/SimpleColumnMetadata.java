package com.tesora.dve.persist;

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

// we're going for really simple here
public class SimpleColumnMetadata {

	boolean generated;
	String name;
	// if this value is populated by some other column - the other column
	SimpleColumnMetadata srcgen;
	// containing table
	SimpleTableMetadata ofTable;
	
	public SimpleColumnMetadata(String n) {
		name = n;
		generated = false;
		srcgen = null;
	}
	
	public SimpleColumnMetadata(String n, boolean gen) {
		name = n;
		generated = gen;
		srcgen = null;
	}
	
	public SimpleColumnMetadata(String n, SimpleColumnMetadata other) {
		name =n;
		generated = false;
		srcgen = other;
	}
	
	public String getName() {
		return name;
	}
	
	public boolean isGenerated() {
		return generated; 
	}
	
	public SimpleColumnMetadata getDependsOn() {
		return srcgen;
	}
	
	public void setTable(SimpleTableMetadata stm) {
		ofTable = stm;
	}
	
	public SimpleTableMetadata getTable() {
		return ofTable;
	}
}
