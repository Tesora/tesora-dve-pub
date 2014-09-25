package com.tesora.dve.sql.infoschema.direct;

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

public class DirectColumnGenerator extends DirectSchemaGenerator {

	private final String name;
	private final String type;
	
	private int orderby = -1;
	private boolean ident = false;
	private boolean full = false;
	
	public DirectColumnGenerator(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}
	
	public DirectColumnGenerator withOrderBy(int offset) {
		this.orderby= offset;
		return this;
	}
	
	public DirectColumnGenerator withIdent() {
		this.ident = true;
		return this;
	}
	
	public DirectColumnGenerator withFull() {
		this.full = true;
		return this;
	}
	
	public DirectColumnGenerator withExtension() {
		return super.withExtension();
	}
	
	public String getName() {
		return name;
	}
	
	public String getType() {
		return type;
	}
	
	public boolean isIdent() {
		return ident;
	}
	
	public int getOrderByOffset() {
		return orderby;
	}
	
	public boolean isFull() {
		return full;
	}
}
