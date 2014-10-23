package com.tesora.dve.charset;

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

import java.io.Serializable;

public abstract class NativeCollation implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private long collationId;
	private String name;
	private String characterSetName;
	private boolean isDefault;
	private boolean isCompiled;
	private long sortLen;
	
	public NativeCollation(long collationId, String name, String characterSetName, boolean isDefault, boolean isCompiled, long sortLen) {
		this.collationId = collationId;
		this.name = name;
		this.characterSetName = characterSetName;
		this.isDefault = isDefault;
		this.isCompiled = isCompiled;
		this.sortLen = sortLen;
	}

	public long getId() {
		return collationId;
	}

	public void setId(long id) {
		this.collationId = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCharacterSetName() {
		return characterSetName;
	}

	public void setCharacterSetName(String characterSetName) {
		this.characterSetName = characterSetName;
	}

	public boolean isDefault() {
		return isDefault;
	}

	public void setDefault(boolean isDefault) {
		this.isDefault = isDefault;
	}

	public boolean isCompiled() {
		return isCompiled;
	}

	public void setCompiled(boolean isCompiled) {
		this.isCompiled = isCompiled;
	}

	public long getSortLen() {
		return sortLen;
	}

	public void setSortLen(long sortLen) {
		this.sortLen = sortLen;
	}

}
