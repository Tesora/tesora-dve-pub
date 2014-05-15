// OS_STATUS: public
package com.tesora.dve.charset;

import java.io.Serializable;

public abstract class NativeCollation implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String name;
	private String characterSetName;
	private boolean isDefault;
	private boolean isCompiled;
	private long sortLen;
	
	public NativeCollation(int id, String name, String characterSetName, boolean isDefault, boolean isCompiled, long sortLen) {
		this.id = id;
		this.name = name;
		this.characterSetName = characterSetName;
		this.isDefault = isDefault;
		this.isCompiled = isCompiled;
		this.sortLen = sortLen;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
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
