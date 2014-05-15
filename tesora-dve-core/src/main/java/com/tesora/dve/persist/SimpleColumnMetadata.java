// OS_STATUS: public
package com.tesora.dve.persist;

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
