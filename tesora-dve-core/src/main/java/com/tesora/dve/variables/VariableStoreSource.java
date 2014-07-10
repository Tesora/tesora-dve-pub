package com.tesora.dve.variables;

import com.tesora.dve.variable.VariableValueStore;

public interface VariableStoreSource {

	public LocalVariableStore getSessionVariableStore();
	
	public GlobalVariableStore getGlobalVariableStore();
	
	public VariableValueStore getUserVariableStore();
	
}
