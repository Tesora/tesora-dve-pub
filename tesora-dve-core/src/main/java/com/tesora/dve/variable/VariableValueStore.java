// OS_STATUS: public
package com.tesora.dve.variable;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEAlreadyExistsException;
import com.tesora.dve.exceptions.PENotFoundException;

public class VariableValueStore {
	
	static Logger logger = Logger.getLogger("com.tesora.dve.variable.VariableConfig");

	Map<String /* name */, String /* value */> valueStore = new HashMap<String, String>();
	String type; // i.e., DVE, Session, User, CloudProvider
	// if false, don't check existence on operations (i.e. false for User)
	boolean checkExistence;
	
	public VariableValueStore(String type) {
		this(type,true);
	}
	
	public VariableValueStore(String type, boolean knownOnly) {
		super();
		this.type = type;
		this.checkExistence = knownOnly;
	}
	
	public void initialiseVariableWithKey(String key, String defaultValue) throws PEAlreadyExistsException {
		if (valueStore.containsKey(key))
			throw new PEAlreadyExistsException("Variable '" + key + "' already exists in variable store '" + type + "'");
		valueStore.put(key, defaultValue);
	}
	
	public void setAll(VariableValueStore source) {
		valueStore.clear();
		
		valueStore.putAll(source.valueStore);
	}
	
	public void setValue(String name, String value) throws PENotFoundException {
		String key = VariableConfig.canonicalise(name);
		validateVariableName(key);
		valueStore.put(key, value);
	}

	public String getValue(String name) throws PENotFoundException {
		String key = VariableConfig.canonicalise(name);
		validateVariableName(key);
		return valueStore.get(key);
	}
	
	public boolean hasValue(String name) {
		String key = VariableConfig.canonicalise(name);
		return valueStore.containsKey(key);
	}
	
	private void validateVariableName(String name) throws PENotFoundException {
		if (false == valueStore.containsKey(name)) {
			String notFoundMessage = "Variable '" + name + "' not found in variable store '" + type + "'";
			if (logger.isDebugEnabled())
				logger.warn(notFoundMessage);

			if (checkExistence)
				throw new PENotFoundException(notFoundMessage);
		}
	}

	public String getStoreType() {
		return type;
	}
	
	public void clear() {
		valueStore.clear();
	}	
	
	public Collection<String> getNames() {
		return valueStore.keySet();
	}

	public Map<String, String> getVariableMap() {
		return Collections.unmodifiableMap(valueStore);
	}	
}
