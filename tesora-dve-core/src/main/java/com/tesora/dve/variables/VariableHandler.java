package com.tesora.dve.variables;

import java.util.EnumSet;
import java.util.Locale;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.variable.GlobalConfig;
import com.tesora.dve.variables.VariableStore.ValueReference;

/*
 * Every nonuser variable has an associated VariableConfig record in the catalog.
 * The value in the catalog means different things depending on the scopes.  If the
 * variable has a global scope, then the catalog value is the persistent value, which 
 * is used to initialize the variable upon startup.  If the variable has no global scope,
 * then it is the default value of a session variable (also used to initialize the variable
 * upon startup, but not alterable during forward processing via set global.foo.
 * 
 * The persistent value is modified via alter dve set ....
 * 
 */

public class VariableHandler<Type> {

	// name of the variable
	private final String variableName;
	// information about the type, valid values, etc.
	private final ValueMetadata<Type> metadata;
	// valid scopes
	private final EnumSet<VariableScope> scopes;
	// if there is no record in the catalog, this is the default value
	private Type defaultOnMissing;
	// if true, the variable is for dve only, and we will never push it down
	private final boolean dveOnly;
	
	
	public VariableHandler(String name, ValueMetadata<Type> md,
			EnumSet<VariableScope> applies,
			Type defaultOnMissing,
			boolean dveOnly) {
		this.variableName = name.toLowerCase(Locale.ENGLISH);
		this.metadata = md;
		this.scopes = applies;
		this.defaultOnMissing = defaultOnMissing;
		this.dveOnly = dveOnly;
	}
	
	public String getName() {
		return variableName;
	}
	
	public ValueMetadata<Type> getMetadata() {
		return metadata;
	}

	public EnumSet<VariableScope> getScopes() {
		return scopes;
	}
	
	public Type getDefaultOnMissing() {
		return defaultOnMissing;
	}

	public boolean isDVEOnly() {
		return dveOnly;
	}
	
	public ValueReference<Type> getDefaultValueReference() {
		ValueReference<Type> out = new ValueReference<Type>(this);
		out.set(getDefaultOnMissing());
		return out;
	}
	
	public static final UnaryPredicate<VariableHandler<?>> isGlobalPredicate = new UnaryPredicate<VariableHandler<?>>() {

		@Override
		public boolean test(VariableHandler<?> object) {
			return object.getScopes().contains(VariableScope.GLOBAL);
		}
		
	};

	public static final UnaryPredicate<VariableHandler<?>> isSessionPredicate = new UnaryPredicate<VariableHandler<?>>() {

		@Override
		public boolean test(VariableHandler<?> object) {
			return object.getScopes().contains(VariableScope.SESSION);
		}
		
	};

	
	
	// for global values the SSConn is not needed - need to extract an interface for that 
	
	public Type getValue(VariableStoreSource source, VariableScope vs) {
		if (!scopes.contains(vs)) 
			throw new VariableException("Attempt to obtain unsupported scope " + vs.name() + " value from variable " + variableName);
		if (vs == VariableScope.GLOBAL) {
			// global map
			if (source == null)
				// transient side
				return GlobalVariableStore.INSTANCE.getValue(this);
			return source.getGlobalVariableStore().getValue(this);
		} else {
			// session map
			return source.getSessionVariableStore().getValue(this);
		}
	}

	// convenience - if a variable only has one scope kind - get the value
	public Type getValue(VariableStoreSource source) {
		if (scopes.size() > 1)
			throw new VariableException("No variable scope specified for variable " + getName() + " which supports " + scopes.toString());
		return getValue(source,scopes.iterator().next());
	}
	
	// maybe a little easier than saying session or global
	public Type getSessionValue(VariableStoreSource source) {
		return getValue(source,VariableScope.SESSION);
	}
	
	public Type getGlobalValue(VariableStoreSource source) {
		return getValue(source,VariableScope.GLOBAL);
	}
	
	public void setValue(VariableStoreSource conn, VariableScope scope, String newValue) throws PEException {
		if (scope == VariableScope.SESSION)
			setSessionValue(conn,newValue);
		else if (scope == VariableScope.GLOBAL)
			setGlobalValue(newValue);
		else
			throw new PEException("Unknown scope for set: " + scope);
	}
	
	public void setSessionValue(VariableStoreSource conn, String newValue) throws PEException {
		if (!scopes.contains(VariableScope.SESSION))
			throw new PECodingException("Attempt to set non existent session variable " + variableName);
		VariableStore sessionValues = conn.getSessionVariableStore(); 
		Type t = getMetadata().convertToInternal(newValue);
		sessionValues.setValue(this, t);
		onSessionValueChange(conn,t);
	}
	
	public void setGlobalValue(String newValue) throws PEException {
		if (!scopes.contains(VariableScope.GLOBAL))
			throw new PECodingException("Attempt to set non existent global variable " + variableName);
		VariableStore globalValues = null;
		Type t = getMetadata().convertToInternal(newValue);
		// the global variable store propagates the change message
		globalValues.setValue(this, t);
	}
	
	public void setPersistentValue(final CatalogDAO c, final String newValue) throws PEException {
		Type validType = getMetadata().convertToInternal(newValue);
		persistValue(c,newValue);
		// broadcast
	}
	
	protected void persistValue(final CatalogDAO c, final String newValue) throws PEException {
		try {
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					GlobalConfig configEntity = c.findConfig(getName());
					configEntity.setValue(newValue);
					return configEntity;
				}
			}.execute();
			return;
		} catch (Throwable e) {
			throw new PEException("Cannot set variable " + getName(), e);
		}		
	}
	
	public void onGlobalValueChange(Type newValue) throws PEException {
		// does nothing
	}
	
	public void onSessionValueChange(VariableStoreSource conn, Type newValue) throws PEException {
		// does nothing
	}

	public String getSessionAssignmentClause(String value) {
		if (scopes.contains(VariableScope.SESSION) && !isDVEOnly())
			return String.format("%s='%s'",getName(),value);
		return null;
	}

}
