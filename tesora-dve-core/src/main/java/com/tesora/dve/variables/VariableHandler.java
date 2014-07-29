package com.tesora.dve.variables;

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

import java.util.EnumSet;
import java.util.Locale;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

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

	public static final String NULL_VALUE = new String("null");
		
	// name of the variable
	private final String variableName;
	// information about the type, valid values, etc.
	private final ValueMetadata<Type> metadata;
	// valid scopes
	private final EnumSet<VariableScopeKind> scopes;
	// if there is no record in the catalog, this is the default value
	private Type defaultOnMissing;
	// options
	private final EnumSet<VariableOption> options;
	// help
	private final String help;
	
	public VariableHandler(String name, ValueMetadata<Type> md,
			EnumSet<VariableScopeKind> applies,
			Type defaultOnMissing,
			EnumSet<VariableOption> options) {
		this(name,md,applies,defaultOnMissing,options,null);
	}	
	
	
	public VariableHandler(String name, ValueMetadata<Type> md,
			EnumSet<VariableScopeKind> applies,
			Type defaultOnMissing,
			EnumSet<VariableOption> options,
			String help) {
		this.variableName = name.toLowerCase(Locale.ENGLISH);
		this.metadata = md;
		this.scopes = applies;
		this.defaultOnMissing = defaultOnMissing;
		this.options = options;
		this.help = help;
	}
	
	public String getName() {
		return variableName;
	}
	
	public ValueMetadata<Type> getMetadata() {
		return metadata;
	}

	public EnumSet<VariableScopeKind> getScopes() {
		return scopes;
	}
	
	public Type getDefaultOnMissing() {
		return defaultOnMissing;
	}

	public boolean isDVEOnly() {
		return options.contains(VariableOption.DVE_ONLY);
	}
	
	public ValueReference<Type> getDefaultValueReference() {
		ValueReference<Type> out = new ValueReference<Type>(this);
		out.set(getDefaultOnMissing());
		return out;
	}
	
	public static final UnaryPredicate<VariableHandler<?>> isGlobalPredicate = new UnaryPredicate<VariableHandler<?>>() {

		@Override
		public boolean test(VariableHandler<?> object) {
			return object.getScopes().contains(VariableScopeKind.GLOBAL);
		}
		
	};

	public static final UnaryPredicate<VariableHandler<?>> isSessionPredicate = new UnaryPredicate<VariableHandler<?>>() {

		@Override
		public boolean test(VariableHandler<?> object) {
			return object.getScopes().contains(VariableScopeKind.SESSION);
		}
		
	};

	
	
	// for global values the SSConn is not needed - need to extract an interface for that 
	
	public Type getValue(VariableStoreSource source, VariableScopeKind vs) {
		if (!scopes.contains(vs)) 
			throw new VariableException("Attempt to obtain unsupported scope " + vs.name() + " value from variable " + variableName);
		if (vs == VariableScopeKind.GLOBAL) {
			// global map.  if the global map is the transient map, use that instead (for the tests)
			if (source == null || source.getGlobalVariableStore().isServer())
				return ServerGlobalVariableStore.INSTANCE.getValue(this);
			else
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
		return getValue(source,VariableScopeKind.SESSION);
	}
	
	public Type getGlobalValue(VariableStoreSource source) {
		return getValue(source,VariableScopeKind.GLOBAL);
	}
	
	public void setValue(VariableStoreSource conn, VariableScopeKind scope, String newValue) throws PEException {
		if (scope == VariableScopeKind.SESSION)
			setSessionValue(conn,newValue);
		else if (scope == VariableScopeKind.GLOBAL)
			setGlobalValue(newValue);
		else
			throw new PEException("Unknown scope for set: " + scope);
	}
	
	public void setSessionValue(VariableStoreSource conn, String newValue) throws PEException {
		if (options.contains(VariableOption.READONLY))
			throw new PEException(String.format("Variable '%s' not settable as session variable",getName()));
		if (!scopes.contains(VariableScopeKind.SESSION))
			throw new PECodingException("Attempt to set non existent session variable " + variableName);
		AbstractVariableStore sessionValues = conn.getSessionVariableStore(); 
		Type t = toInternal(newValue);
		sessionValues.setValue(this, t);
		onSessionValueChange(conn,t);
	}
	
	public void setGlobalValue(String newValue) throws PEException {
		if (options.contains(VariableOption.READONLY))
			throw new PEException(String.format("Unable to set readonly variable '%s'",getName()));
		if (!scopes.contains(VariableScopeKind.GLOBAL))
			throw new PECodingException("Attempt to set non existent global variable " + variableName);
		Type t =  toInternal(newValue);
		// the global variable store propagates the change message
		ServerGlobalVariableStore.INSTANCE.setValue(this, t);
	}
	
	public void setPersistentValue(final CatalogDAO c, final String newValue) throws PEException {
		Type validType = toInternal(newValue);
		persistValue(c,newValue);
		// broadcast
		setGlobalValue(newValue);
	}
	
	protected void persistValue(final CatalogDAO c, final String newValue) throws PEException {
		try {
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					VariableConfig vc = c.findVariableConfig(getName());
					vc.setValue(newValue);
					return vc;
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
		if (!isDVEOnly()) {
			if (conn instanceof SSConnection) {
				SSConnection ssCon = (SSConnection) conn;
				ssCon.updateWorkerState(getSessionAssignmentClause(toExternal(newValue)));
			}
		}
	}

	public String getSessionAssignmentClause(String value) {
		if (scopes.contains(VariableScopeKind.SESSION) && !isDVEOnly() && !options.contains(VariableOption.NO_SESSION_ASSIGNMENT))
			return String.format("%s=%s",getName(),value);
		return null;
	}

	public VariableConfig buildNewConfig() {
		// String name, String valueType, String value, String scopes, boolean emulated, String helpText) {
		return new VariableConfig(getName(),
				getMetadata().getTypeName(),
				toRow(getDefaultOnMissing()),
				convert(getScopes()),
				!isDVEOnly(),
				null);
	}
	
	public static String convert(EnumSet<VariableScopeKind> scopes) {
		return Functional.join(scopes, ",", new UnaryFunction<String,VariableScopeKind>() {

			@Override
			public String evaluate(VariableScopeKind object) {
				return object.name();
			}
			
		});
	}
	
	public static EnumSet<VariableScopeKind> convert(String in) {
		String[] bits = in.split(",");
		EnumSet<VariableScopeKind> out = EnumSet.noneOf(VariableScopeKind.class);
		for(String s : bits) {
			out.add(VariableScopeKind.valueOf(s));
		}
		return out;
	}
	
	public void initialise(CatalogDAO c) throws PEException {
		VariableConfig conf = c.findVariableConfig(getName(),false);
		if (conf != null) {
			defaultOnMissing = toInternal(conf.getValue());
			if (scopes.contains(VariableScopeKind.GLOBAL)) {
				// check with the global version too.  we're going to go directly to the global variable store
				ValueReference<Type> existing = ServerGlobalVariableStore.INSTANCE.getReference(this);
				if (existing != null) {
					defaultOnMissing = existing.get();
				}
			}
		} else {
			conf = buildNewConfig();
			try {
				c.begin();
				c.persistToCatalog(conf);
				c.commit();
			} catch (Throwable t) {
				c.rollback(t);
				throw new PEException("Unable to initialise catalog for variable '" + getName() + "'");
			}
		}
	}

	public String toExternal(Type in) {
		return getMetadata().convertToExternal(in);
	}
	
	public String toRow(Type in) {
		if (in == null) return "";
		return getMetadata().toRow(in);
	}
	
	public Type toInternal(String in) throws PEException {
		if (in == null || NULL_VALUE.equals(in)) {
			if (options.contains(VariableOption.NULLABLE)) 
				return null;
			throw new PEException(String.format("Invalid value for variable '%s': null not allowed",getName()));			
		}
		return getMetadata().convertToInternal(getName(),in);
	}

	// the map does not support nulls
	public String toMap(Type in) {
		if (in == null)
			return NULL_VALUE;
		return toExternal(in);
	}
	
}
