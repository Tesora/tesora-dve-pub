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
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.SchemaException;
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

	public static final String NULL_VALUE = new String("NULL");
		
	// name of the variable
	private final String variableName;
	// information about the type, valid values, etc.
	private final ValueMetadata<Type> metadata;
	// valid scopes
	private final EnumSet<VariableScopeKind> scopes;
	// the compiled in default
	private final Type compiledDefaultOnMissing;
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
		this.compiledDefaultOnMissing = defaultOnMissing;
		this.options = options;
		String actualHelp = help;
		if (actualHelp == null && options.contains(VariableOption.EMULATED))
			actualHelp = "emulated";
		else if (actualHelp == null)
			actualHelp = "";
		this.help = actualHelp;
	}
	
	public String getName() {
		return variableName;
	}
	
	public String toString() {
		return variableName;
	}
	
	public ValueMetadata<Type> getMetadata() {
		return metadata;
	}

	public EnumSet<VariableScopeKind> getScopes() {
		return scopes;
	}

	public EnumSet<VariableOption> getOptions() {
		return options;
	}
	
	public String getDescription() {
		return help;
	}
	
	public Type getDefaultOnMissing() {
		return compiledDefaultOnMissing;
	}

	public boolean isDVEOnly() {
		return !options.contains(VariableOption.EMULATED);
	}

	public boolean isEmulatedPassthrough() {
		return options.contains(VariableOption.EMULATED) && options.contains(VariableOption.PASSTHROUGH);
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
			setGlobalValue(conn,newValue);
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
	
	public void setGlobalValue(VariableStoreSource conn, String newValue) throws PEException {
		if (options.contains(VariableOption.READONLY))
			throw new PEException(String.format("Unable to set readonly variable '%s'",getName()));
		if (!scopes.contains(VariableScopeKind.GLOBAL))
			throw new PECodingException("Attempt to set non existent global variable " + variableName);
		Type t =  toInternal(newValue);
		// push it down first, in case there's a problem
		pushdownGlobalValue(conn,t);
		// the global variable store propagates the change message
		ServerGlobalVariableStore.INSTANCE.setValue(this, t);
	}
	
	public void pushdownGlobalValue(VariableStoreSource conn, Type t) throws PEException {
		// after broadcasting, set the global value
		if (conn instanceof SSConnection) {
			SSConnection ssConnection = (SSConnection) conn;
			String anything = getGlobalAssignmentClause(t);
			if (anything != null)
				ssConnection.updateGlobalVariableState(String.format("set %s",anything));
		}			
	}
	
	public void setPersistentValue(VariableStoreSource conn, final String newValue) throws PEException {
		// validate
		Type t = toInternal(newValue);
		if (conn instanceof SSConnection) {
			CatalogDAO c = ((SSConnection)conn).getCatalogDAO();
			persistValue(c,newValue);
		}
		// broadcast; note that we skip access checking here, because persistent set always updates
		// the global map.  (i.e. setting the pers value for a session variable should effect new sesssions)
		ServerGlobalVariableStore.INSTANCE.setValue(this, t);
		pushdownGlobalValue(conn,t);
	}
	
	public void persistValue(final CatalogDAO c, final String newValue) throws PEException {
		try {
			c.begin();
			VariableConfig vc = c.findVariableConfig(getName());
			vc.setValue(newValue);
			c.commit();
		} catch (Throwable e) {
			throw new PEException("Cannot set variable " + getName(), e);
		}		
	}
	
	// this method is used to update dve state on the various servers
	// any underlying mysql state would have been handled by the originating dve server
	public void onGlobalValueChange(Type newValue) throws PEException {
		// nothing by default
	}
	
	
	public void onSessionValueChange(VariableStoreSource conn, Type newValue) throws PEException {
		if (isEmulatedPassthrough()) {
			if (conn instanceof SSConnection) {
				SSConnection ssCon = (SSConnection) conn;
				ssCon.updateWorkerState();
			}
		}
	}

	public String getSessionAssignmentClause(String value) {
		if (scopes.contains(VariableScopeKind.SESSION) && isEmulatedPassthrough()) 
			return String.format("%s=%s",getName(),value);
		return null;
	}

	public String getGlobalAssignmentClause(Type value) {
		if (scopes.contains(VariableScopeKind.GLOBAL) && isEmulatedPassthrough())
			return String.format("@@global.%s = %s",getName(),toExternal(value));
		return null;
	}
	
	public VariableConfig lookupPersistentConfig(CatalogDAO c) throws PEException {
		return c.findVariableConfig(getName(), true);
	}
	
	public VariableConfig buildNewConfig() {
		// String name, String valueType, String value, String scopes, boolean emulated, String helpText) {
		return new VariableConfig(getName(),
				getMetadata().getTypeName(),
				toRow(getDefaultOnMissing()),
				convertScopes(getScopes()),
				convertOptions(getOptions()),
				getDescription());
	}
	
	public static String convertScopes(EnumSet<VariableScopeKind> scopes) {
		return Functional.join(scopes, ",", new UnaryFunction<String,VariableScopeKind>() {

			@Override
			public String evaluate(VariableScopeKind object) {
				return object.name();
			}
			
		});
	}
	
	public static EnumSet<VariableScopeKind> convertScopes(String in) {
		String[] bits = in.split(",");
		EnumSet<VariableScopeKind> out = EnumSet.noneOf(VariableScopeKind.class);
		for(String s : bits) {
			out.add(VariableScopeKind.valueOf(s.trim()));
		}
		return out;
	}

	public static String convertOptions(EnumSet<VariableOption> options) {
		return Functional.join(options, ",", new UnaryFunction<String,VariableOption>() {

			@Override
			public String evaluate(VariableOption object) {
				return object.name();
			}
			
		});
		
	}
	
	public static EnumSet<VariableOption> convertOptions(String in) {
		String[] bits = in.split(",");
		EnumSet<VariableOption> out = EnumSet.noneOf(VariableOption.class);
		for(String s : bits)
			out.add(VariableOption.valueOf(s.trim()));
		return out;
	}
	
	// for catalog helper
	public VariableConfig initialiseCatalog(CatalogDAO c) throws PEException {
		VariableConfig vc = buildNewConfig(); 
		c.persistToCatalog(vc);
		return vc;
	}
	
	public Type initialise(CatalogDAO c) throws PEException {
		VariableConfig conf = c.findVariableConfig(getName(),false);
		Type out = null;
		if (conf != null) {
			out = toInternal(conf.getValue());
			if (scopes.contains(VariableScopeKind.GLOBAL)) {
				// check with the global version too.  we're going to go directly to the global variable store
				ValueReference<Type> existing = ServerGlobalVariableStore.INSTANCE.getReference(this);
				if (existing != null) {
					out = existing.get();
				}
			}
		} else {
			conf = buildNewConfig();
			out = getDefaultOnMissing();
			try {
				c.begin();
				c.persistToCatalog(conf);
				c.commit();
			} catch (Throwable t) {
				c.rollback(t);
				throw new PEException("Unable to initialise catalog for variable '" + getName() + "'");
			}		
		}
		return out;
	}

	public String toExternal(Type in) {
		return getMetadata().convertToExternal(in);
	}
	
	public String toRow(Type in) {
		if (in == null) return NULL_VALUE;
		return getMetadata().toRow(in);
	}
	
	public Type toInternal(String in) throws PEException {
		if (in == null || NULL_VALUE.equals(in)) {
			if (options.contains(VariableOption.NULLABLE)) 
				return null;
			throw new SchemaException(new ErrorInfo(AvailableErrors.WRONG_VALUE_FOR_VARIABLE, this.getName(), "NULL"));
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
