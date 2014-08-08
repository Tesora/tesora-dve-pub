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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.Functional;

// mostly just a helper class
public class VariableManager {

	private static VariableManager instance = null;
	
	// single point of initialization, has some side effects
	public static VariableManager getManager() throws PEException {
		VariableManager vm = instance;
		if (vm == null) synchronized(VariableManager.class) {
			if (instance == null) {
				instance = new VariableManager();
			}
			vm = instance;
		}
		return vm;
	}
	
	// not final because we could add
	private LinkedHashMap<String,VariableHandler<?>> handlers;
	
	private VariableManager() throws PEException {
		handlers = new LinkedHashMap<String,VariableHandler<?>>();
		for(VariableHandler<?> vh : KnownVariables.namedHandlers)
			addHandler(vh);
		for(VariableHandler<?> vh : KnownVariables.unnamedHandlers)
			addHandler(vh);
	}

	public void addHandler(VariableHandler<?> vh) throws PEException {
		VariableHandler<?> before = handlers.put(normalize(vh.getName()),vh);
		if (before != null)
			throw new PEException("Duplicate handler for variable '" + vh.getName() + "'");
	}
	
	// used in the upgrade
	public static String normalize(String in) {
		return in.toLowerCase(Locale.ENGLISH);
	}
	
	
	public List<VariableHandler<?>> getSessionHandlers() {
		return Functional.select(handlers.values(), VariableHandler.isSessionPredicate);
	}
	
	public List<VariableHandler<?>> getGlobalHandlers() {
		return Functional.select(handlers.values(), VariableHandler.isGlobalPredicate);
	}
	
	public List<VariableHandler<?>> getAllHandlers() {
		return Functional.toList(handlers.values());
	}
	
	public VariableHandler<?> lookupMustExist(String name) throws PEException {
		VariableHandler<?> vh = handlers.get(normalize(name));
		if (vh == null)
			throw new PENotFoundException(String.format("No such variable: '%s'", name));
		return vh;		
	}
	
	public VariableHandler<?> lookup(String name) {
		return handlers.get(normalize(name));
	}
	
	public void initializeDynamicMBeanHandlers(VariableHandlerDynamicMBean mbean) throws PEException {
		if (mbean != null) {
			for(VariableHandler<?> vh : getGlobalHandlers()) {
				mbean.add(normalize(vh.getName()),vh);
			}
		}		
	}
	
	// called during catalog initialization, i.e. CatalogHelper
	public Map<VariableHandler,VariableConfig> initializeCatalog(CatalogDAO c) throws PEException {
		// load up compiled in values.
		HashMap<VariableHandler,VariableConfig> out = new HashMap<VariableHandler,VariableConfig>();
		for(VariableHandler<?> vh : handlers.values()) {
			out.put(vh,vh.initialiseCatalog(c));
		}
		return out;
	}
	
	// called during server initialization
	public void initialize(CatalogDAO in) throws PEException {
		CatalogDAO c = in;
		if (c == null)
			c = CatalogDAOFactory.newInstance();
		HashMap<VariableHandler,Object> values = new HashMap<VariableHandler,Object>();
		try {
			for(VariableHandler<?> vh : handlers.values()) {
				// this will add the variable to the catalog if it is missing
				values.put(vh,vh.initialise(c));
			}
			List<VariableConfig> allConfigs = c.findAllVariableConfigs();
			for(VariableConfig vc : allConfigs) {
				VariableHandler<?> exists = handlers.get(normalize(vc.getName()));
				if (exists == null) {
					// add it
					exists = loadHandler(vc);
					// make sure we call the normal initialisation in case there is a current global value
					values.put(exists,exists.initialise(c));
				}
			}
		} catch (PEException t) {
			throw t;
		} catch (Throwable t) {
			throw new PEException("Unable to initialize variables",t);
		} finally {
			if (in == null) {
				c.close();
			}
		}
		
		// now initialize the global map.  we put both kinds of variables into the global map:
		// the global variables and the session variables.  the same initialization rules apply, however:
		// if the value isn't set, we will put it in.
		// note that for the hazelcast version it's good that we already persisted the values - it will load them
		// from there
		ServerGlobalVariableStore globals = ServerGlobalVariableStore.INSTANCE;
		for(VariableHandler vh : handlers.values()) {
			ValueReference vr = globals.getReference(vh);
			if (vr == null) {
				// not set at all yet, so we get to do that
				globals.setValue(vh, values.get(vh));
			} else if (vh.getScopes().contains(VariableScopeKind.GLOBAL)) {
				// i.e. we got the current value, so set it
				vh.onGlobalValueChange(vr.get());
			}
		}

	}

	public static ValueMetadata findMetadataConverter(String type, String varName) throws PEException {
		ValueMetadata<?> vm = null;
		for(ValueMetadata ivm : KnownVariables.defaultConverters) {
			if (ivm.getTypeName().equals(type)) {
				vm = ivm;
				break;
			}
		}
		if (vm == null)
			throw new PEException("Unable to determine value type for variable '" + varName + "' (type=" + type + ")");
		return vm;
	}
	
	private VariableHandler loadHandler(VariableConfig vc) throws PEException {
		ValueMetadata<?> vm = findMetadataConverter(vc.getValueType(),vc.getName());
		VariableHandler exists = new VariableHandler(vc.getName(),vm,VariableHandler.convertScopes(vc.getScopes()),
				vm.convertToInternal(vc.getName(),vc.getValue()),
				VariableHandler.convertOptions(vc.getOptions()));
		addHandler(exists);
		return exists;
	}
	
	// dynamically add a new variable; this just handles loading the config out of
	// the catalog (where it was previously persisted, possibly by some other server)
	// and adding it to our known variables
	public VariableHandler postInitializationAddVariable(String varName) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			VariableConfig vc = c.findVariableConfig(varName);
			if (vc == null) 
				return null;
			return loadHandler(vc);
		} finally {
			c.close();
		}
	}
	
	// for trans exec engine
	public void initialiseTransient(GlobalVariableStore globals) {
		for(VariableHandler vh : handlers.values()) {
			ValueReference vr = globals.getReference(vh);
			if (vr == null) {
				// not set at all yet, so we get to do that
				globals.setValue(vh,vh.getDefaultOnMissing()); 		
			}
		}
	}
	
}
