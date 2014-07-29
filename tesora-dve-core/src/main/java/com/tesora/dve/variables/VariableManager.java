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
import java.util.LinkedHashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.Functional;

// mostly just a helper class
public class VariableManager {

	// not final because we could add
	private LinkedHashMap<String,VariableHandler<?>> handlers;
	
	public VariableManager() throws PEException {
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
	
	private static String normalize(String in) {
		return in.toUpperCase();
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
	
	public VariableHandler<?> lookup(String name, boolean except) throws PEException {
		VariableHandler<?> vh = handlers.get(normalize(name));
		if (vh == null && except)
			throw new PENotFoundException(String.format("No such variable: '%s'", name));
		return vh;
		
	}
	
	// initialization code
	@SuppressWarnings("unchecked")
	public void initializeCatalog(CatalogDAO in, VariableHandlerDynamicMBean mbean) throws PEException {
		CatalogDAO c = in;
		if (c == null)
			c = CatalogDAOFactory.newInstance();
		try {
			for(VariableHandler<?> vh : handlers.values()) {
				vh.initialise(c);
			}
			for(VariableConfig vc : c.findAllVariableConfigs()) {
				VariableHandler<?> exists = handlers.get(normalize(vc.getName()));
				if (exists == null) {
					ValueMetadata<?> vm = null;
					for(ValueMetadata ivm : KnownVariables.defaultConverters) {
						if (ivm.getTypeName().equals(vc.getValueType())) {
							vm = ivm;
							break;
						}
					}
					if (vm == null)
						throw new PEException("Unable to determine value type for variable '" + vc.getName() + "' (type=" + vc.getValueType() + ")");
					// add it
					exists = new VariableHandler(vc.getName(),vm,VariableHandler.convert(vc.getScopes()),
							vm.convertToInternal(vc.getName(),vc.getValue()),
							(vc.isDVEOnly() ? EnumSet.of(VariableOption.DVE_ONLY) : EnumSet.noneOf(VariableOption.class)));
					addHandler(exists);
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
				globals.setValue(vh, vh.getDefaultValueReference().get());
			} else if (vh.getScopes().contains(VariableScopeKind.GLOBAL)) {
				// i.e. we got the current value, so set it
				vh.onGlobalValueChange(vr.get());
			}
		}
		
		// we have the variables initialized, go register all the globals
		if (mbean != null) {
			for(VariableHandler<?> vh : getGlobalHandlers()) {
				mbean.add(normalize(vh.getName()),vh);
			}
		}
		
	}

	// for trans exec engine
	public void initialiseTransient(GlobalVariableStore globals) {
		for(VariableHandler vh : handlers.values()) {
			ValueReference vr = globals.getReference(vh);
			if (vr == null) {
				// not set at all yet, so we get to do that
				globals.setValue(vh, vh.getDefaultValueReference().get());
			}
		}
	}
	
}
