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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;

public abstract class AbstractVariableAccessor {

	public abstract String getValue(VariableStoreSource conn) throws PEException;

	public abstract void setValue(VariableStoreSource conn, String v) throws Throwable;

	public abstract String getSQL();

	// just a utility copied over from the old variable accessor
	
	private static void convert(Map<String,String> varvalues, String scopeName, List<List<String>> acc) {
		for(Map.Entry<String, String> me : varvalues.entrySet()) {
			ArrayList<String> tuple = new ArrayList<String>(3);
			tuple.add(scopeName);
			tuple.add(me.getKey());
			tuple.add(me.getValue());
			acc.add(tuple);
		}
	}

	// returns a list of <scope-name, variable-name, variable-value>
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<List<String>> getValues(VariableScope scope, VariableStoreSource source) throws PEException {
		List<List<String>> out = new ArrayList<List<String>>();
		if (scope.getKind() == VariableScopeKind.USER) {
			convert(source.getUserVariableStore().getVariableMap(),scope.getKind().name(),out);
		} else if (scope.getKind() == VariableScopeKind.SESSION 
				|| scope.getKind() == VariableScopeKind.GLOBAL) {
			HashMap<String,String> strings = new HashMap<String,String>();
			VariableManager vm = Singletons.require(HostService.class).getVariableManager();
			// there are some wierd rules here:
			// if scope=global, only get global values
			// if scope=session:
			//    if the var has session & global scope, get session value
			//    if the var has session scope, get session value
			//    if the var has global scope, get the global value
			for(VariableHandler vh : vm.getAllHandlers()) {
				if (scope.getKind() == VariableScopeKind.SESSION) {
					if (vh.getScopes().contains(VariableScopeKind.SESSION))
						strings.put(vh.getName(), vh.toRow(vh.getValue(source, VariableScopeKind.SESSION)));
					else if (vh.getScopes().contains(VariableScopeKind.GLOBAL))
						strings.put(vh.getName(), vh.toRow(vh.getValue(source, VariableScopeKind.GLOBAL)));
				} else if (vh.getScopes().contains(VariableScopeKind.GLOBAL)) {
					strings.put(vh.getName(), vh.toRow(vh.getValue(source, VariableScopeKind.GLOBAL)));					
				}
			}
			convert(strings,scope.getKind().name(),out);
		} else if (scope.getKind() == VariableScopeKind.SCOPED) {
			if (scope.getScopeName() == null || "".equals(scope.getScopeName())) {
                for(String sn : Singletons.require(HostService.class).getScopedVariableScopeNames()) {
                    convert(Singletons.require(HostService.class).getScopedVariables(sn), sn, out);
				}
			} else {
				convert(Singletons.require(HostService.class).getScopedVariables(scope.getScopeName()), scope.getScopeName(), out);
			}
		} else {
			throw new PEException("Unknown scope kind: " + scope.getKind());
		}
		// now we need to sort the tuples; the sort order is by first value, then second value
		Collections.sort(out, new Comparator<List<String>>() {

			@Override
			public int compare(List<String> o1, List<String> o2) {
				int ms = o1.get(0).compareTo(o2.get(0));
				if (ms != 0) return ms;
				return o1.get(1).compareTo(o2.get(1));
			}
			
		});
		return out;
	}
		
}
