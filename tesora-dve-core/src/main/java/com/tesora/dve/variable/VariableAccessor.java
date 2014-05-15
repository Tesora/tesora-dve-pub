// OS_STATUS: public
package com.tesora.dve.variable;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class VariableAccessor {
		
	private VariableScopeKind scope;
	private String name;
	private String scopeName;
	
	public VariableAccessor(VariableScopeKind vsk, String scopeName, String variableName) {
		scope = vsk;
		name = variableName;
		this.scopeName = scopeName;
	}
	
	public VariableAccessor(VariableScopeKind vsk, String variableName) {
		this(vsk,null,variableName);
	}
	
	public VariableScopeKind getScopeKind() {
		return scope;
	}
	
	public String getVariableName() {
		return name;
	}
	
	public String getScopeName() {
		return scopeName;
	}
	
	public String getValue(SSConnection conn) throws PEException {
		// if "session", if the session doesn't have the var, we're also going to check in "dve".
		// this handles something like version_comment, which is in "dve" for us, but will never be
		// specified as dve.version_comment.
		if (VariableScopeKind.SESSION == scope)
			return conn.getSessionVariable(name);
		else if (VariableScopeKind.DVE == scope)
            return Singletons.require(HostService.class).getGlobalVariable(conn.getCatalogDAO(), name);
		else if (VariableScopeKind.USER == scope)
			return conn.getUserVariable(name);
			throw new PEException("Unknown variable scope: " + scope);
	}

	public static List<List<String>> getValues(VariableScopeKind scope, String scopeName, SSConnection conn) throws PEException {
		ArrayList<List<String>> raw = new ArrayList<List<String>>();
		if (VariableScopeKind.SESSION == scope) {
			convert(conn.getSessionVariables(), "session", raw);
		} else if (VariableScopeKind.DVE == scope) {
            convert(Singletons.require(HostService.class).getGlobalVariables(conn.getCatalogDAO()), "dve", raw);
		} else if (VariableScopeKind.USER == scope) {
			convert(conn.getUserVariables(), "user", raw);
		} else if (VariableScopeKind.GROUP_PROVIDER == scope) {
			if (scopeName != null)
                convert(Singletons.require(HostService.class).getScopedVariables(scopeName), scopeName, raw);
			else {
                for(String sn : Singletons.require(HostService.class).getScopedVariableScopeNames()) {
                    convert(Singletons.require(HostService.class).getScopedVariables(sn), sn, raw);
				}
			}
		} else {
			throw new PEException("Unknown scope kind: " + scope);			
		}
		// now we need to sort the tuples; the sort order is by first value, then second value
		Collections.sort(raw, new Comparator<List<String>>() {

			@Override
			public int compare(List<String> o1, List<String> o2) {
				int ms = o1.get(0).compareTo(o2.get(0));
				if (ms != 0) return ms;
				return o1.get(1).compareTo(o2.get(1));
			}
			
		});
		return raw;
	}
	
	public void setValue(SSConnection conn, String v) throws Throwable {
		if (VariableScopeKind.DVE == scope) {
            Singletons.require(HostService.class).setGlobalVariable(conn.getCatalogDAO(), name, v);
		} else if (VariableScopeKind.SESSION == scope) {
			conn.setSessionVariable(name, v);
		} else if (VariableScopeKind.USER == scope) {
			conn.setUserVariable(name, v);
		} else {
            Singletons.require(HostService.class).setScopedVariable(scopeName, name, v);
		}

	}
	
	private static void convert(Map<String,String> varvalues, String scopeName, List<List<String>> acc) {
		for(Map.Entry<String, String> me : varvalues.entrySet()) {
			ArrayList<String> tuple = new ArrayList<String>(3);
			tuple.add(scopeName);
			tuple.add(me.getKey());
			tuple.add(me.getValue());
			acc.add(tuple);
		}
	}
	
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		if (scope == VariableScopeKind.SESSION) {
			buf.append("@@session.");
		} else if (scope == VariableScopeKind.USER) {
			buf.append("@");
		} else if (scope == null) {
			buf.append(scopeName).append(".");
		}
		buf.append(name);
		return buf.toString();
	}
}
