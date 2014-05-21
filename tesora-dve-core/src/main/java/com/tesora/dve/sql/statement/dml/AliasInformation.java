// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

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


import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.sql.expression.Scope;
import com.tesora.dve.sql.expression.ScopeStack;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class AliasInformation {

	// all aliases within the statement.
	private Map<String, Integer> aliases;
	
	public AliasInformation() {
		aliases = new HashMap<String, Integer>();
	}
	
	public AliasInformation(AliasInformation ai) {
		aliases = new HashMap<String,Integer>();
		aliases.putAll(ai.aliases);
	}
	
	public AliasInformation(ScopeStack scopes) {
		aliases = new HashMap<String, Integer>();
		if (scopes == null) return;
		List<Scope> ls = scopes.getCurrentStack();
		for(int i = ls.size() - 1; i > -1; i--) {
			take(ls.get(i));
		}
	}
	
	public boolean addAlias(String s) {
		Integer any = aliases.get(s);
		if (any == null)
			any = new Integer(1);
		else
			any = new Integer(any.intValue() + 1);
		return aliases.put(s,any) == null;
	}
	
	public void addAliases(Collection<String> coll) {
		for(String s : coll)
			addAlias(s);
	}
	
	private void take(Scope s) {
		addAliases(s.getAllAliases());
	}
	
	public void take(Set<String> existing) {
		addAliases(existing);
	}
	
	public void take(AliasInformation other) {
		aliases.putAll(other.aliases);
	}

	public void removeAlias(String s) {
		Integer any = aliases.get(s);
		if (any == null) return;
		if (any.intValue()  < 2) {
			aliases.remove(s);
		} else {
			aliases.put(s, new Integer(any.intValue() - 1));
		}
	}
	
	public UnqualifiedName buildNewAlias(Name inprefix) {
		Name prefix = inprefix;
		if (inprefix == null) prefix = new UnqualifiedName("");
		UnqualifiedName un = prefix.getUnqualified();
        if ( un.get().length() >= Singletons.require(HostService.class).getDBNative().getMaxAliasNameLen()) {
            int maxLenLessSuffix = Singletons.require(HostService.class).getDBNative().getMaxAliasNameLen() - 4;
			un = shortenAlias(un.get(), maxLenLessSuffix);
		}
		if (addAlias(un.get()))
			return un;
		int suffix = aliases.size() + 1;
		String raw = un.get();
		do {
			un = new UnqualifiedName(raw + "_" + suffix);
			suffix++;
		} while (!addAlias(un.get()));
		return un;
	}
	
	public boolean isDuplicateAlias(UnqualifiedName unq) {
		return getAliasCount(unq) >= 2;
	}
	
	public int getAliasCount(UnqualifiedName unq) {
		String n = unq.get();
		Integer count = aliases.get(n);
		return (count == null ? 0 : count.intValue());
	}
	
	private UnqualifiedName shortenAlias(String unq, int maxLen) {
		StringBuilder buf = new StringBuilder();
		
		// first look for '_' and pick the character following the '_' to make the alias
		String[] parts = StringUtils.splitByWholeSeparator(unq, "_");
		if (parts.length > 1) {
			buf.append(unq.charAt(0));
			for (String part : parts) {
				buf.append(part.charAt(0));
			}
		} else {
			// pick every other character
			for(int i=0; i < unq.length(); i++) {
				if ((i & 1) == 0) {
					buf.append(unq.charAt(i));
				}
			}
		}
		if (buf.length() > maxLen) {
			return shortenAlias(buf.toString(), maxLen);
		}
		
		return new UnqualifiedName(buf.toString());
	}
}
