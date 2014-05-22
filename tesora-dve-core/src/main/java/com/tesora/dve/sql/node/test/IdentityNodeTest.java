package com.tesora.dve.sql.node.test;

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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MigrationException;

public class IdentityNodeTest implements NodeTest<LanguageNode> {

	protected final Class<?> ofClass;
	protected final boolean acceptsParameter;
	protected final String etag;
	
	public IdentityNodeTest(String etag, Class<?> tc, boolean hasParam) {
		this.etag = etag;
		acceptsParameter = hasParam;
		ofClass = tc;
	}
	
	public IdentityNodeTest(String etag, Class<?> tc) {
		this(etag, tc, false);
	}
	
	@Override
	public boolean has(LanguageNode in) {
		return ofClass.isInstance(in);
	}
	
	@Override
	public boolean has(LanguageNode in, EngineToken tok) {
		if (!acceptsParameter) return has(in);
		throw new MigrationException("Body for IdentityNodeTest.has(LanguageNode, String)");
	}
	
	@Override
	public LanguageNode get(LanguageNode in) {
		if (has(in)) return in;
		return null;
	}
	
	@Override
	public LanguageNode get(LanguageNode in, EngineToken tok) {
		if (has(in,tok)) return in;
		return null;
	}

	@Override
	public List<LanguageNode> getMulti(LanguageNode in) {
		if (has(in)) return Collections.singletonList(in);
		return null;
	}

	
}
