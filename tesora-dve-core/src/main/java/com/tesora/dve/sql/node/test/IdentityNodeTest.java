// OS_STATUS: public
package com.tesora.dve.sql.node.test;

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
