// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import java.util.List;

import com.tesora.dve.sql.node.LanguageNode;

public interface NodeTest<T> {

	public boolean has(LanguageNode in);
	public boolean has(LanguageNode in, EngineToken tok);
	
	public T get(LanguageNode in);
	public T get(LanguageNode in, EngineToken tok);

	public List<T> getMulti(LanguageNode in);
	
}
