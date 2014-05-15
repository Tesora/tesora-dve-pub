// OS_STATUS: public
package com.tesora.dve.sql.expression;


import java.util.Stack;

import com.tesora.dve.sql.node.LanguageNode;

public class VisitorContext {

	protected Stack<LanguageNode> path;
	
	public VisitorContext() {
		path = new Stack<LanguageNode>();
	}
	
	public VisitorContext(VisitorContext vc) {
		path = new Stack<LanguageNode>();
		path.addAll(vc.path);
	}
	
	public Stack<LanguageNode> getPath() { return path; }
	public void push(LanguageNode e) { path.push(e); }
	public void pop() { path.pop(); }
	
}
