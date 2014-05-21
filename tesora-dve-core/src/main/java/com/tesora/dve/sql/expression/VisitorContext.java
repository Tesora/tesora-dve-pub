package com.tesora.dve.sql.expression;

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
