// OS_STATUS: public
package com.tesora.dve.sql.node;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;

public abstract class AbstractTraversal<NT> {

	public enum Order {
		PREORDER, 
		POSTORDER, 
		NATURAL_ORDER // left to right order in the original sql
	}
	
	public enum ExecStyle {
		ONCE,
		REPEAT
	}
	
	protected final Order order;
	protected final ExecStyle repeat;

	protected final Stack<NT> path;
		
	public AbstractTraversal(Order direction, ExecStyle execStyle) {
		this.order = direction;
		if (this.order == null)
			throw new SchemaException(Pass.PLANNER, "Must specify ordering for traversal");
		this.repeat = execStyle;
		if (this.repeat == null)
			throw new SchemaException(Pass.PLANNER, "Must specify execution style for traversal");
		this.path = new Stack<NT>();
	}

	protected final Stack<NT> getPath() {
		return path;
	}
	
	protected void push(NT ln) {
		path.push(ln);
	}
	
	protected NT pop() {
		return path.pop();
	}
	
	public boolean allow(NT ln) {
		return true;
	}
	
	public abstract NT action(NT in);
	
	protected NT performAction(NT in) {
		return action(in);
	}

	protected abstract void traverseInternal(NT n);

	protected final NT doAction(NT in) {
		if (repeat == ExecStyle.ONCE)
			return performAction(in);
		else if (repeat == ExecStyle.REPEAT) {
			NT c = in;
			NT last = null;
			do {
				last = c;
				c = performAction(c);
			} while(c != last);
			return c;			
		} else {
			throw new SchemaException(Pass.PLANNER, "Unknown execution style for traversal: " + repeat);
		}
	}

	public final NT traverse(NT n) {
		if (n == null) return n;
		if (!allow(n)) return n;
		NT out = null;
		push(n);
		if (order == Order.PREORDER) {
			out = doAction(n);
			traverseInternal(out);
		} else if (order == Order.POSTORDER || order == Order.NATURAL_ORDER) {
			traverseInternal(n);
			out = doAction(n);
		} else {
			throw new MigrationException("Unknown ordering for traversal: " + order);
		}
		pop();
		return out;
	}	


	
}
