// OS_STATUS: public
package com.tesora.dve.sql.parser;

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

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;

public class TreeSourceLocation extends SourceLocation {

	private static final long serialVersionUID = 1L;
	protected CommonTree tree;
	
	public TreeSourceLocation(CommonTree ct) {
		super();
		tree = ct;
	}
	
	@Override
	public String getText() {
		return tree.getText();
	}

	@Override
	public String getText(TokenStream tns) {
		CommonToken leftToken = (CommonToken) tns.get(tree.getTokenStartIndex());
		CommonToken rightToken = (CommonToken) tns.get(tree.getTokenStopIndex());
		String otxt = leftToken.getInputStream().substring(leftToken.getStartIndex(), rightToken.getStopIndex());
		return otxt;
	}

	@Override
	public int getType() {
		return tree.getType();
	}

	@Override
	public int getLineNumber() {
		return tree.getLine();
	}

	@Override
	public int getPositionInLine() {
		return tree.getCharPositionInLine();
	}

}
