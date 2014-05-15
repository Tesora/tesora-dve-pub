// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
