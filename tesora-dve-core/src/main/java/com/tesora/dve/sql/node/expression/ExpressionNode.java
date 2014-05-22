package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionKey;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;

public abstract class ExpressionNode extends LanguageNode {

	public static final int GROUPED = 1;
	public static final int NEGATED = 2;
	public static final int BIT_NEGATED = 4;
	
	protected byte flags;
	
	protected ExpressionNode(SourceLocation tree) {
		super(tree);
		flags = 0;
	}
	
	protected ExpressionNode(ExpressionNode en) {
		super(en);
		flags = en.flags;
	}
	
	protected boolean isSet(int f) {
		return (flags & f) != 0;
	}
	
	protected void clearFlag(int f) {
		flags &= ~f;
	}
	
	protected void setFlag(int f) {
		flags |= f;
	}
		
	public boolean isGrouped() { return isSet(GROUPED); }
	public void setGrouped() { setFlag(GROUPED); }
	
	public boolean isNegated() { return isSet(NEGATED); }
	public void setNegated() { setFlag(NEGATED); }
	
	public boolean isBitNegated() { return isSet(BIT_NEGATED); }
	public void setBitNegated() { setFlag(BIT_NEGATED); }
	
	@Override
	public String toString() {
		return toString(SchemaContext.threadContext.get());
	}
	
	public String toString(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
        Singletons.require(HostService.class).getDBNative().getEmitter().emitExpression(sc,this, buf);
		return buf.toString();		
	}
	
	/**
	 * @param sc
	 * @return
	 */
	public NameAlias buildAlias(SchemaContext sc) {
		throw new SchemaException(Pass.NORMALIZE,"Unsupported expression type for buildAlias: " + this.getClass().getName());
	}
	
	public RewriteKey getRewriteKey() {
		return new ExpressionKey(this);
	}

	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return Collections.emptyList();
	}


	public <T extends ExpressionNode> T copy(T me, CopyContext cc) {
		@SuppressWarnings("unchecked")
		T out = (T) me.copySelf(cc);
		if (isGrouped())
			out.setGrouped();
		if (isNegated())
			out.setNegated();
		return out;
	}
	
	@Override
	public LanguageNode copy(CopyContext cc) {
		return copy(this,cc);
	}
	
	protected abstract LanguageNode copySelf(CopyContext cc);
	
	public List<ExpressionNode> getSubExpressions() {
		return Collections.emptyList();
	}
	
}
