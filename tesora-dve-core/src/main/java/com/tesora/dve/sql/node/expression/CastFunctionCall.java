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


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class CastFunctionCall extends FunctionCall {

	private Name typeName;
	private String asText;
	
	public CastFunctionCall(ExpressionNode firstArg, Name typeName) {
		this(firstArg,"CAST",typeName,"AS");
	}
	
	public CastFunctionCall(ExpressionNode firstArg, String castText, Name typeName, String asText) {
		super(new FunctionName(castText, TokenTypes.CAST, false), firstArg);
		this.typeName = typeName;
		this.asText = asText;
	}

	public Name getTypeName() {
		return typeName;
	}
	
	public String getAsText() {
		return asText;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ExpressionNode argcopy = (ExpressionNode) getParameters().get(0).copy(cc); 
		CastFunctionCall nfc = new CastFunctionCall(argcopy, getFunctionName().get(), typeName, asText);
		nfc.setSetQuantifier(getSetQuantifier());
		return nfc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		CastFunctionCall oth = (CastFunctionCall) other;
		return typeName.equals(oth.typeName);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),typeName.hashCode());
	}
	
}
