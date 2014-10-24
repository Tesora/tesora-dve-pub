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


import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.types.Type;

public abstract class LiteralExpression extends ConstantExpression implements ILiteralExpression {
	
	private int tokenType;
	private UnqualifiedName charsetHint;
	
	protected LiteralExpression(int tt, SourceLocation sloc, UnqualifiedName charsetHint) {
		super(sloc);
		tokenType = tt;
		this.charsetHint = charsetHint;
	}

	protected LiteralExpression(LiteralExpression other) {
		super(other);
		tokenType = other.tokenType;
		this.charsetHint = other.charsetHint;
	}
		
	@Override
	public int getValueType() { return tokenType; }
	
	@Override
	public UnqualifiedName getCharsetHint() {
		return charsetHint;
	}
	
	@Override
	public ConstantType getConstantType() {
		return ConstantType.LITERAL;
	}

	
	@Override
	public boolean isNullLiteral() {
		return tokenType == TokenTypes.NULL;
	}

	public boolean isGlobalLiteral() {
		return tokenType == TokenTypes.GLOBAL;
	}

	@Override
	public boolean isStringLiteral() {
		return tokenType == TokenTypes.Character_String_Literal;
	}
	
	public boolean isBitStringLiteral() {
		return tokenType == TokenTypes.Bit_String_Literal;
	}
	
	public boolean isHexStringLiteral() {
		return tokenType == TokenTypes.Hex_String_Literal;
	}
	
	public boolean isNumericLiteral() {
		switch (tokenType) {
		case TokenTypes.Signed_Float:
		case TokenTypes.Signed_Integer:
		case TokenTypes.Signed_Large_Integer:
		case TokenTypes.Unsigned_Float:
		case TokenTypes.Unsigned_Integer:
		case TokenTypes.Unsigned_Large_Integer:
			return true;
			
		default:
			return false;
		}
	}
	
	public boolean isBooleanLiteral() {
		return isBooleanTrueLiteral() || isBooleanFalseLiteral();
	}

	public boolean isBooleanTrueLiteral() {
		return tokenType == TokenTypes.TRUE;
	}
	
	public boolean isBooleanFalseLiteral() {
		return tokenType == TokenTypes.FALSE;
	}
	public boolean isFloatLiteral() {
		switch (tokenType) {
		case TokenTypes.Signed_Float:
		case TokenTypes.Unsigned_Float:
			return true;
			
		default:
			return false;
		}
	}

	public boolean isBigIntegerLiteral() {
		switch (tokenType) {
		case TokenTypes.Signed_Large_Integer:
		case TokenTypes.Unsigned_Large_Integer:
			return true;
			
		default:
			return false;
		}
	}

	public boolean isIntegerLiteral() {
		switch (tokenType) {
		case TokenTypes.Signed_Integer:
		case TokenTypes.Unsigned_Integer:
			return true;
			
		default:
			return false;
		}
	}

	public String asString(SchemaContext sc) {
		Object lv = getValue(sc);
		if (lv instanceof String) {
			return (String)lv;
		} else {
			throw new ParserException(Pass.SECOND, "Expecting string literal, got: " + lv);
		}
	}
		
	public static LiteralExpression makeNullLiteral() {
		return new ActualLiteralExpression("NULL", TokenTypes.NULL,null,null);
	}
	
	public static LiteralExpression makeAutoIncrLiteral(long value) {
		return makeLongLiteral(value);
	}
	
	public static LiteralExpression makeSignedIntegerLiteral(final int value) {
		return new ActualLiteralExpression(new Integer(value), TokenTypes.Signed_Integer, null, null);
	}

	public static LiteralExpression makeLongLiteral(long value) {
		return new ActualLiteralExpression(new Long(value), TokenTypes.Unsigned_Large_Integer,null,null);		
	}
	
	public static LiteralExpression makeStringLiteral(String in) {
		return new ActualLiteralExpression(in, TokenTypes.Character_String_Literal,null,null);
	}
	
	public static LiteralExpression makeHexStringLiteral(String in) {
		return new ActualLiteralExpression(in, TokenTypes.Hex_String_Literal, null, null);
	}

	public static LiteralExpression makeDoubleLiteral(double d) {
		return new ActualLiteralExpression(new Double(d), TokenTypes.Unsigned_Float, null,null);
	}
	
	public static LiteralExpression makeBooleanLiteral(boolean b) {
		return new ActualLiteralExpression(Boolean.valueOf(b), b ? TokenTypes.TRUE : TokenTypes.FALSE, null, null);
	}
	
	public Object convert(SchemaContext sc, Type type) {
		if (isNullLiteral())
			return null;
        return Singletons.require(HostService.class).getDBNative().getValueConverter().convert(getValue(sc), type);
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("litex"));
	}
	
	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		LiteralExpression olit = (LiteralExpression) other;
		return olit.tokenType == tokenType;
	}

	@Override
	protected int selfHashCode() {
		return tokenType;
	}

}
