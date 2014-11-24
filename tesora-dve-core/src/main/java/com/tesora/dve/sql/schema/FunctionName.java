package com.tesora.dve.sql.schema;

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

import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.parser.TokenTypes;

public class FunctionName extends UnqualifiedName {

	private static final long serialVersionUID = 1L;
	private final int tokenID;
	private final boolean operator;
	
	public FunctionName(String raw, int tok, boolean op, SourceLocation sloc) {
		super(raw,sloc);
		tokenID = tok;
		operator = op;
	}
	
	public FunctionName(String raw, int tok, boolean op) {
		super(raw);
		tokenID = tok;
		operator = op;
	}

	public FunctionName(UnqualifiedName unq, boolean op) {
		super(unq.get(), unq.getOrig());
		tokenID = unq.getOrig().getType();
		operator = op;
	}
	
	public boolean isAggregate() {
		return isMax() 
			|| isCount()
			|| isAverage()
			|| isSum()
			|| isMin()
			|| isGroupConcat()
			|| isVariance()
			|| isUnsupportedAggregate();
	}

	public boolean isAggregateNotIncludingCount() {
		return isMax() 
			|| isAverage()
			|| isSum()
			|| isMin()
			|| isGroupConcat()
			|| isVariance()
			|| isUnsupportedAggregate();
	}
	
	private static final int[] unsupportedAggFuns = new int[] {
			TokenTypes.BIT_AND, TokenTypes.BIT_OR, TokenTypes.BIT_XOR
	};
	
	public boolean isUnsupportedAggregate() {
		for(int uaf : unsupportedAggFuns)
			if (tokenID == uaf) return true;
		return false;
	}
	
	public boolean isOperator() {
		return operator;
	}
	
	public boolean isUnknown() {
		return tokenID == -1;
	}
	
	public boolean isRelationalOperator() {
		return tokenID == TokenTypes.AND
		|| tokenID == TokenTypes.OR
		|| tokenID == TokenTypes.Equals_Operator
		|| tokenID == TokenTypes.Less_Or_Equals_Operator
		|| tokenID == TokenTypes.Less_Than_Operator
		|| tokenID == TokenTypes.Greater_Or_Equals_Operator
		|| tokenID == TokenTypes.Greater_Than_Operator
		|| tokenID == TokenTypes.Not_Equals_Operator
		|| tokenID == TokenTypes.NOT;
	}
	
	public int getTokenID() {
		return tokenID;
	}
	
	public boolean isAnd() {
		return tokenID == TokenTypes.AND;
	}
	
	public boolean isEquals() {
		return tokenID == TokenTypes.Equals_Operator;
	}
	
	public boolean isOr() {
		return tokenID == TokenTypes.OR;
	}
	
	public boolean isIn() {
		return tokenID == TokenTypes.IN;
	}
	
	public boolean isNotIn() {
		return tokenID == TokenTypes.NOTIN;
	}
	
	public boolean isNot() {
		return tokenID == TokenTypes.NOT;
	}
	
	public boolean isNotEqual() {
		return tokenID == TokenTypes.Not_Equals_Operator;
	}
	
	public boolean isEffectiveNot() {
		return isNot() || isNotIn() || isNotLike() || isNotBetween() || isNotIs() || isNotEqual();
	}
	
	public boolean isBooleanOperator() {
		return isAnd() || isOr() || isNot();
	}
	
	public boolean isMax() {
		return tokenID == TokenTypes.MAX;
	}
	
	public boolean isDatabase() {
		return tokenID == TokenTypes.DATABASE;
	}
	
	public boolean isCount() {
		return tokenID == TokenTypes.COUNT;
	}
	
	public boolean isAverage() {
		return tokenID == TokenTypes.AVG;
	}
	
	public boolean isVariance() {
		return tokenID == TokenTypes.VARIANCE;
	}

	public boolean isSum() {
		return tokenID == TokenTypes.SUM;
	}
	
	public boolean isMin() {
		return tokenID == TokenTypes.MIN;
	}
	
	public boolean isIs() {
		return tokenID == TokenTypes.IS;
	}
	
	public boolean isNotIs() {
		return tokenID == TokenTypes.NOTIS;
	}
	
	public boolean isNotLike() {
		return tokenID == TokenTypes.NOTLIKE;
	}
	
	public boolean isCast() {
		return tokenID == TokenTypes.CAST;
	}
	
	public boolean isChar() {
		return tokenID == TokenTypes.CHAR;
	}

	public boolean isConvert() {
		return tokenID == TokenTypes.CONVERT;
	}
	
	public boolean isBetween() {
		return tokenID == TokenTypes.BETWEEN;
	}
	
	public boolean isNotBetween() {
		return tokenID == TokenTypes.NOTBETWEEN;
	}

	public boolean isLike() {
		return tokenID == TokenTypes.LIKE;
	}
	
	public boolean isPipes() {
		return tokenID == TokenTypes.Concatenation_Operator;
	}
	
	public boolean isDoubleAmpersand() {
		return tokenID == TokenTypes.Double_Ampersand;
	}
	
	public boolean isIfNull() {
		return tokenID == TokenTypes.IFNULL;
	}
	
	public boolean isGroupConcat() {
		return tokenID == TokenTypes.GROUP_CONCAT;
	}
	
	public boolean isExists() {
		return tokenID == TokenTypes.EXISTS;
	}
	
	public boolean isRand() {
		return tokenID == TokenTypes.RAND;
	}

	public static FunctionName makeEquals() {
		return new FunctionName("=", TokenTypes.Equals_Operator, true);
	}
	
	public static FunctionName makeNotEquals() {
		return new FunctionName("!=", TokenTypes.Not_Equals_Operator, true);
	}
	
	public static FunctionName makeOr() {
		return new FunctionName("OR", TokenTypes.OR, true);
	}
	
	public static FunctionName makeAnd() {
		return new FunctionName("AND", TokenTypes.AND, true);
	}
	
	public static FunctionName makeIn() {
		return new FunctionName("IN", TokenTypes.IN, true);
	}
	
	public static FunctionName makeSum() {
		return new FunctionName("sum", TokenTypes.SUM, false);
	}

	public static FunctionName makeMax() {
		return new FunctionName("MAX", TokenTypes.MAX, false);
	}
	
	public static FunctionName makeMin() {
		return new FunctionName("MIN", TokenTypes.MIN, false);
	}
	
	public static FunctionName makeAvg() {
		return new FunctionName("AVG", TokenTypes.AVG, false);
	}
	
	public static FunctionName makeVariance() {
		return new FunctionName("VARIANCE", TokenTypes.VARIANCE, false);
	}

	public static FunctionName makeCount() {
		return new FunctionName("COUNT", TokenTypes.COUNT, false);
	}
	
	public static FunctionName makeDivide() {
		return new FunctionName("/", TokenTypes.Slash, true);
	}
	
	public static FunctionName makeChar() {
		return new FunctionName("CHAR", TokenTypes.CHAR, false);
	}

	public static FunctionName makeConvert() {
		return new FunctionName("CONVERT",TokenTypes.CONVERT,false);
	}

	public static FunctionName makeLike() {
		return new FunctionName("LIKE", TokenTypes.LIKE, true);
	}

	public static FunctionName makeMinus() {
		return new FunctionName("-", TokenTypes.Minus_Sign, true);
	}
	
	public static FunctionName makePlus() {
		return new FunctionName("+", TokenTypes.Plus_Sign, true);
	}
	
	public static FunctionName makeMult() {
		return new FunctionName("*", TokenTypes.Asterisk, true);
	}
	
	public static FunctionName makeIs() {
		return new FunctionName("IS", TokenTypes.IS, true);
	}

	public static FunctionName makeIsNot() {
		return new FunctionName("IS NOT", TokenTypes.NOTIS, true);

	}
	
	public static FunctionName makeCoalesce() {
		return new FunctionName("COALESCE", TokenTypes.COALESCE, false);
	}
	
	public static FunctionName makeNot() {
		return new FunctionName("NOT", TokenTypes.NOT, true);
	}
	
	public static FunctionName makeRound() {
		return new FunctionName("ROUND",0,false);
	}
	
	public static FunctionName makeIfNull() {
		return new FunctionName("IFNULL",TokenTypes.IFNULL,false);
	}
	
	public static FunctionName makeRand() {
		return new FunctionName("RAND", TokenTypes.RAND, false);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (operator ? 1231 : 1237);
		result = prime * result + tokenID;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		FunctionName other = (FunctionName) obj;
		if (operator != other.operator)
			return false;
		if (tokenID != other.tokenID)
			return false;
		return true;
	}
	
	@Override
	public Name copy() {
		return new FunctionName(name,tokenID,operator);
	}

}
