// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LateEvaluatingLiteralExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class RuntimeLimitSpecification {

	private LimitSpecification intermediateSpecification;
	private LiteralExpression inMemoryLimit;
	private boolean isInconstantLimit;
	
	private RuntimeLimitSpecification(LimitSpecification intermediate, LiteralExpression inMemory, boolean variable) {
		this.intermediateSpecification = intermediate;
		this.inMemoryLimit = inMemory;
		this.isInconstantLimit = variable;
	}
	
	public LimitSpecification getIntermediateSpecification() {
		return intermediateSpecification;
	}
	
	public LiteralExpression getInMemoryLimit() {
		return inMemoryLimit;
	}
	
	public boolean isInconstantLimit() {
		return isInconstantLimit;
	}
	
	public static RuntimeLimitSpecification analyze(SchemaContext sc, DMLStatement sourceStatement, LimitSpecification ls) throws PEException {
		if (ls == null) return new RuntimeLimitSpecification(null,null,false);
		LimitSpecification intermediateSpec = null;
		LiteralExpression inMemoryLimit = null;
		boolean inconstantLimit = false;
		ExpressionNode origOffExpr = ls.getOffset();
		ExpressionNode origRCExpr = ls.getRowcount();
		LiteralExpression origOff = (origOffExpr instanceof LiteralExpression ? (LiteralExpression)origOffExpr : null);
		LiteralExpression origRC = (origRCExpr instanceof LiteralExpression ? (LiteralExpression)origRCExpr : null);
		
		if (origOff != null && origRC != null) {
			// we can't use limit a+b because mysql doesn't like the expression, so instead we use
			// a late evaluating literal expression which will convert the delegating literals into actual values
			// and execute the sum at sql generation time
			LateEvaluatingLiteralExpression later = new LateEvaluatingLiteralExpression(TokenTypes.Unsigned_Large_Integer,
					sc.getValueManager(),
					new ConstantExpression[] { origOff, origRC },
					LateEvaluatingLiteralExpression.SUM);					
			intermediateSpec = new LimitSpecification(later,null);
		} else if (origRC != null && origOff == null) {
			// row count with no offset - we can push it down as is - also we may be able to use the in mem optimization
			// as long as there is no order by
			intermediateSpec = ls;
			if (!EngineConstant.ORDERBY.has(sourceStatement))
				inMemoryLimit = origRC;
		} else {
			// have to push down, do the old way
			// offset exists but either offset or row count is not constant, revert to the slow way
			intermediateSpec = null;
			inconstantLimit = true;
		}			
		return new RuntimeLimitSpecification(intermediateSpec,inMemoryLimit,inconstantLimit);
	}
	
}
