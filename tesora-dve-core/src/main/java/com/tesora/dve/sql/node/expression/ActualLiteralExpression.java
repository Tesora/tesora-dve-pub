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

import java.util.Objects;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class ActualLiteralExpression extends LiteralExpression {

	protected final Object value;

	public ActualLiteralExpression(Object v, int tt, SourceLocation sloc, UnqualifiedName charsetHint) {
		super(tt,sloc, charsetHint);
		this.value = v;
	}

	public ActualLiteralExpression(String s, SourceLocation sloc) {
		super(TokenTypes.Character_String_Literal, sloc, null);
		this.value = s;
	}

	protected ActualLiteralExpression(ActualLiteralExpression other) {
		super(other);
		this.value = other.value;
	}
	
	public Object getValue() { return this.value; }
	
	@Override
	public Object getValue(ConnectionValues cv) {
		return isNullLiteral() ? null : this.value; 
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ActualLiteralExpression out = new ActualLiteralExpression(this);
		return out;
	}

	@Override
	public int getPosition() {
		return 0;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		// we still require a cache version for p statements, where literals are not delegated.
		return new CachedActualLiteralExpression(getValueType(), getValue());
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		ActualLiteralExpression ale = (ActualLiteralExpression) other;
		return ObjectUtils.equals(this.value, ale.value);
	}

	@Override
	protected int selfHashCode() {
		return Objects.hashCode(this.value);
	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null) {
			return false;
		} else if (this == other) {
			return true;
		} else if (!(other instanceof ActualLiteralExpression)) {
			return false;
		}

		final ActualLiteralExpression otherExpression = (ActualLiteralExpression) other;

		return (this.getValueType() == otherExpression.getValueType())
				&& ObjectUtils.equals(this.value, otherExpression.value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 17;
		result = prime * result + this.getValueType();
		result = prime * result + Objects.hashCode(this.value);
		return result;
	}

}
