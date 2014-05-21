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


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class CharFunctionCall extends FunctionCall {

	private final Name outputEncoding;

	public CharFunctionCall(final List<ExpressionNode> arguments, final Name outputEncoding) {
		super(FunctionName.makeChar(), arguments);
		this.outputEncoding = outputEncoding;
	}

	public Name getOutputEncoding() {
		return this.outputEncoding;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		final List<ExpressionNode> argsCopy = new ArrayList<ExpressionNode>(getParameters());
		final CharFunctionCall copy = new CharFunctionCall(argsCopy,
				this.outputEncoding);
		copy.setSetQuantifier(getSetQuantifier());

		return copy;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		CharFunctionCall otherFunctionCall = (CharFunctionCall) other;
		return ObjectUtils.equals(this.outputEncoding, otherFunctionCall.outputEncoding);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),(outputEncoding == null ? 0 : outputEncoding.hashCode()));
	}
	
}
