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

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.CopyContext;

public class GroupConcatCall extends FunctionCall {

	private String separator;
	
	public GroupConcatCall(FunctionName funName, List<ExpressionNode> params,
			SetQuantifier quant, String separator) {
		super(funName, params, quant, null);
		this.separator = separator;
	}

	public String getSeparator() {
		return separator;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ArrayList<ExpressionNode> np = new ArrayList<ExpressionNode>();
		for(ExpressionNode p : getParameters()) {
			np.add((ExpressionNode)p.copy(cc));
		}
		GroupConcatCall gcc = new GroupConcatCall(getFunctionName(),np,getSetQuantifier(),separator);
		return gcc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		GroupConcatCall gcc = (GroupConcatCall) other;
		return ObjectUtils.equals(separator, gcc.separator);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),(separator == null ? 0 : separator.hashCode()));
	}



}
