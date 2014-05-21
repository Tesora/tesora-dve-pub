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
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class ConvertFunctionCall extends FunctionCall {

	private final Name name;
	private final boolean transcoding;
	
	public ConvertFunctionCall(ExpressionNode expr, Name rhs, boolean transcoding) {
		super(FunctionName.makeConvert(),expr);
		this.name = rhs;
		this.transcoding = transcoding;
	}
	
	public boolean isTranscoding() {
		return transcoding;
	}
	
	public Name getTypeName() {
		return name;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ExpressionNode argcopy = (ExpressionNode) getParameters().get(0).copy(cc); 
		ConvertFunctionCall nfc = new ConvertFunctionCall(argcopy, name, transcoding);
		nfc.setSetQuantifier(getSetQuantifier());
		return nfc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		ConvertFunctionCall ocfc = (ConvertFunctionCall) other;
		return this.name.equals(ocfc.name) && this.transcoding == ocfc.transcoding;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(addSchemaHash(super.selfHashCode(),name.hashCode()),transcoding);
	}	
}
