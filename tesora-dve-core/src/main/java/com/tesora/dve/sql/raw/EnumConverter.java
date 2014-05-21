// OS_STATUS: public
package com.tesora.dve.sql.raw;

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

import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.raw.jaxb.GroupScaleType;
import com.tesora.dve.sql.raw.jaxb.LiteralType;
import com.tesora.dve.sql.raw.jaxb.ModelType;
import com.tesora.dve.sql.schema.DistributionVector;

public class EnumConverter {

	private static final LiteralType[] xmlLiteralTypes = LiteralType.values();
	private static final ExtractedLiteral.Type[] candidateLiteralTypes = ExtractedLiteral.Type.values();
	
	public static LiteralType convert(ExtractedLiteral.Type in) {
		return xmlLiteralTypes[in.ordinal()];
	}
	
	public static ExtractedLiteral.Type convert(LiteralType in) {
		return candidateLiteralTypes[in.ordinal()];
	}
	
	private static final GroupScaleType[] xmlScaleTypes = GroupScaleType.values();
	private static final GroupScale[] dynGroupSizes = GroupScale.values();
	
	public static GroupScaleType convert(GroupScale in) {
		return xmlScaleTypes[in.ordinal()];
	}
	
	public static GroupScale convert(GroupScaleType in) {
		return dynGroupSizes[in.ordinal()];
	}
	
	private static final ModelType[] xmlModelTypes = ModelType.values();
	private static final DistributionVector.Model[] modelTypes = DistributionVector.Model.values();
	
	public static ModelType convert(DistributionVector.Model in) {
		return xmlModelTypes[in.ordinal()];
	}
	
	public static DistributionVector.Model convert(ModelType in) {
		return modelTypes[in.ordinal()];
	}
	
	public static int literalTypeToTokenType(LiteralType in) {
		switch(in) {
		case STRING:
			return TokenTypes.Character_String_Literal;
		case INTEGRAL:
			return TokenTypes.Unsigned_Large_Integer;
		case DECIMAL:
			return TokenTypes.Unsigned_Float;
		case HEX:
			return TokenTypes.Hex_String_Literal;
		default:
			throw new SchemaException(Pass.PLANNER, "Unknown literal type: " + in);
		}
	}
}
