package com.tesora.dve.sql.infoschema.computed;

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

import com.tesora.dve.sql.infoschema.InformationSchemaColumnAdapter;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.SyntheticLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

// columns with constant value.
public class ConstantComputedInformationSchemaColumn extends SyntheticComputedInformationSchemaColumn {

	private final Object constantValue;
	
	public ConstantComputedInformationSchemaColumn(InfoView view,
			UnqualifiedName nameInView, Type type, Object value) {
		super(view, nameInView, type, null);
		constantValue = value;
		setAdapter(new ConstantInformationSchemaColumnAdapter(this));
	}

	@Override
	public ExpressionNode buildReplacement(ColumnInstance in) {
		if (constantValue instanceof String)
			return LiteralExpression.makeStringLiteral((String)constantValue);
		else if (constantValue instanceof Long)
			return LiteralExpression.makeLongLiteral((Long)constantValue);
		else if (constantValue instanceof Integer)
			return LiteralExpression.makeLongLiteral(((Integer)constantValue).longValue());
		else if (constantValue == null)
			return LiteralExpression.makeNullLiteral();
		else
			throw new InformationSchemaException("Invalid constant value: " + constantValue);
	}

	/*
	*/
	
	private static class ConstantInformationSchemaColumnAdapter extends InformationSchemaColumnAdapter {
		
		private final ConstantComputedInformationSchemaColumn ours;
		private SyntheticLogicalInformationSchemaColumn logicalColumn = null;
		
		public ConstantInformationSchemaColumnAdapter(ConstantComputedInformationSchemaColumn me) {
			ours = me;
		}

		@Override
		public LogicalInformationSchemaColumn getLogicalColumn() {
			if (logicalColumn != null) return logicalColumn;
			
			logicalColumn = new SyntheticLogicalInformationSchemaColumn(ours.getName().getUnqualified(), ours.getType()) {

				@Override
				public LanguageNode explode(ColumnInstance subject) {
					return ours.buildReplacement(subject);
				}
				
				@Override
				public Object getValue() {
					return ours.constantValue;
				}
			};
			
			return logicalColumn;
		}

	}
}
