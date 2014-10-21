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

import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class TriggerColumnInstance extends ColumnInstance implements
		IConstantExpression {

	public TriggerColumnInstance(Column<?> c, TriggerTableInstance ti) {
		super(c,ti);
	}

	
	public TriggerColumnInstance(Name origName, Column<?> column,
			TriggerTableInstance table) {
		super(origName, column, table);
	}

	public TriggerTableInstance getTriggerTableInstance() {
		return (TriggerTableInstance) getTableInstance();
	}
	
	public boolean isBefore() {
		return getTriggerTableInstance().isBefore();
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPosition() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.TRIGGER_COLUMN;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected TriggerColumnInstance copySelf(CopyContext cc) {
		if (cc == null) 
			return new TriggerColumnInstance(specifiedAs, schemaColumn, (TriggerTableInstance) ofTable);
		TriggerColumnInstance out = (TriggerColumnInstance) cc.getColumnInstance(this);
		if (out != null) return out;
		TriggerTableInstance cti = (TriggerTableInstance) cc.getTableInstance(ofTable);
		if (cti == null) cti = (TriggerTableInstance) ofTable.copy(cc);
		out = new TriggerColumnInstance(specifiedAs, schemaColumn, cti);
		return cc.put(this, out);
	}
	

	
}
