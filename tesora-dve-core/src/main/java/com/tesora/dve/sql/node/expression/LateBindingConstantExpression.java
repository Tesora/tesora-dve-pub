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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;

public class LateBindingConstantExpression extends ConstantExpression {

	private final int position;
	private final Type type;
	
	public LateBindingConstantExpression(int position, Type type) {
		super((SourceLocation)null);
		this.position = position;
		this.type = type;
	}
	
	public LateBindingConstantExpression(LateBindingConstantExpression o) {
		super(o);
		this.position = o.position;
		this.type = o.type;
	}
	
	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.RUNTIME;
	}
	
	@Override
	public IConstantExpression getCacheExpression() {
		// we can be our own cache expression since we are really just an offset
		return this;
	}

	@Override
	public Object getValue(ConnectionValues cv) {
		return cv.getRuntimeConstant(position);
	}

	@Override
	public Object convert(ConnectionValues cv, Type type) {
		Object val = getValue(cv);
		if (val == null) return null;
        return Singletons.require(DBNative.class).getValueConverter().convert(val, type);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new LateBindingConstantExpression(this);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (other instanceof LateBindingConstantExpression) {
			LateBindingConstantExpression o = (LateBindingConstantExpression) other;
			return position == o.position;
		}
		return false;
	}

	@Override
	protected int selfHashCode() {
		return position;
	}

	public Type getType() {
		return type;
	}
	
}
