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


import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;

public class Parameter extends ConstantExpression implements IParameter {

	protected int position;
	
	public Parameter(SourceLocation sloc) {
		super(sloc);
	}

	protected Parameter(Parameter p) {
		super(p);
		this.position = p.position;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getValue(sc, this);
	}
	
	@Override
	public Object convert(SchemaContext sc, Type type) {
        return Singletons.require(HostService.class).getDBNative().getValueConverter().convert(getValue(sc), type);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new Parameter(this);
	}
	
	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		Parameter op = (Parameter) other;
		return position == op.position;
	}

	@Override
	protected int selfHashCode() {
		return position;
	}
	

	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int p) {
		position = p;
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("param"));
	}

	@Override
	public boolean isParameter() {
		return true;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return new CachedParameterExpression(position);
	}

}
