package com.tesora.dve.sql.schema.modifiers;

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

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.Comment;
import com.tesora.dve.sql.schema.SchemaContext;

public class CommentTableModifier extends TableModifier {

	private final Comment comment;
	
	public CommentTableModifier(Comment c) {
		super();
		comment = c;
	}
	
	public Comment getComment() {
		return comment;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		emitter.emitComment(comment, buf);
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.COMMENT;
	}

}
