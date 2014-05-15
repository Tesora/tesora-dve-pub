// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

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
