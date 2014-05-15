// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.sql.expression.Traversable;
import com.tesora.dve.sql.transform.CopyContext;

public class Comment extends Traversable {

	private String comment;
	
	public Comment(String c) {
		comment = c;
	}
	
	public String getComment() {
		return comment;
	}

	@Override
	public boolean replaceTraversable(Traversable prev, Traversable nv) {
		return false;
	}

	@Override
	public Traversable copy(CopyContext cc) {
		return new Comment(comment);
	}
	
}
