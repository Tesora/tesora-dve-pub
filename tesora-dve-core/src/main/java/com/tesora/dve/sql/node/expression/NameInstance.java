// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

// used only during parsing; after parsing & resolution is done these are converted into
// aliases, column references, etc.
public class NameInstance extends ExpressionNode {

	private Name name;
	
	public NameInstance(Name n, SourceLocation sloc) {
		super(sloc);
		name = n;
	}
	
	public Name getName() {
		return name;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected int selfHashCode() {
		// TODO Auto-generated method stub
		return 0;
	}


}
