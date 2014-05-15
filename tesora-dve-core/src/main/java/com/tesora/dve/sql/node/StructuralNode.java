// OS_STATUS: public
package com.tesora.dve.sql.node;

import com.tesora.dve.sql.parser.SourceLocation;

public abstract class StructuralNode extends LanguageNode {

	protected StructuralNode(SourceLocation sloc) {
		super(sloc);
	}
	
}
