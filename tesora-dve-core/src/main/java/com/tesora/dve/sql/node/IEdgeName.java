// OS_STATUS: public
package com.tesora.dve.sql.node;

import java.util.EnumSet;

public interface IEdgeName {

	public String getName();
	
	public boolean matches(IEdgeName in);
	
	// not applicable for offset edges
	public boolean any(EnumSet<EdgeName> set);
	
	public boolean baseMatches(IEdgeName in);
	
	public boolean isOffset();
	
	public OffsetEdgeName makeOffset(int i);
	
	public IEdgeName getBase();
	
}
