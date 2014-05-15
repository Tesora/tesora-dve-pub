// OS_STATUS: public
package com.tesora.dve.distribution;

import java.io.Serializable;

public interface IColumnDatum extends Serializable {

	public String getColumnName();
	
	public int getColumnId();
	
	public Object getValue();
	
	public Comparable<?> getValueForComparison();
	
	public String getNativeType();
	
	public String getComparatorClassName();

	public void setComparatorClassName(String comparatorClassName);
	
	@Override
	public int hashCode();
	
	@Override
	public boolean equals(Object o);
	
}
