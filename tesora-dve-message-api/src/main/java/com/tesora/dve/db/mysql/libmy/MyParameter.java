// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PEException;

public class MyParameter {

	MyFieldType type;
	Object value;
	
	MyParameter(MyFieldType type, Object value) {
		this.type = type;
		this.value = value;
	}

	public MyParameter(MyFieldType type) {
		this.type = type;
		this.value = null;
	}

	public MyFieldType getType() {
		return type;
	}

	public void setType(MyFieldType type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	public String getValueForQuery() throws PEException {
		return DBTypeBasedUtils.getMysqlTypeFunc(type).getParamReplacement(value,true);
	}
}
