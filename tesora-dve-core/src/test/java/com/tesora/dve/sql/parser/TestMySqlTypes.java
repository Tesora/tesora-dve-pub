// OS_STATUS: public
package com.tesora.dve.sql.parser;

import java.util.List;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

public class TestMySqlTypes extends TestTypes {

	@Override
	public List<NativeType> getNativeTypes() {
        NativeTypeCatalog ntc = Singletons.require(HostService.class).getDBNative().getTypeCatalog();
		return Functional.select(ntc.getTypesByName().values(), new UnaryPredicate<NativeType>() {

			@Override
			public boolean test(NativeType object) {
				// we now autoconvert numeric into decimal
				if ("numeric".equalsIgnoreCase(object.getTypeName()))
					return false;
				return true;
			}
			
		});
	}
}
