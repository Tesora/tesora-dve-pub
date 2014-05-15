// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class RawPlans extends SimpleCatalogVersion {

	public RawPlans(int v) {
		super(v, true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
			"create table rawplan (plan_id integer not null auto_increment, cachekey longtext not null, plan_comment varchar(255), definition longtext not null, enabled integer not null, name varchar(255) not null, user_database_id integer not null, primary key (plan_id)) ENGINE=InnoDB;",
			"alter table rawplan add index FK3ACDEF51F229CB7C (user_database_id), add constraint FK3ACDEF51F229CB7C foreign key (user_database_id) references user_database (user_database_id);"	
		};
	}

}
