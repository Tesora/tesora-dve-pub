// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.Pair;

public class AddViewsVersion extends ComplexCatalogVersion {

	public AddViewsVersion(int v) {
		super(v,true);
	}

	private static final String[] before = new String[] {
		"alter table user_table change `engine` `engine` varchar(255)",
		"alter table user_table add `table_type` varchar(255)",
		"create table user_view (view_id integer not null auto_increment, "
		+"algorithm varchar(255) not null, "
		+"character_set_client varchar(255) not null, "
		+"check_option varchar(255) not null, "
		+"collation_connection varchar(255) not null, "
		+"definition longtext not null, "
		+"mode varchar(255) not null, "
		+"security varchar(255) not null, "
		+"user_id integer not null, "
		+"table_id integer not null, "
		+"primary key (view_id), unique (table_id)) ENGINE=InnoDB",
		"alter table user_view add index FK143DAE99D1F1FE05 (user_id), add constraint FK143DAE99D1F1FE05 foreign key (user_id) references user (id)",
		"alter table user_view add index FK143DAE994B90C0A4 (table_id), add constraint FK143DAE994B90C0A4 foreign key (table_id) references user_table (table_id)"
	};
	
	private static final String[] after = new String[] {
		"alter table user_table change `table_type` `table_type` varchar(255) not null"
	};

	@Override
	public void upgrade(DBHelper helper) throws PEException {
		execQuery(helper, before);
		Pair<Long,Long> dbids = getSimpleBounds(helper,"user_database","user_database_id");
		for(long i = dbids.getFirst(); i <= dbids.getSecond(); i++) {
			setTableTypeOnDatabase(helper,i);
		}
		execQuery(helper,after);
	}

	private void setTableTypeOnDatabase(DBHelper helper, long dbid) throws PEException {
		String dbname = (String) getSingleField(helper,"select name from user_database where user_database_id = " + dbid);
		if (dbname == null) return;
		String tableType = PEConstants.DEFAULT_TABLE_TYPE;
		if (PEConstants.INFORMATION_SCHEMA_DBNAME.equalsIgnoreCase(dbname)) {
			tableType = "SYSTEM VIEW";
		}
		execQuery(helper, "update user_table set table_type = '" + tableType + "' where user_database_id = " + dbid);
	}
	
}
