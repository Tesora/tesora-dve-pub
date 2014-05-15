// OS_STATUS: public
package com.tesora.dve.common;

// holds all the names in the show schema
// unless otherwise noted, the order of the constants in the nested classes is generally the order
// of the columns in the show command when extensions are on
public class ShowSchema {

	public interface Database {
		
		// the name of the database
		public static final String NAME = "Database";
		
		// default persistent group
		public static final String DEFAULT_PERSISTENT_GROUP = "Default_Persistent_Group";
		
		// template
		public static final String TEMPLATE = "Template";
		
		// whether the template is optional, required, strict...
		public static final String TEMPLATE_MODE = "Template_Mode";
		
		// multitenant mode
		public static final String MULTITENANT = "Multitenant_Mode";
		
		// foreign key mode
		public static final String FKMODE = "Foreign_Key_Mode";
		
		// default character set
		public static final String DEFAULT_CHARACTER_SET = "Default_Character_Set_Name";

		// default collation
		public static final String DEFAULT_COLLATION = "Default_Collation_Name";
		
	}
	
	public interface Table {
		
		public static final String NAME = "Tables";
		
		public static final String MODEL = "Distribution_Model";
		
		public static final String PERSISTENT_GROUP = "Persistent_group";
		
		public static final String TYPE = "Table_type";
		
	}
	
	public interface Range {
		
		public static final String NAME = "Range";
		
		public static final String PERSISTENT_GROUP = "Persistent_Group";
		
		public static final String SIGNATURE = "Signature";
		
	}
	
	public interface GroupPolicy {
		
		public static final String NAME = "Group_Policy";
		
		public static final String CONFIG = "Configuration";
	}
	
	public interface GroupProvider {
		
		public static final String NAME = "Group_Provider";
		
		public static final String PLUGIN = "Plugin";
		
		public static final String ENABLED = "Enabled";
		
	}
	
	public interface GroupProviderSites {
		
		public static final String SITE_NAME = "Site_Name";
		
		public static final String NAME = "Name";
		
		public static final String URL = "Url";

		public static final String USER = "User";

		public static final String PASSWORD = "Password";
		
		public static final String PROVIDER = "Provider";
		
		public static final String POOL = "Pool";
		
		public static final String MAX_QUERIES = "Max_Queries";
		
		public static final String CURRENT_QUERIES = "Current_Queries";
		
		public static final String TOTAL_QUERIES = "Total_Queries";
		
		public static final String STATUS = "Status";
		
		public static final String TIMESTAMP = "Time_State_Change";
		
	}
	
	public interface PersistentGroup {
		
		public static final String NAME = "Persistent_Group";
		
		public static final String LAST_GENERATION = "Latest_Generation";
		
	}
	
	public interface PersistentSite {
		
		public static final String NAME = "Persistent_Site";
		
		public static final String HA_TYPE = "HA_Type";
		
		public static final String URL = "Master_Url";

	}
	
	public interface Tenant {
		
		public static final String NAME = "Tenant";
		
		public static final String DATABASE = "Database";
		
		public static final String SUSPENDED = "Suspended";
		
		public static final String DESCRIPTION = "Description";
		
		public static final String ID = "ID";
		
	}
	
	public interface Template {
		
		public static final String NAME = "Template_Name";
		
		public static final String COMMENT = "Comment";
		
		public static final String BODY = "Template";
		
		public static final String MATCH = "DB_Match";
		
	}
	
	public interface RawPlan {
		
		public static final String NAME = "Plan_Name";
		
		public static final String COMMENT = "Comment";
		
		public static final String DB = "Database";
		
		public static final String BODY = "Plan";
		
		public static final String ENABLED = "Enabled";
		
		public static final String CACHE_KEY = "Cache_Key";
				
	}
	
	public interface Column {
		
		public static final String NAME = "Field";
		
		public static final String TYPE = "Type";
		
		public static final String NULLABLE = "Null";
		
		public static final String KEY = "Key";
		
		public static final String DEFAULT = "Default";
		
		public static final String EXTRA = "Extra";
	}
	
	public interface Generation {
		
		public static final String NAME = "id";
		
		public static final String PERSISTENT_GROUP = "Persistent_Group";
		
		public static final String LOCKED = "Locked";
		
		public static final String VERSION = "Version";
		
	}
	
	public interface DistributionModel {
		
		public static final String NAME = "Model";
		
	}
	
	public interface GenerationSite {
		
		public static final String NAME = "Persistent_Group";
		
		public static final String VERSION = "Version";
		
		public static final String SITE = "Site";
	}

	public interface SiteInstance {
		
		public static final String NAME = "Name";
		
		public static final String PERSISTENT_SITE = "Persistent_Site";
		
		public static final String URL = "URL";

		public static final String USER = "User";

		public static final String PASSWORD = "Password";
		
		public static final String IS_MASTER = "Master";

		public static final String STATUS = "Status";
	}

	public interface ExternalService {
		
		public static final String NAME = "name";
		
		public static final String PLUGIN = "plugin";
		
		public static final String AUTO_START = "auto_start";
		
		public static final String CONNECT_USER = "connect_user";
		
		public static final String USES_DATASTORE = "uses_datastore";
		
	}

	public interface Container {
		
		public static final String NAME = "Container";
		
		public static final String BASE_TABLE = "Base_Table";
		
		public static final String PERSISTENT_GROUP = "Persistent_Group";
		
	}
	
	public interface ContainerTenant {
		
		public static final String NAME = "Discriminant";
		
		public static final String CONTAINER = "Container";
		
		public static final String ID = "ID";
	}

	public interface Engines {
		public static final String ENGINE = "Engine";
		
		public static final String SUPPORT = "Support";
		
		public static final String COMMENT = "Comment";
		
		public static final String TRANSACTIONS = "Transactions";

		public static final String XA = "XA";
		
		public static final String SAVEPOINTS = "Savepoints";
		
	}

	public interface CharSet {
		public static final String CHARSET = "Charset";
		
		public static final String DESCRIPTION = "Description"; 
		
		public static final String MAXLEN = "Maxlen"; 
	}

	public interface Collation {
		public static final String NAME = "Collation";
		
		public static final String CHARSET_NAME = "Charset"; 
		
		public static final String DEFAULT = "Default";
		
		public static final String ID = "Id"; 
		
		public static final String COMPILED = "Compiled";
		
		public static final String SORTLEN = "Sortlen"; 
	}
}
