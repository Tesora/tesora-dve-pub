// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

public class ClientCapabilities {
	
	// Server Capabilities for Handshake (ServerGreetingResponse)
	public static final long CLIENT_LONG_PASSWORD=1;			/* new more secure passwords */
	public static final long CLIENT_FOUND_ROWS=1 << 1;			/* Found instead of affected rows */
	public static final long CLIENT_LONG_FLAG=1 << 2;			/* Get all column flags */
	public static final long CLIENT_CONNECT_WITH_DB=1 << 3;		/* One can specify db on connect */
	public static final long CLIENT_NO_SCHEMA=1 << 4;			/* Don't allow database.table.column */
	public static final long CLIENT_COMPRESS=1 << 5;			/* Can use compression protocol */
	public static final long CLIENT_ODBC=1 << 6;				/* Odbc client */
	public static final long CLIENT_LOCAL_FILES=1 << 7;			/* Can use LOAD DATA LOCAL */
	public static final long CLIENT_IGNORE_SPACE=1 << 8;		/* Ignore spaces before '(' */
	public static final long CLIENT_PROTOCOL_41=1 << 9;			/* New 4.1 protocol */
	public static final long CLIENT_INTERACTIVE=1 << 10;		/* This is an interactive client */
	public static final long CLIENT_SSL=1 << 11;				/* Switch to SSL after handshake */
	public static final long CLIENT_IGNORE_SIGPIPE=1 << 12;   	/* IGNORE sigpipes */
	public static final long CLIENT_TRANSACTIONS=1 << 13;		/* Client knows about transactions */
	public static final long CLIENT_RESERVED=1 << 14;	   		/* Old flag for 4.1 protocol  */
	public static final long CLIENT_SECURE_CONNECTION=1 << 15;  /* New 4.1 authentication */
	public static final long CLIENT_MULTI_STATEMENTS=1 << 16; 	/* Enable/disable multi-stmt support */
	public static final long CLIENT_MULTI_RESULTS=1 << 17; 		/* Enable/disable multi-results */
	public static final long CLIENT_PS_MULTI_RESULTS=1 << 18; 	/* Multi-results in PS-protocol */
	public static final long CLIENT_PLUGIN_AUTH=1 << 19; 		/* Client supports plugin authentication */
	public static final long CLIENT_SSL_VERIFY_SERVER_CERT=1 << 30;
	public static final long CLIENT_REMEMBER_OPTIONS=1 << 31;

	long clientCapabilities = 0;
	
	public ClientCapabilities() {
	}

    public ClientCapabilities(long clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
    }

	public void setClientCapability(long clientCapability) {
		this.clientCapabilities = clientCapability;
	}
	
	public boolean useLongPassword() { return isFlagSet(CLIENT_LONG_PASSWORD);	}
	/** Send found rows instead of affected rows in EOF Packet */
	public boolean sendFoundRows() {return isFlagSet(CLIENT_FOUND_ROWS); }
	public boolean useLongFlag() { return isFlagSet(CLIENT_LONG_FLAG); }
	public boolean connectWithDB() { return isFlagSet(CLIENT_CONNECT_WITH_DB); }
	public boolean disallowSchema() { return isFlagSet(CLIENT_NO_SCHEMA); }
	public boolean supportCompression() { return isFlagSet(CLIENT_COMPRESS); }
	public boolean useODBCBehaviour() { return isFlagSet(CLIENT_ODBC); }
	public boolean allowLocalInfile() { return isFlagSet(CLIENT_LOCAL_FILES); }
	public boolean ignoreSpace() { return isFlagSet(CLIENT_IGNORE_SPACE); }
	public boolean support41Protocol() { return isFlagSet(CLIENT_PROTOCOL_41); }
	public boolean interactiveClient() { return isFlagSet(CLIENT_INTERACTIVE); }
	public boolean supportSSL() { return isFlagSet(CLIENT_SSL); }
	public boolean ignoreSIGPIPE() { return isFlagSet(CLIENT_IGNORE_SIGPIPE); }
	public boolean hasTransactions() { return isFlagSet(CLIENT_TRANSACTIONS); }
	public boolean reserved() { return isFlagSet(CLIENT_RESERVED); }
	public boolean supportSecureConnection() { return isFlagSet(CLIENT_SECURE_CONNECTION); }
	public boolean supportMultipleStatements() { return isFlagSet(CLIENT_MULTI_STATEMENTS); }
	public boolean supportMultipleResults() { return isFlagSet(CLIENT_MULTI_RESULTS); }
	public boolean supportPrepStmtMultipleResults() { return isFlagSet(CLIENT_PS_MULTI_RESULTS); }
	public boolean supportPluginAuth() { return isFlagSet(CLIENT_PLUGIN_AUTH); }
	public boolean useSSLVerifyServerCertg() { return isFlagSet(CLIENT_SSL_VERIFY_SERVER_CERT); }
	public boolean hasRememberOptions() { return isFlagSet(CLIENT_REMEMBER_OPTIONS); }
	
	public boolean isFlagSet(long flag) { return (clientCapabilities & flag) != 0; }
}
