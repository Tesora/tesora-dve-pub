// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.show.ShowInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowView;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public final class InformationSchemas {

	protected final LogicalInformationSchema logical;
	
	protected final InformationSchemaView infoSchema;
	protected final ShowView show;
	protected final MysqlView mysql;
	
	private InformationSchemas(LogicalInformationSchema lis, InformationSchemaView isv, ShowView sv, MysqlView msv) {
		this.logical = lis;
		this.infoSchema = isv;
		this.show = sv;
		this.mysql = msv;
	}
	
	public LogicalInformationSchema getLogical() {
		return logical;
	}
	
	public InformationSchemaView getInfoSchema() {
		return infoSchema;
	}
	
	public ShowView getShowSchema() {
		return this.show;
	}
	
	public ShowInformationSchemaTable lookupShowTable(UnqualifiedName unq) {
		return getShowSchema().lookupTable(unq);
	}
	
	public MysqlView getMysqlSchema() {
		return this.mysql;
	}
	
	public List<PersistedEntity> buildEntities(int groupid, int modelid, String charSet, String collation) throws PEException {
		CatalogSchema cs = new CatalogSchema();
		ArrayList<PersistedEntity> acc = new ArrayList<PersistedEntity>();
		infoSchema.buildEntities(cs,groupid, modelid, charSet, collation, acc);
		show.buildEntities(cs, groupid, modelid, charSet, collation, acc);
		mysql.buildEntities(cs, groupid, modelid, charSet, collation, acc);
		return acc;
	}
	
	public static InformationSchemas build(DBNative dbn) throws PEException {
		try {
			LogicalInformationSchema logicalSchema = new LogicalInformationSchema();
			InformationSchemaView informationSchema = new InformationSchemaView(logicalSchema);
			ShowView showSchema = new ShowView(logicalSchema);
			MysqlView mysqlSchema = new MysqlView(logicalSchema);
			
			// make the builders for each schema & then build them.
			InformationSchemaBuilder builders[] = new InformationSchemaBuilder[] {
					// the order these are built in is important
					new AnnotationInformationSchemaBuilder(),
					new SyntheticInformationSchemaBuilder()
			};
			for(InformationSchemaBuilder isb : builders)
				isb.populate(logicalSchema, informationSchema, showSchema, mysqlSchema, dbn);
			// freeze the schemas.  we freeze the logical schema
			// first to build the derived information so that it can be used in the views.
			logicalSchema.freeze(dbn);
			informationSchema.freeze(dbn);
			showSchema.freeze(dbn);
			mysqlSchema.freeze(dbn);
			return new InformationSchemas(logicalSchema,informationSchema,showSchema,mysqlSchema);
		} catch (PEException pe) {
			throw pe;
		} catch (Throwable t) {
			throw new PEException("Unable to initialize information schema",t);
		}
	}
	
	public DatabaseView buildPEDatabase(SchemaContext sc, UserDatabase udb) {
		if (InfoView.INFORMATION.getUserDatabaseName().equals(udb.getName()))
			return new DatabaseView(sc, udb, infoSchema);
		else if (InfoView.MYSQL.getUserDatabaseName().equals(udb.getName()))
			return new DatabaseView(sc, udb, mysql);
		return null;
	}
}
