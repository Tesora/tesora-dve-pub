package com.tesora.dve.sql.infoschema;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */




import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.common.TwoDimensionalMap;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CharacterSets;
import com.tesora.dve.common.catalog.Collations;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.ContainerTenant;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.common.catalog.ServerRegistration;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.TemporaryTable;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaColumnsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.ColumnCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.DatabaseCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.TableCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.ViewCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ExternalServiceSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowColumnInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowContainerInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowDatabaseInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowTableInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowView;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.Functional;

public class AnnotationInformationSchemaBuilder implements
		InformationSchemaBuilder {

	// define the catalog entities which provide information we can query in service of info schema requests.
	private static final InfoTableConfig[] catalogClasses = new InfoTableConfig[] {
		new InfoTableConfig(UserTable.class).withLogical(TableCatalogInformationSchemaTable.class).withShow(ShowTableInformationSchemaTable.class),
		new InfoTableConfig(UserDatabase.class).withShow(ShowDatabaseInformationSchemaTable.class).withLogical(DatabaseCatalogInformationSchemaTable.class),
		new InfoTableConfig(DistributionRange.class),
		new InfoTableConfig(Provider.class),
		new InfoTableConfig(PersistentGroup.class),
		new InfoTableConfig(PersistentSite.class),
		new InfoTableConfig(Tenant.class),
		new InfoTableConfig(StorageGroupGeneration.class),
		new InfoTableConfig(DistributionModel.class),
		new InfoTableConfig(User.class),
		new InfoTableConfig(TableVisibility.class),
		new InfoTableConfig(SiteInstance.class),
		new InfoTableConfig(Priviledge.class),
		new InfoTableConfig(ExternalService.class).withShow(ExternalServiceSchemaTable.class),
		new InfoTableConfig(Container.class).withShow(ShowContainerInformationSchemaTable.class),
		new InfoTableConfig(ContainerTenant.class),
		new InfoTableConfig(CharacterSets.class),
		new InfoTableConfig(RangeTableRelationship.class),
		new InfoTableConfig(PersistentTemplate.class),
		new InfoTableConfig(ServerRegistration.class),
		new InfoTableConfig(RawPlan.class),
		new InfoTableConfig(UserView.class).withLogical(ViewCatalogInformationSchemaTable.class),
		new InfoTableConfig(Collations.class),
		new InfoTableConfig(TemporaryTable.class),
		new InfoTableConfig(VariableConfig.class)
	};
	
	private static Type buildType(Class<?> retVal, InfoSchemaColumn info, Class<?> enc, DBNative dbn) throws PEException {
		if (info.sqlType() == java.sql.Types.VARCHAR || info.sqlType() == java.sql.Types.LONGVARCHAR) {
			if (String.class.equals(retVal)) {
				// can trust the width
				return BasicType.buildType(info.sqlType(), info.sqlWidth(),dbn);
			}
			// the original was not a string, but we are flattening it down to one.  guess as to the original type.
			if (Boolean.class.equals(retVal) || Boolean.TYPE.equals(retVal))
				// usually yes/no, or true/false - 8 characters is plenty
				return BasicType.buildType(info.sqlType(), 8,dbn);
			else if (CatalogEntity.class.isAssignableFrom(retVal)) 
				return BasicType.buildType(info.sqlType(), info.sqlWidth(), dbn);
			else if (Enum.class.isAssignableFrom(retVal)) 
				return BasicType.buildType(info.sqlType(), info.sqlWidth(), dbn);			
		} else if ((info.sqlType() == java.sql.Types.INTEGER) || info.sqlType() == java.sql.Types.BIGINT) {
			return BasicType.buildType(info.sqlType(),info.sqlWidth(),dbn);
		}
		throw new PEException("Unable to compute metadata for info schema column " + info.logicalName() + " of class " + enc.getSimpleName());
	}

	private void build(Class<?> c, InfoTableConfig itc, LogicalInformationSchema logicalSchema,
			Map<InfoView, AbstractInformationSchema> schemas,
			DBNative dbn, InfoSchemaTable ist, javax.persistence.Table ptable) throws PEException {
		CatalogInformationSchemaTable cist = null;
		try {
			Class<?> catClass = itc.getLogical();
			Constructor<?> cons = catClass.getConstructor(Class.class, InfoSchemaTable.class, String.class);
			cist = (CatalogInformationSchemaTable) cons.newInstance(c, ist, ptable.name());
		} catch (Throwable t) {
			throw new PEException("Unable to construct logical table for " + c.getSimpleName(),t);
		}
		logicalSchema.addTable(null,cist);
		HashMap<InfoView, ComputedInformationSchemaTable> views = new HashMap<InfoView, ComputedInformationSchemaTable>();
		for(TableView tv : ist.views()) {
			try {
				if (tv.view() == InfoView.SHOW) {
					Class<?> showClass = itc.getShow();
					Constructor<?> cons = showClass.getConstructor(LogicalInformationSchemaTable.class,UnqualifiedName.class,UnqualifiedName.class,Boolean.TYPE,Boolean.TYPE);
					ShowInformationSchemaTable sist = (ShowInformationSchemaTable) cons.newInstance(cist,new UnqualifiedName(tv.name()), new UnqualifiedName(tv.pluralName()), tv.priviledged(), tv.extension());
					views.put(tv.view(), sist);
				} else {
					Class<?> infoClass = itc.getInfo();
					Constructor<?> cons = infoClass.getConstructor(InfoView.class, LogicalInformationSchemaTable.class, UnqualifiedName.class, UnqualifiedName.class, Boolean.TYPE, Boolean.TYPE);
					ComputedInformationSchemaTable istv = 
						(ComputedInformationSchemaTable) cons.newInstance(tv.view(), cist, new UnqualifiedName(tv.name()), 
								("".equals(tv.pluralName()) ? null : new UnqualifiedName(tv.pluralName())),
								tv.priviledged(), tv.extension());
					views.put(tv.view(), istv);
				}
			} catch (Throwable t) {
				throw new PEException("Unable to construct " + tv.view() + " table for " + c.getSimpleName(),t);
			}
		}
		// per-view, also maintain a map of view column name to view column
		TwoDimensionalMap<InfoView, String, ComputedInformationSchemaColumn> viewColumns = new TwoDimensionalMap<InfoView, String, ComputedInformationSchemaColumn>();

		// try to obtain the table name via introspection.
		Method[] methods = c.getDeclaredMethods();
		for(Method m : methods) {
			InfoSchemaColumn isc = m.getAnnotation(InfoSchemaColumn.class);
			if (isc == null) continue;
			String fieldName = isc.fieldName();
			String columnName = null;
			javax.persistence.Column columnAnno = null;
			javax.persistence.JoinColumn joinColumnAnno = null;
			boolean id = false;
			if (fieldName != null && !"".equals(fieldName)) try {
				Field f = c.getDeclaredField(fieldName);
				columnAnno = f.getAnnotation(javax.persistence.Column.class);
				joinColumnAnno = f.getAnnotation(javax.persistence.JoinColumn.class);
				if (f.getAnnotation(javax.persistence.Id.class) != null)
					id = true;
				if (joinColumnAnno != null)
					columnName = joinColumnAnno.name();
				else if (columnAnno != null)
					columnName = columnAnno.name();
				else
					columnName = fieldName;
			} catch (Throwable t) {
				throw new PEException("Invalid info schema data: no such field " + fieldName + " in " + c.getSimpleName() + " for info schema column on " + m,t);				
			}
			Type t = buildType(m.getReturnType(),isc,c,dbn);
			CatalogLogicalInformationSchemaColumn cisc = 
				new CatalogLogicalInformationSchemaColumn(isc,t,m,columnName,columnAnno,joinColumnAnno,id);
			cisc = (CatalogLogicalInformationSchemaColumn) cist.addColumn(null,cisc);
			for(ColumnView cv : isc.views()) {
				ComputedInformationSchemaColumn iscv = new CatalogInformationSchemaColumn(cv, cisc);
				viewColumns.put(cv.view(), cv.name(), iscv);
				if (iscv.isInjected() && !cisc.isInjected())
					throw new PEException("Invalid info schema data: column " + cv.name() + " in view " + cv.view() + " is injected but backing logical column is not");
			}
		}		
		// go back to the table views and build the final declaration order
		for(TableView tv : ist.views()) {
			ComputedInformationSchemaTable table = views.get(tv.view()); 
			Map<String,ComputedInformationSchemaColumn> found = viewColumns.get(tv.view());
			for(String n : tv.columnOrder()) {
				ComputedInformationSchemaColumn iscv = found.remove(n);
				if (iscv == null)
					throw new PEException("Missing column decl for " + n + " in table " + tv.name() + " in info schema view " + tv.view());
				table.addColumn(null,iscv);
			}
			if (!found.isEmpty()) {				
				throw new PEException("Missing column decl order for column(s): " + Functional.joinToString(found.keySet(), ", ") + " in info schema view " + tv.view() + " for table " + table.getName().get());
			}
			schemas.get(tv.view()).addTable(null,table);
		}
	}
	
	@Override
	public void populate(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, ShowView showSchema,
			MysqlSchema mysqlSchema, DBNative dbn) throws PEException {
		HashMap<InfoView, AbstractInformationSchema> schemas = new HashMap<InfoView, AbstractInformationSchema>();
		schemas.put(infoSchema.getView(), infoSchema);
		schemas.put(showSchema.getView(), showSchema);
		schemas.put(mysqlSchema.getView(), mysqlSchema);
		for(InfoTableConfig itc : catalogClasses) {
			Class<?> c = itc.getTarget();
			InfoSchemaTable ist = c.getAnnotation(InfoSchemaTable.class);
			javax.persistence.Table ptable = c.getAnnotation(javax.persistence.Table.class);
			if (ist == null || ptable == null) continue;
			build(c, itc, logicalSchema, schemas, dbn, ist, ptable);
		}
	}
	
	private static class InfoTableConfig {
		
		private Class<?> matchClass;
		private Class<?> logicalClass;
		private Class<?> showClass;
		private Class<?> infoSchemaClass;
		
		public InfoTableConfig(Class<?> m) {
			matchClass = m;
			logicalClass = CatalogInformationSchemaTable.class;
			showClass = ShowInformationSchemaTable.class;
			infoSchemaClass = ComputedInformationSchemaTable.class;
		}
		
		public InfoTableConfig withLogical(Class<?> c) {
			logicalClass = c;
			return this;
		}
		
		public InfoTableConfig withShow(Class<?> c) {
			showClass = c;
			return this;
		}
		
		public InfoTableConfig withInfo(Class<?> c) {
			infoSchemaClass = c;
			return this;
		}
		
		public Class<?> getTarget() {
			return matchClass;
		}
		
		public Class<?> getLogical() {
			return logicalClass;
		}
		
		public Class<?> getShow() {
			return showClass;
		}
		
		public Class<?> getInfo() {
			return infoSchemaClass;
		}
	}
	
}
