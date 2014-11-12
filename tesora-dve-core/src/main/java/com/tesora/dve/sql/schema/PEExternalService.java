package com.tesora.dve.sql.schema;

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

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.util.Pair;

public class PEExternalService extends Persistable<PEExternalService, ExternalService> {
	public static final String OPTION_PLUGIN = "plugin";
	public static final String OPTION_CONNECT_USER = "user";
	public static final String OPTION_AUTOSTART = "autostart";
	public static final String OPTION_USE_DATASTORE = "usesdatastore";
	public static final String OPTION_CONFIG = "config";
	
	String externalServiceName;
	String plugin;
	String connectUser = null;
	String config = null;
	Boolean autoStart = true;
	Boolean usesDataStore = false;
	
	public PEExternalService(SchemaContext pc, Name externalServiceName, List<Pair<Name,LiteralExpression>> options) {
		super(null);

		this.externalServiceName = externalServiceName.getUnqualified().get();
		setName(externalServiceName);

		parseOptions(pc,options);
	}

	public PEExternalService(SchemaContext pc, ExternalService es) {
		this(pc, es, null);
	}

	public PEExternalService(SchemaContext pc, ExternalService es, List<Pair<Name,LiteralExpression>> options) {
		super(null);

		this.externalServiceName = es.getName();
		setName(new UnqualifiedName(es.getName()));
		
		if (options != null) {
			for(Pair<Name,LiteralExpression> option : options) {
				if (option.getFirst().get().equalsIgnoreCase(OPTION_PLUGIN)) {
					es.setPlugin(plugin = option.getSecond().asString(pc.getValues()));
				} else if (option.getFirst().get().equalsIgnoreCase(OPTION_CONNECT_USER)) {
					es.setConnectUser(connectUser = option.getSecond().asString(pc.getValues()));
				} else if (option.getFirst().get().equalsIgnoreCase(OPTION_CONFIG)) {
					es.setConfig(config = option.getSecond().asString(pc.getValues()));
				} else if (option.getFirst().get().equalsIgnoreCase(OPTION_AUTOSTART)) {
					es.setAutoStart(autoStart = Boolean.parseBoolean(option.getSecond().asString(pc.getValues())));
				} else if (option.getFirst().get().equalsIgnoreCase(OPTION_USE_DATASTORE)) {
					es.setUsesDataStore(usesDataStore = Boolean.parseBoolean(option.getSecond().asString(pc.getValues())));
				}
			} 
		}
		setPersistent(pc, es, es.getId());
	}
	
	public static PEExternalService load(ExternalService es, SchemaContext pc) {
		PEExternalService pesg = (PEExternalService)pc.getLoaded(es,null);
		if (pesg == null)
			pesg = new PEExternalService(pc, es);
		return pesg;
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return ExternalService.class;
	}

	@Override
	protected ExternalService lookup(SchemaContext pc) throws PEException {
		return pc.getCatalog().findExternalService(externalServiceName);
	}

	@Override
	protected ExternalService createEmptyNew(SchemaContext pc) throws PEException {
		ExternalService es = pc.getCatalog()
				.createExternalService(externalServiceName, plugin,
						connectUser, usesDataStore, config);
		return es;
	}

	@Override
	protected void populateNew(SchemaContext pc, ExternalService p) throws PEException {
	}

	@Override
	protected Persistable<PEExternalService, ExternalService> load(SchemaContext pc,
			ExternalService p) throws PEException {
		return PEExternalService.load(p, pc);
	}

	@Override
	protected int getID(ExternalService p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return null;
	}

	public String getExternalServiceName() {
		return externalServiceName;
	}

	public void setExternalServiceName(String externalServiceName) {
		this.externalServiceName = externalServiceName;
	}

	public String getPlugin() {
		return plugin;
	}

	public void setPlugin(String plugin) {
		this.plugin = plugin;
	}

	public String getConnectUser() {
		return connectUser;
	}

	public void setConnectUser(String connectUser) {
		this.connectUser = connectUser;
	}

	public String getConfig() {
		return config;
	}

	public void setConfig(String config) {
		this.config = config;
	}

	public Boolean isAutoStart() {
		return autoStart;
	}

	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}

	public Boolean getUsesDataStore() {
		return usesDataStore;
	}

	public void setUsesDataStore(Boolean usesDataStore) {
		this.usesDataStore = usesDataStore;
	}

	public void parseOptions(SchemaContext pc,List<Pair<Name,LiteralExpression>> options) {
		if (options == null) {
			return;
		}
		
		for(Pair<Name,LiteralExpression> option : options) {
			if (option.getFirst().get().equalsIgnoreCase(OPTION_PLUGIN)) {
				plugin = option.getSecond().asString(pc.getValues());
			} else if (option.getFirst().get().equalsIgnoreCase(OPTION_CONNECT_USER)) {
				connectUser = option.getSecond().asString(pc.getValues());
			} else if (option.getFirst().get().equalsIgnoreCase(OPTION_CONFIG)) {
				config = option.getSecond().asString(pc.getValues());
			} else if (option.getFirst().get().equalsIgnoreCase(OPTION_AUTOSTART)) {
				autoStart = Boolean.parseBoolean(option.getSecond().asString(pc.getValues()));
			} else if (option.getFirst().get().equalsIgnoreCase(OPTION_USE_DATASTORE)) {
				usesDataStore = Boolean.parseBoolean(option.getSecond().asString(pc.getValues()));
			}
		} 
	}
	
	public String getOptions() {
		StringBuilder buf = new StringBuilder();
		if (plugin != null) {
			buf.append(" plugin='").append(plugin).append("' ");
		}
		
		if (connectUser != null) {
			buf.append(" user='").append(connectUser).append("' ");
		}

		if (connectUser != null) {
			buf.append(" config='").append(config).append("' ");
		}

		if (autoStart != null) {
			buf.append(" autoStart='").append(autoStart.toString()).append("' ");
		}
		
		if (usesDataStore != null) {
			buf.append(" usesDataStore='").append(usesDataStore.toString()).append("' ");
		}

		return buf.toString();
	}
}
