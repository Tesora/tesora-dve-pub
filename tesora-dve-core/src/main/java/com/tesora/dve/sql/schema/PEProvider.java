// OS_STATUS: public
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.siteprovider.SiteProviderPlugin;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderFactory;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCollectionCacheKey;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.UnaryFunction;

public class PEProvider extends Persistable<PEProvider, Provider> {
	
	protected String plugin;
	protected boolean active;

	public PEProvider(SchemaContext pc, Name providerName, String plugin, boolean isActive) {
		super(getProviderKey(providerName.getUnquotedName().get()));
		setName(providerName);
		this.plugin = plugin;
		this.active = isActive;
	}
	
	public PEProvider(SchemaContext pc, Name providerName, ListOfPairs<Name,LiteralExpression> opts) {
		super(getProviderKey(providerName.getUnquotedName().get()));
		Boolean isActive = null;
		if (isActive == null) isActive = Boolean.TRUE;
		active = isActive.booleanValue();
	}
	
	public static PEProvider load(Provider p, SchemaContext sc) {
		PEProvider pep = (PEProvider)sc.getLoaded(p,getProviderKey(p.getName()));
		if (pep == null)
			pep= new PEProvider(p, sc);
		return pep;
	}
	
	private PEProvider(Provider p, SchemaContext sc) {
		super(getProviderKey(p.getName()));
		sc.startLoading(this,p);
		setName(new UnqualifiedName(p.getName()));
		plugin = p.getPlugin();
		active = p.isEnabled();
		setPersistent(sc,p, p.getId());
		sc.finishedLoading(this, p);
	}
 	
	public String getPlugin() {
		return plugin;
	}
	
	public boolean isActive() {
		return active;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Provider.class;
	}

	@Override
	protected Provider lookup(SchemaContext pc) throws PEException {
		return pc.getCatalog().findProvider(name.get());
	}

	@Override
	protected Provider createEmptyNew(SchemaContext pc) throws PEException {
		return new Provider(name.get(), plugin);
	}

	@Override
	protected void populateNew(SchemaContext pc, Provider p) throws PEException {
		p.setIsEnabled(active);
	}

	@Override
	protected Persistable<PEProvider, Provider> load(SchemaContext pc, Provider p)
			throws PEException {
		return null;
	}

	@Override
	protected int getID(Provider p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return "Dynamic Site Provider";
	}

	public static SchemaCacheKey<?> getProviderKey(String n) {
		return new ProviderCacheKey(n);
	}
	
	public static class ProviderCacheKey extends SchemaCacheKey<PEProvider> {

		private static final long serialVersionUID = 1L;
		private final String name;
		public ProviderCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEProvider.class,name.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof ProviderCacheKey) {
				ProviderCacheKey pck = (ProviderCacheKey) o;
				return name.equals(pck.name);
			}
			return false;
		}

		@Override
		public PEProvider load(SchemaContext sc) {
			Provider p = sc.getCatalog().findProvider(name);
			if (p == null) return null;
			return PEProvider.load(p, sc);
		}

		@Override
		public String toString() {
			return "PEProvider:" + name;
		}
		
	}
	
	public static class AllProvidersCacheKey extends SchemaCollectionCacheKey<PEProvider> {

		private static final long serialVersionUID = 1L;
		private static final AllProvidersCacheKey key = new AllProvidersCacheKey();
		private final String name = "AllProviders";
		
		private AllProvidersCacheKey() {
			super();
		}
		
		public static Collection<PEProvider> get(SchemaContext sc) {
			return key.resolve(sc);
		}
		
		@Override
		public int hashCode() {
			return addHash(Collection.class.hashCode(),addHash(PEProvider.class.hashCode(),name.hashCode()));
		}

		@Override
		public boolean equals(Object o) {
			return (o instanceof AllProvidersCacheKey);
		}

		@Override
		public Collection<PEProvider> find(final SchemaContext sc) {
			List<Provider> providers = sc.getCatalog().findAllProviders();
			return Functional.apply(providers, new UnaryFunction<PEProvider,Provider>() {

				@Override
				public PEProvider evaluate(Provider object) {
					return PEProvider.load(object, sc);
				}
				
			});
		}

		@Override
		public String toString() {
			return name;
		}
		
	}
	
	public static Collection<StorageSite> findAllDynamicSites(SchemaContext sc) throws PEException {
		ArrayList<StorageSite> sites = new ArrayList<StorageSite>();
		for(PEProvider pep : sc.findAllProviders()) {
			SiteProviderPlugin sm = SiteProviderFactory.getInstance(pep.getName().getUnquotedName().get());
			sites.addAll(sm.getAllSites());
		} 
		return sites;
	}

}
