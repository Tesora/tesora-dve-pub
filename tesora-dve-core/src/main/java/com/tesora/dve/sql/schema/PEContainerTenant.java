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
import com.tesora.dve.common.catalog.ContainerTenant;
import com.tesora.dve.common.catalog.ITenant;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.util.Pair;

public class PEContainerTenant extends
		Persistable<PEContainerTenant, ContainerTenant> implements IPETenant {

	private SchemaEdge<PEContainer> container;
	private String discriminant;
	
	@SuppressWarnings("unchecked")
	public PEContainerTenant(SchemaContext sc, PEContainer cont, String disc) {
		super(getContainerTenantKey(cont,disc));
		setName(null);
		if (cont != null)
			container = StructuralUtils.buildEdge(sc,cont, false);
		if (container != null && disc == null)
			throw new SchemaException(Pass.SECOND, "Invalid discriminant");
		discriminant = disc;		
		setPersistent(sc,null,null);
	}
	
	public PEContainer getContainer(SchemaContext sc) {
		if (container == null) return null;
		return container.get(sc);
	}
	
	public SchemaCacheKey<PEContainer> getContainerCacheKey() {
		if (container == null) return null;
		return container.getCacheKey();
	}
	public static PEContainerTenant load(ContainerTenant container, SchemaContext pc) {
		PEContainerTenant peContainer = (PEContainerTenant) pc.getLoaded(container,getContainerTenantKey(container));
		if (peContainer == null)
			peContainer = new PEContainerTenant(pc, container);
		return peContainer;
	}

	@SuppressWarnings("unchecked")
	private PEContainerTenant(SchemaContext sc, ContainerTenant ct) {
		super(getContainerTenantKey(ct));
		sc.startLoading(this, ct);
		setPersistent(sc, ct, ct.getId());
		setName(null);
		if (ct.isGlobalTenant()) {
			discriminant = null;
			container = null;
		} else {
			discriminant = ct.getDiscriminant();
			container = StructuralUtils.buildEdge(sc,PEContainer.load(ct.getContainer(),sc), true);
		}
		sc.finishedLoading(this, ct);
	}
	
	public String getDiscriminant() {
		return discriminant;
	}
	
	@Override
	public String getUniqueIdentifier() {
		return discriminant;
	}
	
	@Override
	public boolean isGlobalTenant() {
		return container == null && discriminant == null;
	}
	
	@Override
	public Long getTenantID() {
		if (getPersistentID() == null) return null;
		return new Long(getPersistentID());
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return ContainerTenant.class;
	}

	@Override
	protected int getID(ContainerTenant p) {
		return p.getId();
	}

	@Override
	protected ContainerTenant lookup(SchemaContext sc) throws PEException {
		return null;
	}

	@Override
	protected ContainerTenant createEmptyNew(SchemaContext sc)
			throws PEException {
		if (discriminant == null)
			return null;
		return new ContainerTenant(container.get(sc).getPersistent(sc),discriminant);
	}

	@Override
	protected void populateNew(SchemaContext sc, ContainerTenant p)
			throws PEException {
	}

	@Override
	protected Persistable<PEContainerTenant, ContainerTenant> load(
			SchemaContext sc, ContainerTenant p) throws PEException {
		return null;
	}

	@Override
	public ContainerTenant getPersistent(SchemaContext pc, boolean create) {
		if (discriminant == null)
			return ContainerTenant.GLOBAL_CONTAINER_TENANT;
		return super.getPersistent(pc,create);
	}
	
	@Override
	public ITenant getPersistentTenant(SchemaContext sc) throws PEException {
		return getPersistent(sc);
	}
	
	@Override
	protected String getDiffTag() {
		return null;
	}
	
	public static String buildDiscriminantValue(SchemaContext sc, ConnectionValues cv, List<Pair<PEColumn,LiteralExpression>> orderedValues) { 
		StringBuilder buf = new StringBuilder();
        Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		for(Pair<PEColumn,LiteralExpression> p : orderedValues) {
			if (buf.length() > 0)
				buf.append(",");
			LiteralExpression litex = p.getSecond();
			buf.append(p.getFirst().getName().get()).append(":");
			emitter.emitLiteral(cv, litex, buf);
		}
		return buf.toString();
	}
	
	private static final ContainerTenantCacheKey globalContainerTenantCacheKey = new ContainerTenantCacheKey("GLOBAL","UNUSED");
	
	public static SchemaCacheKey<PEContainerTenant> getContainerTenantKey(ContainerTenant ct) {
		if (ct.isGlobalTenant()) return globalContainerTenantCacheKey;
		return new ContainerTenantCacheKey(ct.getContainer().getName(), ct.getDiscriminant());
	}
	
	public static SchemaCacheKey<PEContainerTenant> getContainerTenantKey(PEContainer cont, String disc) {
		if (cont == null && disc == null) return globalContainerTenantCacheKey; 
		return new ContainerTenantCacheKey(cont.getName().getUnquotedName().get(),disc);
	}
	
	public static class ContainerTenantCacheKey extends SchemaCacheKey<PEContainerTenant> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String containerName;
		private String discriminant;
		
		public ContainerTenantCacheKey(String ofContainer, String keyvals) {
			super();
			containerName = ofContainer;
			discriminant = keyvals;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(PEContainerTenant.class,containerName.hashCode()),discriminant.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEContainerTenant:" + containerName + "(" + discriminant + ")";
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof ContainerTenantCacheKey) {
				ContainerTenantCacheKey octk = (ContainerTenantCacheKey) o;
				return octk.containerName.equals(containerName) && octk.discriminant.equals(discriminant);
			}
			return false;
		}

		@Override
		public PEContainerTenant load(SchemaContext sc) {
			ContainerTenant ten = null;
			if (isGlobalTenant())
				ten = ContainerTenant.GLOBAL_CONTAINER_TENANT;
			else
				ten = sc.getCatalog().findContainerTenant(containerName, discriminant);
			if (ten == null) return null;
			return PEContainerTenant.load(ten, sc);
		}

		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.TENANT;
		}

		public boolean isGlobalTenant() {
			return "GLOBAL".equals(containerName) && "UNUSED".equals(discriminant);
		}
		
	}

}
