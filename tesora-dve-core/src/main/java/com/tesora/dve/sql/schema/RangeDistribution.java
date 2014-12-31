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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.compare.ComparatorCache;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.modifiers.StringTypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class RangeDistribution extends Persistable<RangeDistribution, DistributionRange> {

	private static final String SIGNATURE_SEPARATOR = ":";
	private final List<Type> types;
	private List<NativeType> nativeTypes = null;
	private final SchemaEdge<PEPersistentGroup> psg;
	private final String signature;
	
	@SuppressWarnings("unchecked")
	public RangeDistribution(SchemaContext pc, Name n, List<BasicType> rangeTypes, PEPersistentGroup sg) {
		super(buildRangeKey(n,sg.getName()));
		setName(n);
		setPersistent(pc, null,null);
		types = new ArrayList<Type>();
		for(Type t : rangeTypes) {
			Type nt = t.normalize();
			if (!nt.isAcceptableRangeType()) 
				throw new SchemaException(Pass.SECOND, "Invalid type for range distribution: " + t.getName());
			types.add(nt);
		}
		psg = StructuralUtils.buildEdge(pc,sg, false);
		signature = Functional.join(types, ",", new UnaryFunction<String, Type>(){

			@Override
			public String evaluate(Type object) {
				String compClass = object.getComparison();
				String comparator = StringUtils.EMPTY;
				if (compClass != null) {
					comparator = compClass;
					ComparatorCache.add(comparator);
					comparator = SIGNATURE_SEPARATOR + comparator;
				}
				return object.getBaseType().getTypeName() + comparator;
			}
			
		});
		nativeTypes = null;
	}
	
	@SuppressWarnings("unchecked")
	private RangeDistribution(DistributionRange dr, SchemaContext lc) {
		super(buildRangeKey(dr.getName(),dr.getStorageGroup().getName()));
		lc.startLoading(this,dr);
		setName(new UnqualifiedName(dr.getName()));
		setPersistent(lc, dr,dr.getId());
		signature = dr.getSignature();
		psg = StructuralUtils.buildEdge(lc,PEPersistentGroup.load(dr.getStorageGroup(), lc),true);
		String[] typeNames = signature.split(",");
		ArrayList<Type> buf = new ArrayList<Type>();
		for(String tn : typeNames) try {
			String[] sigParts = tn.split(SIGNATURE_SEPARATOR);
			List<TypeModifier> modifiers = new ArrayList<TypeModifier>();
			if (sigParts.length>1) {
				modifiers.add(new StringTypeModifier(TypeModifierKind.COMPARISON, sigParts[1]));
			}
			BasicType t = BasicType.buildType(lc.getTypes().findType(sigParts[0], true), 0, modifiers).normalize();
			buf.add(t);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unknown type in range distribution type signature: '" + tn + "'",pe);
		}
		types = buf;
		lc.finishedLoading(this, dr);
		nativeTypes = null;
	}
	
	public static RangeDistribution load(DistributionRange dr, SchemaContext lc) {
		RangeDistribution rd = (RangeDistribution)lc.getLoaded(dr,buildRangeKey(dr.getName(),dr.getStorageGroup().getName()));
		if (rd == null)
			rd = new RangeDistribution(dr, lc);
		return rd;
	}
	
	public List<NativeType> getNativeTypes() {
		if (nativeTypes == null) {
			nativeTypes = new ArrayList<NativeType>();
			for(Type t : types) {
				nativeTypes.add(t.getBaseType());
			}
		}
		return nativeTypes;
	}
	
	public List<Type> getTypes() {
		return types;
	}
	
	public String getSignature() {
		return signature;
	}
	
	public PEPersistentGroup getStorageGroup(SchemaContext sc) {
		return psg.get(sc);
	}
	
	public void validate(SchemaContext sc, List<PEColumn> vectorColumns) {
		if (types.size() > vectorColumns.size()) {
			throw new SchemaException(Pass.SECOND, "Not enough columns specified to use range " + getName().getSQL());
		} else if (types.size() < vectorColumns.size()) {
			throw new SchemaException(Pass.SECOND, "Too many columns specified to use range " + getName().getSQL());
		}
		Iterator<PEColumn> citer = vectorColumns.iterator();
		Iterator<Type> titer = types.iterator();
		while(citer.hasNext() && titer.hasNext()) {
			PEColumn c = citer.next();
			Type t = titer.next();
			if (!t.isAcceptableColumnTypeForRangeType(c.getType())) {
				StringBuilder buf = new StringBuilder();
                Emitter emitter = Singletons.require(DBNative.class).getEmitter();
				buf.append("Column ");
				emitter.emitDeclaration(sc,sc.getValues(), c, buf);
				buf.append(" cannot be used with range ").append(getName().getSQL()).append(", incompatible type: ");
				emitter.emitDeclaration(t, null, buf, true);
				throw new SchemaException(Pass.SECOND, buf.toString());
				
			}
		}
	}
	
	public boolean comparableForDistribution(SchemaContext sc, RangeDistribution rd) {
		return getName().equals(rd.getName()) &&
			getStorageGroup(sc).equals(rd.getStorageGroup(sc)) &&
			getSignature().equals(rd.getSignature());
	}
	
	@Override
	protected String getDiffTag() {
		return "RangeDistribution";
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<RangeDistribution, DistributionRange> other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		RangeDistribution ord = other.get();
		
		if (visited.contains(this) && visited.contains(ord)) {
			return false;
		}
		visited.add(this);
		visited.add(ord);

		if (maybeBuildDiffMessage(sc, messages,"name",getName(), other.getName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages,"persistent group", getStorageGroup(sc), ord.getStorageGroup(sc), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages,"signature", getSignature(), ord.getSignature(), first, visited))
			return true;
		return false;
	}

	@Override
	public Persistable<RangeDistribution, DistributionRange> reload(SchemaContext usingContext) {
		return usingContext.findRange(getName(),getStorageGroup(usingContext).getName());
	}

	@Override
	protected int getID(DistributionRange p) {
		return p.getId();
	}

	@Override
	protected DistributionRange lookup(SchemaContext pc) throws PEException {
		String persistName = getName().get();
		return pc.getCatalog().findDistributionRange(persistName, psg.get(pc).getName().getUnquotedName().get());
	}

	@Override
	protected DistributionRange createEmptyNew(SchemaContext pc) throws PEException {
		PersistentGroup onGroup = psg.get(pc).persistTree(pc);
		DistributionRange dr = new DistributionRange(name.get(), onGroup, signature);
		pc.getSaveContext().add(this,dr);
		return dr;
	}

	@Override
	protected void populateNew(SchemaContext pc, DistributionRange p) throws PEException {
	}

	@Override
	protected Persistable<RangeDistribution, DistributionRange> load(SchemaContext pc, 
			DistributionRange p) throws PEException {
		return RangeDistribution.load(p, pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return DistributionRange.class;
	}
	
	public static SchemaCacheKey<RangeDistribution> buildRangeKey(Name rangeName, Name groupName) {
		return buildRangeKey(rangeName.getUnquotedName().get(), groupName.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<RangeDistribution> buildRangeKey(String rangeName, String groupName) {
		return new RangeDistributionCacheKey(rangeName, groupName);
	}
		
	public static class RangeDistributionCacheKey extends SchemaCacheKey<RangeDistribution> {

		private static final long serialVersionUID = 1L;
		private final String name;
		private final String group;
		
		public RangeDistributionCacheKey(String n, String group) {
			super();
			this.name = n;
			this.group  = group;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(RangeDistribution.class, name.hashCode()),group.hashCode());
		}
		
		@Override
		public String toString() {
			return "RangeDistribution:" + name + ":" + group;
		}
		
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof RangeDistributionCacheKey) {
				RangeDistributionCacheKey rdk = (RangeDistributionCacheKey) o;
				return name.equals(rdk.name) && group.equals(rdk.group);
			}
			return false;
		}

		@Override
		public RangeDistribution load(SchemaContext sc) {
			DistributionRange dr = sc.getCatalog().findDistributionRange(name, group);
			if (dr == null) return null;
			return RangeDistribution.load(dr, sc);
		}
		
	}
}
