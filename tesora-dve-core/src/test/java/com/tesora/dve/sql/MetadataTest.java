package com.tesora.dve.sql;

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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResizableArray;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.UnaryPredicate;

@Ignore
public class MetadataTest extends SchemaMirrorTest {

	// normalization errors about charsets
	private static final boolean useCharsets = true;
	// normalization errors about timestamps
	private static final boolean useTimestamps = true;
	// or maybe about binary, hard to tell
	private static final boolean useBinary = true;
	
	private static final String TENANT_NAME = "highfive";
	
	private static final int SITES = 3;

	private static final ProjectDDL mtDDL =
		new PEDDL("mtdb",
				new StorageGroupDDL("mt",SITES,"mtg"),
				"schema").withMTMode(MultitenantMode.ADAPTIVE);
	private static final NativeDDL nativeDDL =
		new NativeDDL(TENANT_NAME);
	
	private static final ProjectDDL ddl =
			new PEDDL("mddb",
					new StorageGroupDDL("md",SITES,"mdg"), "database");
					
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return mtDDL;
	}

	@Override
	protected ProjectDDL getSingleDDL() {
		return ddl;
	}

	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
//		setup(ddl,null,nativeDDL,initialize());
//		setup(ddl,mtDDL,nativeDDL,initialize());
		setup(null,null,nativeDDL,initialize());
	}

	@Test
	public void test() throws Throwable {
		System.out.println("here");
	}
	
	// our initialize is to create a bunch of tables
	protected static List<MirrorTest> initialize() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		List<DataType> types = buildTypes();
		List<ColumnAttribute> attrs = buildAttributes();
		List<String> decls = buildTableDecls(types,attrs);
		for(String s : decls) {
//			System.out.println(s);
			out.add(new StatementMirrorProc(s));
		}
		return out;
	}	

	private static List<DataType> buildTypes() {
		ArrayList<DataType> out = new ArrayList<DataType>();
		// date/times
		out.add(new DataType("date",null));
		out.add(new DataType("date","'2014-09-11'"));
		out.add(new DataType("time",null));
		out.add(new DataType("time","'16:22:17'"));
		if (useTimestamps) {
			out.add(new DataType("timestamp",null));
			out.add(new DataType("timestamp","'2014-09-11 16:22:17'"));
		}
		out.add(new DataType("datetime",null));
		out.add(new DataType("datetime","'2014-09-11 09:22:17'"));
		out.add(new DataType("year",null));
		out.add(new DataType("year","'2014'"));
		// integral types
		for(int i : new int[] { -1,1,4 }) {
			out.add(new BitDataType(null,i));
			out.add(new BitDataType("1",i));
		}
		Map<String,int[]> ints = new LinkedHashMap<String,int[]>();
		ints.put("tinyint", new int[] { -1, 10 });
		ints.put("smallint", new int[] { -1, 10 });
		ints.put("mediumint", new int[] { -1, 10 });
		ints.put("int", new int[] { -1, 15 });
		ints.put("bigint",new int[] { -1, 30 });
		for(String tn : ints.keySet()) {
			for(int l : ints.get(tn)) {
				out.add(new IntegralDataType(tn,null,l));
				out.add(new IntegralDataType(tn,"'42'",l));
			}
		}
		// floating point types
		Map<String,int[]> decs = new LinkedHashMap<String,int[]>();
		decs.put("real",new int[] { -1, -1, 5, 3 });
		decs.put("double",new int[] { -1, -1, 15, 10});
		decs.put("float",new int[] { -1, -1, 10, 5 });
		decs.put("decimal", new int[] { -1, -1, 10, -1, 15, 10 });
		decs.put("numeric", new int[] { -1, -1, 10, -1, 15, 10 });
		for(String tn : decs.keySet()) {
			int[] vals = decs.get(tn);
			for(int i = 0; i < vals.length/2; i++) {
				int l = vals[2*i];
				int d = vals[(2*i)+1];
				out.add(new DecimalDataType(tn,null,l,d));
				out.add(new DecimalDataType(tn,"'42.42'",l,d));
			}
		}
		// text types
		String[] textTypes = new String[] { "tinytext", "text", "mediumtext", "longtext" };
		for(String s : textTypes) {
			out.add(new TextDataType(s,null));
		}
		// blob types
		String[] blobTypes = new String[] { "tinyblob", "blob", "mediumblob", "longblob" };
		for(String s : blobTypes) {
			out.add(new BlobDataType(s,null));
		}
		// char, binary can omit the length
		// varchar, varbinary cannot		
		int[] stringLengths = new int[] { -1, 24, 200 };
		String[] stringTypes = new String[] { "char", "varchar" };
		String[] binTypes = new String[] { "binary", "varbinary" };
		for(int l : stringLengths) {
			if (l == -1) {
				out.add(new StringDataType(false,"char",null,l));
				out.add(new StringDataType(false,"char","'Z'",l));
				out.add(new StringDataType(true,"binary",null,l));
				out.add(new StringDataType(true,"binary","'Y'",l));
			} else {
				for(String s : stringTypes) {
					out.add(new StringDataType(false,s,null,l));
					out.add(new StringDataType(false,s,"'lorem ipsum'",l));
				}
				for(String b : binTypes) {
					out.add(new StringDataType(true,b,null,l));
					out.add(new StringDataType(true,b,"'lorem ipsum'",l));
				}
			}
		}
		// wierd types
		out.add(new SetDataType("enum('a','b','c','d')",null));
		out.add(new SetDataType("enum('i','ii','iii','iv','v')","'v'"));
		out.add(new SetDataType("set('I','II','III','IV')",null));
		out.add(new SetDataType("set('A','C','T')","'C,A,T'"));
		return out;
	}
	
	private static List<ColumnAttribute> buildAttributes() {
		ArrayList<ColumnAttribute> out = new ArrayList<ColumnAttribute>();
		out.add(new ColumnAttribute(AttributeType.NULLABLE,"NOT NULL","NULL"){
			@Override
			public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
				// if variant 0, cannot be applied next to a pk
				if (variant == 0) return true;
				for(AttributeValue av : applied)
					if (av.getAttribute().getType() == AttributeType.PRIMARY)
						return false;
				return true;
			}
		});
		out.add(new ColumnAttribute(AttributeType.UNSIGNED,"UNSIGNED") {

			@Override
			public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
				if (dt instanceof IntegralDataType) {
					IntegralDataType idt = (IntegralDataType) dt;
					return !idt.isBit();
				}
				return false;
			}
		});
		out.add(new ColumnAttribute(AttributeType.ZEROFILL,"ZEROFILL") {
			@Override
			public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
				if (dt instanceof IntegralDataType) {
					IntegralDataType idt = (IntegralDataType) dt;
					return !idt.isBit();
				}
				return false;
			}
			
		});
		out.add(new ColumnAttribute(AttributeType.AUTOINCREMENT,"AUTO_INCREMENT") {
			
			@Override
			public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
				return dt.acceptsAuto() && dt.getDefaultValue() == null;
			}
		});
		out.add(new ColumnAttribute(AttributeType.PRIMARY, "PRIMARY KEY") {
			@Override
			public boolean canApply(DataType dt, int varient, Collection<AttributeValue> applied) {
				if (dt.isTextType() || dt.isBlobType()) return false;
//				return !applied.contains(AttributeType.AUTOINCREMENT) && !dt.isTextType();
				return true;
			}

		});
		out.add(new ColumnAttribute(AttributeType.UNIQUE, "UNIQUE", "UNIQUE KEY") {
			@Override
			public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
				if (dt.isTextType() || dt.isBlobType()) return false;
				for(AttributeValue av : applied)
					if (av.getAttribute().getType() == AttributeType.PRIMARY)
						return false;
				return true;
			}
		});
		if (useBinary) {
			out.add(new ColumnAttribute(AttributeType.BINARY, "BINARY") {
				@Override
				public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
					return dt.isTextType();
				}
			});
		}
		if (useCharsets) {
			out.add(new ColumnAttribute(AttributeType.CHARACTER_SET,
					"character set latin1",
					"character set utf8") {
				@Override
				public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
					if (!dt.acceptsCharset()) return false;
					// variant 0 => [0,1]
					// variant 1 => [2,3]
					for(AttributeValue av : applied) {
						if (av.getAttribute().getType() == AttributeType.COLLATE) {
							if (variant == 0)
								return av.getVariant() < 2;
							else
								return av.getVariant() > 1;
						}
					}
					return true;
				}			
			});
			out.add(new ColumnAttribute(AttributeType.COLLATE,
					"collate latin1_swedish_ci",
					"collate latin1_general_ci",
					"collate utf8_general_ci",
					"collate utf8_unicode_ci") {
				@Override
				public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
					if (!dt.acceptsCharset()) return false;
					// inverse:
					// 0 => 0, 1 => 0, 2 => 1, 3 => 1
					for(AttributeValue av : applied) {
						if (av.getAttribute().getType() == AttributeType.CHARACTER_SET) {
							if (variant < 2)
								return av.getVariant() == 0;
							else
								return av.getVariant() == 1;
						}
					}
					return true;
				}						
			});
		}
//		out.add(new ColumnAttribute(AttributeType.COLUMN_FORMAT, "COLUMN_FORMAT FIXED", "COLUMN_FORMAT DYNAMIC", "COLUMN_FORMAT DEFAULT"));
//		out.add(new ColumnAttribute(AttributeType.STORAGE, "STORAGE DISK", "STORAGE MEMORY", "STORAGE DEFAULT"));
		return out;
	}

	private static List<String> buildTableDecls(List<DataType> dataTypes, List<ColumnAttribute> attrTypes) {
		LinkedHashMap<String,ColumnDecl> uniqueDecls = new LinkedHashMap<String,ColumnDecl>();
		TreeMap<String,ColumnDecl> regularDecls = new TreeMap<String,ColumnDecl>();
		// unique decls are those that are declared autoincrement or primary key
		// there is one of each where appropriate; these all go in the uniqueDecls set.
		// regular decls involve every combo of non pk/autoinc attributes.  if there are multiple variants
		// then we do one of each variant.
		ColumnAttribute pk = null;
		ColumnAttribute ai = null;
		List<ColumnAttribute> variables = new ArrayList<ColumnAttribute>();
		for(Iterator<ColumnAttribute> iter = attrTypes.iterator(); iter.hasNext();) {
			ColumnAttribute ca = iter.next();
			if (ca.getType() == AttributeType.AUTOINCREMENT) {
				ai = ca;
				iter.remove();
			} else if (ca.getType() == AttributeType.PRIMARY) {
				pk = ca;
				iter.remove();
			} else if (ca.getVariants() > 1) 
				variables.add(ca);
		}
		List<List<AttributeValue>> nakedUniverse = new ArrayList<List<AttributeValue>>();
		List<List<AttributeValue>> pkUniverse = new ArrayList<List<AttributeValue>>();
		List<List<AttributeValue>> aiUniverse = new ArrayList<List<AttributeValue>>();
		for(ColumnAttribute ca : variables) {
			for(int i = 0; i < ca.getVariants(); i++) {
				List<AttributeValue> combo = new ArrayList<AttributeValue>();
				for(ColumnAttribute ica : attrTypes) {
					if (ica == ca) {
						combo.add(new AttributeValue(ica,i));
					} else {
						combo.add(new AttributeValue(ica,0));
					}
				}
				nakedUniverse.add(combo);
				List<AttributeValue> pkPrefix = new ArrayList<AttributeValue>();
				pkPrefix.add(new AttributeValue(pk, 0));
				pkPrefix.addAll(combo);
				pkUniverse.add(pkPrefix);
				List<AttributeValue> aiPrefix = new ArrayList<AttributeValue>();
				aiPrefix.add(new AttributeValue(ai,0));
				aiPrefix.addAll(combo);
				aiUniverse.add(aiPrefix);
			}
		}

		Set<Set<ColumnAttribute>> ps = Sets.powerSet(new LinkedHashSet<ColumnAttribute>(attrTypes));
		List<List<AttributeValue>> vps = new ArrayList<List<AttributeValue>>();
		for(Set<ColumnAttribute> s : ps) {
			// also off of the powerset arrange for variants
			List<AttributeValue> entry = new ArrayList<AttributeValue>();
			List<ColumnAttribute> variants = new ArrayList<ColumnAttribute>();
			for(ColumnAttribute ca : s) {
				entry.add(new AttributeValue(ca,0));
				if (ca.getVariants() > 1) {
					variants.add(ca);
				}
			}
			vps.add(entry);
			for(ColumnAttribute oca : variants) {
				for(int i = 1; i < oca.getVariants(); i++) {
					List<AttributeValue> ventry = new ArrayList<AttributeValue>();
					for(ColumnAttribute ica : s) {
						if (ica == oca) {
							ventry.add(new AttributeValue(ica,i));
						} else {
							ventry.add(new AttributeValue(ica,0));
						}
					}
					vps.add(ventry);
				}
			}
		}
		
		System.out.println("|nakedUniverse|=" + nakedUniverse.size());
		System.out.println("|pkUniverse|=" + pkUniverse.size());
		System.out.println("|aiUniverse|=" + aiUniverse.size());
		System.out.println("|vps|=" + vps.size());
		
		List<List<AttributeValue>> pkOnly = buildSingleList(pk);
		
		List<DataType> nonTextTypes = Functional.select(dataTypes, new UnaryPredicate<DataType>() {

			@Override
			public boolean test(DataType object) {
				return !(object.isTextType() || object.isBlobType());
			}
			
		});
		
		// pk decls with a ton of attributes; note that we don't generate for text types
		// due to the key length thing, will have to come back to this
		buildColumnDecls(nonTextTypes,pkUniverse,uniqueDecls);
		// ai decls with a ton of attributes
		buildColumnDecls(nonTextTypes,aiUniverse,uniqueDecls);
		// pk only decls
		buildColumnDecls(nonTextTypes,pkOnly,uniqueDecls);
		// no attributes
		buildColumnDecls(dataTypes,null,regularDecls);
		// all combos of attributes
		buildColumnDecls(dataTypes,vps,regularDecls);
		
		System.out.println("|uniqueDecls|=" + uniqueDecls.size());
		System.out.println("|regularDecls|=" + regularDecls.size());
		
		int nreg = (regularDecls.size() / uniqueDecls.size()) + 1;
		
		List<String> out = new ArrayList<String>(uniqueDecls.size());
		int tcounter = 0;
		for(ColumnDecl cd : uniqueDecls.values()) {
			StringBuilder buf = new StringBuilder();
			buf.append("create table `t").append(++tcounter).append("` (").append(PEConstants.LINE_SEPARATOR);
			buf.append("`c0` ").append(cd.getDecl());
			for(int i = 0; i < nreg; i++) {
				if (!regularDecls.isEmpty()) {
					buf.append(",").append(PEConstants.LINE_SEPARATOR);
					ColumnDecl icd = regularDecls.firstEntry().getValue();
					regularDecls.remove(icd.getDecl());
					buf.append("`c").append(i+1).append("` ").append(icd.getDecl());
				}
			}
			buf.append(")");
			out.add(buf.toString());
		}
		
		return out;
	}

	private static List<List<AttributeValue>> buildSingleList(ColumnAttribute ca) {
		List<List<AttributeValue>> out = new ArrayList<List<AttributeValue>>();
		List<AttributeValue> p = new ArrayList<AttributeValue>();
		p.add(new AttributeValue(ca,0));
		out.add(p);
		return out;
	}
	
	private static void buildColumnDecls(List<DataType> dataTypes, List<List<AttributeValue>> universe, 
			Map<String,ColumnDecl> built) {
		for(DataType dt : dataTypes) {
			if (universe == null) {
				ColumnDecl cd = buildDecl(dt,null);
				built.put(cd.getDecl(),cd);
			} else {
				for(List<AttributeValue> combo : universe) {
					ColumnDecl cd = buildDecl(dt,combo);
					built.put(cd.getDecl(),cd);
				}
			}
		}
	}
	
	private static ColumnDecl buildDecl(DataType dt, List<AttributeValue> attrs) {
		StringBuilder buf = new StringBuilder();
		List<AttributeValue> attrApplied = new ArrayList<AttributeValue>();
		ResizableArray<AttributeValue> leftBind = new ResizableArray<AttributeValue>();
		if (attrs != null) {
			for(AttributeValue p : attrs) {
				if (p.canApply(dt, attrApplied)) {
					if (p.getAttribute().getType().getOrder() > -1)
						leftBind.set(p.getAttribute().getType().getOrder(), p);
					attrApplied.add(p);
				}
			}
		}
		buf.append(dt.getType(leftBind));
		for(AttributeValue av : attrApplied) {
			if (av.isOrdered()) continue;
			buf.append(" ").append(av.get());
		}
		return new ColumnDecl(dt,attrApplied,buf.toString());
	}
	
	
	private static class ColumnDecl {
		
		private final DataType dt;
		private final List<AttributeValue> attrs;
		private final String decl;
		
		public ColumnDecl(DataType dt, List<AttributeValue> attrs, String decl) {
			this.dt = dt;
			this.attrs = attrs;
			this.decl = decl;
		}
		
		public String getDecl() {
			return decl;
		}

	}
	
	private static class DataType {
		
		protected final String name;
		protected final String defaultValue;
		
		public DataType(String n, String defVal) {
			this.name = n;
			this.defaultValue = defVal;
		}
		
		protected String get() {
			return name;
		}
		
		public String getDefaultValue() {
			return defaultValue;
		}
		
		protected void addDefaultValue(StringBuilder buf) {
			if (defaultValue != null)
				buf.append(" DEFAULT ").append(defaultValue);
		}

		public String getType(ResizableArray<AttributeValue> leftBindAttrs) {
			StringBuilder buf = new StringBuilder();
			buf.append(get());
			for(int i = 0; i < leftBindAttrs.size(); i++) {
				AttributeValue av = leftBindAttrs.get(i);
				if (av == null) continue;
				buf.append(" ").append(av.get());
			}
			addDefaultValue(buf);
			return buf.toString();
		}
		
		public boolean acceptsAuto() {
			return false;
		}
		
		public boolean acceptsCharset() {
			return false;
		}
		
		public boolean isTextType() {
			return false;
		}
		
		public boolean isBlobType() {
			return false;
		}
		
	}
	
	private static class IntegralDataType extends DataType {
		
		protected final int length; // set to -1 to omit
		
		public IntegralDataType(String name, String defVal, int length) {
			super(name, defVal);
			this.length = length;
		}
		
		protected void addLength(StringBuilder buf) {
			if (length > -1) {
				buf.append("(").append(length).append(")");
			}			
		}
		
		protected String get() {
			StringBuilder buf = new StringBuilder();
			buf.append(super.get());
			addLength(buf);
			return buf.toString();
		}
		
		public boolean isBit() {
			return false;
		}
		
		public boolean acceptsAuto() {
			return true;
		}
	}
	
	private static class BitDataType extends IntegralDataType {
		
		public BitDataType(String defVal, int length) {
			super("BIT",defVal,length);
		}
		
		public boolean isBit() {
			return true;
		}

		public boolean acceptsAuto() {
			return false;
		}
	}
	
	private static class DecimalDataType extends IntegralDataType {
		
		protected final int decimals;
		
		public DecimalDataType(String name, String defVal, int length, int decimals) {
			super(name,defVal, length);
			this.decimals = decimals;
		}
		
		protected void addLength(StringBuilder buf) {
			if (length > -1) {
				buf.append("(").append(length);
				if (decimals > -1)
					buf.append(",").append(decimals);
				buf.append(")");
			}
		}
		
		@Override
		public boolean acceptsAuto() {
			return false;
		}
	}
	
	enum AttributeType {
		NULLABLE,
		UNSIGNED(0),
		ZEROFILL(1),
		AUTOINCREMENT,
		UNIQUE,
		PRIMARY,
		COMMENT,
		COLUMN_FORMAT,
		STORAGE,
		BINARY(0),
		CHARACTER_SET(1),
		COLLATE(2);
		
		private int order;
		
		private AttributeType(int order) {
			this.order = order;
		}
		
		private AttributeType() {
			this.order = -1;
		}
		
		public int getOrder() {
			return order;
		}
	}
	
	private static class ColumnAttribute {
		
		private final AttributeType type;
		private final String[] values;
		
		public ColumnAttribute(AttributeType at, String...values) {
			this.type = at;
			this.values = values;
		}
		
		public int getVariants() {
			return values.length;
		}
		
		public String get(int i) {
			return values[i];
		}
		
		public AttributeType getType() {
			return type;
		}
		
		public boolean canApply(DataType dt, int variant, Collection<AttributeValue> applied) {
			return true;
		}
		
		public String toString() {
			return type.toString();
		}
	}
	
	private static class AttributeValue {
		
		private int variant;
		private ColumnAttribute attr;
		
		public AttributeValue(ColumnAttribute ca, int var) {
			this.attr = ca;
			this.variant = var;
		}
		
		public String get(){
			return attr.get(variant);
		}
		
		public ColumnAttribute getAttribute() {
			return attr;
		}
		
		public boolean canApply(DataType dt, Collection<AttributeValue> applied) {
			return attr.canApply(dt, variant, applied);
		}
		
		public boolean isOrdered() {
			return attr.getType().getOrder() > -1;
		}
		
		public int getVariant() {
			return variant;
		}
	}
	
	private static class TextDataType extends DataType {

		public TextDataType(String n, String defVal) {
			super(n, defVal);
		}
		
		@Override
		public boolean isTextType() {
			return true;
		}
		
		@Override
		public boolean acceptsCharset() {
			return true;
		}
	}
	
	private static class BlobDataType extends DataType {

		public BlobDataType(String n, String defVal) {
			super(n, defVal);
		}

		public boolean isBlobType() {
			return true;
		}
		
	}
	
	private static class StringDataType extends DataType {

		private final int length;
		private final boolean binary;
		
		public StringDataType(boolean binary, String n, String defVal, int length) {
			super(n, defVal);
			this.length = length;
			this.binary = binary;
		}
		
		@Override
		public boolean acceptsCharset() {
			return !binary;
		}
		
		protected String get() {
			if (length == -1)
				return name;
			return String.format("%s(%d)",name,length);
		}

	}
	
	private static class SetDataType extends DataType {

		public SetDataType(String n, String defVal) {
			super(n, defVal);
			// TODO Auto-generated constructor stub
		}
		
		@Override
		public boolean acceptsCharset() {
			return true;
		}
	}
}
