// OS_STATUS: public
package com.tesora.dve.sql.schema.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FloatSizeTypeAttribute;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SizeTypeAttribute;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.StringTypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.util.Functional;

public class BasicType implements Type {
	
	protected static final String COMPARISON_TAG = "COMPARISON";
	
	public static final short UNSIGNED = 1;
	public static final short ZEROFILL = 2;
	public static final short BINARY = 4;
	
	protected NativeType base;
	protected short flags;

	protected BasicType(NativeType nt, short f) {
		base = nt;
		flags = f;
		if (nt == null) 
			throw new IllegalArgumentException("BasicType native type parameter cannot be null"); 
	}

	protected boolean isSet(int f) {
		return (flags & f) != 0;
	}
	
	protected void clearFlag(int f) {
		flags &= ~f;
	}
	
	protected void setFlag(int f) {
		flags |= f;
	}
	
	@Override
	public NativeType getBaseType() { return base; }

	@Override
	public MysqlNativeType getMysqlType() {
		return (MysqlNativeType) base;
	}
	
	@Override
	public boolean isUnsigned() {
		return isSet(UNSIGNED);
	}
		
	@Override
	public boolean isZeroFill() {
		return isSet(ZEROFILL);
	}
	
	@Override
	public boolean isBinaryText() {
		return isSet(BINARY);
	}
	
	@Override
	public int getSize() {
		return 0;
	}
	
	@Override
	public boolean hasSize() {
		return false;
	}
	
	@Override
	public boolean hasPrecisionAndScale() {
		return false;
	}

	@Override
	public int getPrecision() {
		return 0;
	}
	
	@Override
	public int getScale() {
		return 0;
	}

	@Override
	public UnqualifiedName getCharset() {
		return null;
	}
	
	@Override
	public UnqualifiedName getCollation() {
		return null;
	}
	
	@Override
	public Integer getIndexSize() {
		MysqlNativeType nt = (MysqlNativeType) getBaseType();
		if (nt == null) return null;
		switch(nt.getMysqlType()) {
		case BIGINT:
		case DOUBLE:
		case DATETIME:
			return 8;
		case INT:
		case FLOAT:
		case TIMESTAMP:
			return 4;
		case MEDIUMINT:
		case DATE:
		case TIME:
			return 3;
		case SMALLINT:
			return 2;
		case TINYINT:
		case YEAR:
			return 1;
		default:
			return null;
		}
	}
	
	@Override
	public TextType asTextType() {
		return new TextType(getBaseType(),flags,getSize(),null,null);		
	}

	
	@Override
	public String getComparison() {
		return null;
	}
	
	@Override
	public boolean declUsesSizing() { 
		return true; 
	}

	@Override
	public boolean isSerialPlaceholder() {
		return false;
	}
	
	@Override
	public void addColumnTypeModifiers(UserColumn uc) {
		ArrayList<String> entries = new ArrayList<String>();
		if (isUnsigned())
			entries.add(MysqlNativeType.MODIFIER_UNSIGNED);
		if (isZeroFill())
			entries.add(MysqlNativeType.MODIFIER_ZEROFILL);
		if (isBinaryText())
			entries.add("BINARY");
		if (!entries.isEmpty()) 
			uc.setNativeTypeModifiers(Functional.join(entries, " "));
		else if (uc.getId() != 0)
			uc.setNativeTypeModifiers(null);
	}
	
	@Override
	public void addColumnTypeModifiers(CatalogColumnEntity cce) throws PEException {
		ArrayList<String> entries = new ArrayList<String>();
		if (isUnsigned())
			entries.add(MysqlNativeType.MODIFIER_UNSIGNED);
		if (isZeroFill())
			entries.add(MysqlNativeType.MODIFIER_ZEROFILL);
		if (isBinaryText())
			entries.add("BINARY");
		if (!entries.isEmpty())
			cce.setNativeTypeModifiers(Functional.join(entries, " "));
	}
	
	@Override
	public String getTypeName() {
		return getBaseType().getTypeName();
	}
	
	@Override
	public Integer getDataType() {
		return (getBaseType() == null ? null : getBaseType().getDataType());
	}
	
	@Override
	public boolean comparableForDistribution(Type t) {
		if (t instanceof TempColumnType)
			return ((TempColumnType)t).comparableForDistribution(this);
		return getBaseType().getDataType() == t.getBaseType().getDataType()
			&& getSize() == t.getSize();
	}
		
	@Override
	public String getName() {
		StringBuilder buf = new StringBuilder();
        Singletons.require(HostService.class).getDBNative().getEmitter().emitDeclaration(this, null, buf, false);
		return buf.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((base == null) ? 0 : base.hashCode());
		result = prime * result + flags;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BasicType other = (BasicType) obj;
		if (base == null) {
			if (other.base != null)
				return false;
		} else if (!base.equals(other.base))
			return false;
		if (flags != other.flags)
			return false;
		return true;
	}
	
	public static NativeType lookupNativeType(String name) {
		try {
            return Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(name, true);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "No such type: " + name,pe);
		}
	}
	
	/**
	 * @param dataType
	 * @param sizing
	 * @param dbn
	 * @return
	 */
	private static NativeType lookupNativeType(int dataType, int sizing, DBNative dbn) {
		try {
			return dbn.getTypeCatalog().findType(dataType, /* sizing, */true);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "No type for type code: " + dataType,pe);
		}
	}
	
	protected static short buildFlags(List<TypeModifier> modifiers) {
		short flags = 0;
		for(Iterator<TypeModifier> iter = modifiers.iterator(); iter.hasNext();) {
			TypeModifier tm = iter.next();
			if (tm.getKind() == TypeModifierKind.UNSIGNED) {
				flags |= UNSIGNED;
				iter.remove();
			} else if (tm.getKind() == TypeModifierKind.BINARY) {
				flags |= BINARY;
				iter.remove();
			} else if (tm.getKind() == TypeModifierKind.ZEROFILL) {
				flags |= ZEROFILL;
				flags |= UNSIGNED;
				iter.remove();
			} else if (tm.getKind() == TypeModifierKind.SIGNED) {
				// ignore - we're signed by default unless we are unsigned
				iter.remove();
			}
			
		}
		return flags;
	}
	
	protected static String buildStringModifier(TypeModifierKind tk, List<TypeModifier> modifiers) {
		for(Iterator<TypeModifier> iter = modifiers.iterator(); iter.hasNext();) {
			TypeModifier tm = iter.next();
			if (tm.getKind() == tk) {
				iter.remove();
				StringTypeModifier stm = (StringTypeModifier) tm;
				return stm.getValue();
			}
		}
		return null;
	}

	protected static UnqualifiedName buildNameModifier(TypeModifierKind tk, List<TypeModifier> modifiers) {
		String value = buildStringModifier(tk,modifiers);
		if (value != null)
			return new UnqualifiedName(value);
		return null;
	}

	protected static class FlagsAndModifiers {
		
		public short flags;
		public UnqualifiedName charset;
		public UnqualifiedName collation;
		public String comparator;
	}
	
	protected static FlagsAndModifiers buildFlagsAndModifiers(List<TypeModifier> modifiers) {
		FlagsAndModifiers out = new FlagsAndModifiers();
		out.flags = buildFlags(modifiers);
		out.charset = buildNameModifier(TypeModifierKind.CHARSET, modifiers);
		out.collation = buildNameModifier(TypeModifierKind.COLLATE, modifiers);
		out.comparator = buildStringModifier(TypeModifierKind.COMPARISON, modifiers);
		if (!modifiers.isEmpty()) 
			throw new SchemaException(Pass.SECOND, "Unhandled type modifier: " + modifiers.get(0).getKind().getSQL());
		return out;
	}
	
	public static BasicType buildType(NativeType backing, Integer size, List<TypeModifier> modifiers) {
		FlagsAndModifiers fam = buildFlagsAndModifiers(modifiers);
		if (fam.comparator != null)
			return new ComparisonType(backing, fam.flags, (size == null ? 0 : size), fam.charset, fam.collation, fam.comparator);
		else if (fam.charset != null || fam.collation != null)
			return new TextType(backing,fam.flags,(size == null ? 0 : size),fam.charset,fam.collation);
		else if (size != null && size > 0)
			return new SizedType(backing,fam.flags,size);
		else
			return new BasicType(backing,fam.flags);
	}

	public static BasicType buildType(NativeType backing, int size, int precision, int scale, List<TypeModifier> modifiers) {
		FlagsAndModifiers fam = buildFlagsAndModifiers(modifiers);
		if (fam.charset != null || fam.collation != null)
			throw new SchemaException(Pass.SECOND,"Invalid type modifiers, found charset or collation on floating point");
		else if (fam.comparator != null)
			throw new SchemaException(Pass.SECOND,"Invalid type modifiers, found comparator on floating point");
		return new FloatingPointType(backing,fam.flags,size,precision,scale);
	}
	
	public static Type buildType(String typeName, int size, List<TypeModifier> modifiers) {
		ArrayList<TypeModifier> copy = new ArrayList<TypeModifier>(modifiers);
		NativeType bt = lookupNativeType(typeName.toString());
		return buildType(bt,size,copy);
	}
	
	public static BasicType buildType(List<Name> typeNames, List<SizeTypeAttribute> sizes, List<TypeModifier> modifiers) {
		StringBuilder buf = new StringBuilder();
		for(Iterator<Name> iter = typeNames.iterator(); iter.hasNext();) {
			buf.append(iter.next().getCapitalized().get());
			if (iter.hasNext())
				buf.append(" ");
		}
		String str = buf.toString();
		BasicType any = handleSerialType(str);
		if (any != null) return any;
		NativeType bt = lookupNativeType(str);
		// there could be multiple sizing hints - collapse them down to one 
		FloatSizeTypeAttribute floatSizing = null;
		SizeTypeAttribute sizing = null;
		for(SizeTypeAttribute sta : sizes) {
			if (sta instanceof FloatSizeTypeAttribute)
				floatSizing = (FloatSizeTypeAttribute)sta;
			else
				sizing = sta;
		}
		if (floatSizing != null && !bt.getSupportsPrecision()) { 
			throw new SchemaException(Pass.SECOND, "Type " + bt.getTypeName() + " does not support precision/scale");
		}
		if (floatSizing != null && sizing != null) {
			throw new SchemaException(Pass.SECOND, "Cannot specify both sizing and precision");
		}
		if (floatSizing != null)
			return buildType(bt,floatSizing.getSize(),floatSizing.getPrecision(),floatSizing.getScale(),modifiers);
		return buildType(bt,(sizing == null ? 0 : sizing.getSize()),modifiers);
	}

	public static Type buildType(int dataType, int sizing, DBNative dbn) {
		NativeType nativeType = lookupNativeType(dataType, sizing, dbn);
		if (sizing > 0)
			return new SizedType(nativeType,(short)0,sizing);
		return new BasicType(nativeType,(short)0);		
	}
	
	public static Type buildType(UserColumn uc, NativeTypeCatalog types) {
		MysqlNativeType mnType = (MysqlNativeType)lookupNativeType(uc.getNativeTypeName());
		if (MysqlType.ENUM.equals(mnType.getMysqlType()) || MysqlType.SET.equals(mnType.getMysqlType()))
			return DBEnumType.buildType(uc, types);
		List<TypeModifier> modifiers = buildModifiers(uc);
		if (uc.getPrecision() != 0 || uc.getScale() != 0) {
			return buildType(mnType,uc.getSize(),uc.getPrecision(),uc.getScale(),modifiers);
		}
		return buildType(mnType,uc.getSize(),modifiers);
	}
	
	public static List<TypeModifier> buildModifiers(UserColumn uc) {
		List<TypeModifier> modifiers = new ArrayList<TypeModifier>();
		String mods = uc.getNativeTypeModifiers();
		if (mods != null) {
			if (mods.contains(MysqlNativeType.MODIFIER_UNSIGNED))
				modifiers.add(new TypeModifier(TypeModifierKind.UNSIGNED));
			if (mods.contains(MysqlNativeType.MODIFIER_ZEROFILL))
				modifiers.add(new TypeModifier(TypeModifierKind.ZEROFILL));
			int offset = mods.indexOf(COMPARISON_TAG);
			if (offset > -1) {
				int boundary = offset + COMPARISON_TAG.length();
				int nextSpace = mods.indexOf(" ", boundary);
				String value = mods.substring(boundary,nextSpace);
				modifiers.add(new StringTypeModifier(TypeModifierKind.COMPARISON, value));
			}
			if (mods.contains("BINARY") || mods.contains("binary"))
				modifiers.add(new TypeModifier(TypeModifierKind.BINARY));
		}
		
		
		if (uc.getCharset() != null)
			modifiers.add(new StringTypeModifier(TypeModifierKind.CHARSET, uc.getCharset()));
		if (uc.getCollation() != null) 
			modifiers.add(new StringTypeModifier(TypeModifierKind.COLLATE, uc.getCollation()));
		
		return modifiers;
	}
	
	@Override
	public boolean mustParameterize() {
		return isBinaryType() || isStringType();
	}

	@Override
	public boolean isBinaryType() {
		return getBaseType().isBinaryType();
	}
	
	@Override
	public boolean supportsDefaultValue() {
		return getBaseType().supportsDefaultValue();
	}
	
	@Override
	public boolean isStringType() {
		return getBaseType().isStringType();
	}

	@Override
	public boolean isFloatType() {
		return getBaseType().isFloatType();
	}
	
	@Override
	public boolean isNumericType() {
		return getBaseType().isNumericType();
	}
	
	@Override
	public boolean isDecimalType() {
		return getBaseType().isDecimalType();
	}
	
	@Override
	public boolean isIntegralType() {
		return getBaseType().isIntegralType();
	}
	
	@Override
	public boolean isBitType() {
		return getMysqlType().getMysqlType() == MysqlType.BIT;
	}
	
	@Override
	public boolean isTimestampType() {
		return getBaseType().isTimestampType();
	}
	
	@Override
	public boolean asKeyRequiresPrefix() {
		return getBaseType().asKeyRequiresPrefix();
	}
	
	@Override
	public LiteralExpression getZeroValueLiteral() {
		Object value = null;
		try {
			value = getBaseType().getZeroValue();
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to determine zero value", pe);
		}
		if (value == null)
			return LiteralExpression.makeNullLiteral();
		else if (value instanceof String)
			return LiteralExpression.makeStringLiteral((String)value);
		else if (value instanceof Number)
			return LiteralExpression.makeLongLiteral(((Number)value).longValue());
		else
			throw new SchemaException(Pass.SECOND, "Unhandled zero value: " + value + " (type=" + value.getClass().getSimpleName() + ")");
	}
	
	@SuppressWarnings("unchecked")
	public static Type buildFromLiteralExpression(SchemaContext sc, LiteralExpression literal) throws PEException {
		if (literal.isNumericLiteral()) {
			if (literal.isFloatLiteral()) {
                NativeType nt = Singletons.require(HostService.class).getDBNative().findType("DECIMAL");
				return BasicType.buildType(nt, 0, (int)nt.getPrecision(), nt.getMaximumScale(), Collections.EMPTY_LIST);
			}
			// assume integer
            return BasicType.buildType(java.sql.Types.BIGINT, 21, Singletons.require(HostService.class).getDBNative());
		}
		// get the length of the string
		String value = (String)(literal.getValue(sc));
        return BasicType.buildType(java.sql.Types.VARCHAR, value.length(), Singletons.require(HostService.class).getDBNative());
	}
	
	public static Type getLongType() {
		return buildType("INT");
	}
	public static Type getDateTimeType() {
		return buildType("DATETIME");
	}
	
	private static Type buildType(String nativeName) {
		NativeType nt = lookupNativeType(nativeName);
		return new BasicType(nt, (short)0);
	}
	
	public static class SerialPlaceholderType extends BasicType {
		
		public SerialPlaceholderType() {
			super(lookupNativeType("BIGINT"),UNSIGNED);
		}
		
		@Override
		public boolean isSerialPlaceholder() {
			return true;
		}
		
		public Type convert() {
			return new BasicType(base,flags);
		}
	}
	
	private static BasicType handleSerialType(String tn) {
		if ("SERIAL".equalsIgnoreCase(tn)) {
			return new SerialPlaceholderType();
		}
		return null;
	}
	
	@Override
	public BasicType normalize() {
		if (base instanceof MysqlNativeType) {
			MysqlNativeType mnt = (MysqlNativeType) base;
			Normalizer n = normalizers.get(mnt.getMysqlType());
			if (n != null)
				return n.normalize(this);
		}
		return this;
	}
	
	private static final Map<MysqlNativeType.MysqlType, Normalizer> normalizers = buildNormalizers();
	
	@SuppressWarnings("synthetic-access")
	private static Map<MysqlNativeType.MysqlType,Normalizer> buildNormalizers() {
		HashMap<MysqlNativeType.MysqlType,Normalizer> out = new HashMap<MysqlNativeType.MysqlType,Normalizer>();
		out.put(MysqlType.BIGINT, new IntegralTypeNormalizer(20,20));
		out.put(MysqlType.INT, new IntegralTypeNormalizer(11,10));
		out.put(MysqlType.SMALLINT, new IntegralTypeNormalizer(6,5));
		out.put(MysqlType.MEDIUMINT, new IntegralTypeNormalizer(9,8));
		out.put(MysqlType.TINYINT, new IntegralTypeNormalizer(4,3));
		out.put(MysqlType.DECIMAL, new DecimalTypeNormalizer());
		out.put(MysqlType.BIT, new FixedSizeNormalizer(1));
		out.put(MysqlType.YEAR, new FixedSizeNormalizer(4));
		out.put(MysqlType.CHAR, new StringTypeNormalizer(true));
		out.put(MysqlType.VARCHAR, new StringTypeNormalizer(true));
		out.put(MysqlType.BINARY, new FixedSizeNormalizer(1));
		out.put(MysqlType.TEXT, new StringTypeNormalizer(false));
		out.put(MysqlType.TINYTEXT, new StringTypeNormalizer(false));
		out.put(MysqlType.MEDIUMTEXT, new StringTypeNormalizer(false));
		out.put(MysqlType.LONGTEXT, new StringTypeNormalizer(false));
		return out;
	}
	
	private static abstract class Normalizer {
		
		public abstract BasicType normalize(BasicType t);

	}
	
	private static class IntegralTypeNormalizer extends Normalizer {
		
		int signedSize;
		int unsignedSize;
		
		@SuppressWarnings("synthetic-access")
		public IntegralTypeNormalizer(int signed, int unsigned) {
			signedSize = signed;
			unsignedSize = unsigned;
		}
		
		@Override
		public BasicType normalize(BasicType t) {
			if (t.getSize() > 0) return t;
			String comparator = t.getComparison();
			int newSize = (t.isUnsigned() ? unsignedSize : signedSize);
			if (comparator == null) {
				return new SizedType(t.getBaseType(),t.flags,newSize);
			}
			return new ComparisonType(t.getBaseType(), t.flags, newSize, t.getCharset(), t.getCollation(), t.getComparison());
		}
	}
	
	@SuppressWarnings("synthetic-access")
	private static class DecimalTypeNormalizer extends Normalizer {
		
		@Override
		public BasicType normalize(BasicType t) {
			if (t.getPrecision() > 0) return t;
			int prec = 0;
			int scale = 0;
			if (t.getSize() > 0)
				prec = t.getSize();
			else
				prec = 10;
			String comparator = t.getComparison();
			if (comparator == null) {
				return new FloatingPointType(t.getBaseType(),t.flags,prec,prec,scale);
			}
			throw new SchemaException(Pass.NORMALIZE, "Invalid type modifier: comparator on floating point type");
		}
	}
	
	private static class FixedSizeNormalizer extends Normalizer {
		
		private final int size;
		
		@SuppressWarnings("synthetic-access")
		public FixedSizeNormalizer(int s) {
			super();
			size = s;
		}

		@Override
		public BasicType normalize(BasicType t) {
			if (t.getSize() > 0) return t;
			String comparator = t.getComparison();
			if (comparator == null) {
				return new SizedType(t.getBaseType(),t.flags,size);
			}
			return new ComparisonType(t.getBaseType(),t.flags,size,t.getCharset(),t.getCollation(),comparator);
		}

	}
	
	private static class StringTypeNormalizer extends Normalizer {
		
		private final boolean sized;
		
		@SuppressWarnings("synthetic-access")
		public StringTypeNormalizer(boolean hasSize) {
			super();
			sized = hasSize;
		}
		
		@Override
		public BasicType normalize(BasicType t) {
			if (t.isBinaryText()) {
				if (t.getCharset() != null && t.getCollation() != null && t.getSize() > 0)
					return t;
				int ns = t.getSize();
				if (ns == 0 && sized)
					ns = 1;
				UnqualifiedName cs = t.getCharset();
				if (cs == null)
                    cs = new UnqualifiedName(Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet());
				UnqualifiedName coll = t.getCollation();
				if (coll == null)
                    coll = new UnqualifiedName(Singletons.require(HostService.class).getDBNative().getDefaultServerBinaryCollation());
				String comparator = t.getComparison();
				if (comparator != null)
					return new ComparisonType(t.getBaseType(),t.flags,ns,cs,coll,comparator);
				return new TextType(t.getBaseType(),t.flags,ns,cs,coll);
			}
			if (t.getSize() > 0)
				return t;
			int ns = t.getSize();
			if (ns == 0 && sized)
				ns = 1;
			UnqualifiedName cs = t.getCharset();
			UnqualifiedName coll = t.getCollation();
			String comparator = t.getComparison();
			if (comparator != null)
				return new ComparisonType(t.getBaseType(),t.flags,ns,cs,coll,comparator);
			else if (cs != null || coll != null)
				return new TextType(t.getBaseType(),t.flags,ns,cs,coll);
			else
				return new SizedType(t.getBaseType(),t.flags,ns);
		}
	}
	
	// determining whether types are range dist compatible.  the default is they are only compatible if they
	// are exactly the same (upto flags like unsigned, zerofill).  we relax these requirements for some types.
	@Override
	public boolean isAcceptableColumnTypeForRangeType(Type columnType) {
		if (isFloatType())
			return false;
		if (columnType.isFloatType() || columnType.isNumericType())
			return false;
		if (getMysqlType().getMysqlType() != columnType.getMysqlType().getMysqlType())
			return false;
		return true;
	}       

	// generally types are acceptable
	@Override
	public boolean isAcceptableRangeType() {
		if (getMysqlType().getMysqlType() == MysqlType.SET)
			return false;
		if (isFloatType() || isDecimalType() || isBitType())
			return false;
		return true;
	}

	@Override
	public boolean isUnknown() {
		return false;
	}

	@Override
	public TextType toTextType() {
		if (this.isStringType()) {
			if (!(this instanceof TextType)) {
				return this.asTextType();
			}

			return (TextType) this;
		}

		throw new PECodingException("Type '" + this.getName() + "' cannot be converted to text.");
	}

}