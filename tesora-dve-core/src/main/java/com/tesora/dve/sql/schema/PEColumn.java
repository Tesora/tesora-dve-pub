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




import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.Traversable;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.IdentifierLiteralExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.modifiers.CharsetTableModifier;
import com.tesora.dve.sql.schema.modifiers.CollationTableModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifierKind;
import com.tesora.dve.sql.schema.modifiers.DefaultValueModifier;
import com.tesora.dve.sql.schema.mt.TenantColumn;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.BasicType.SerialPlaceholderType;
import com.tesora.dve.sql.schema.types.TextType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Accessor;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

public class PEColumn extends Persistable<PEColumn, UserColumn> 
	implements HasComment, TableComponent<PEColumn>, Column<PEAbstractTable<?>>, PersistentColumn {
	static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
	
	protected static final String ON_UPDATE_TAG = "ON UPDATE CURRENT_TIMESTAMP";
	
	private static final short NOT_NULLABLE = 1;
	private static final short AUTO_INCREMENT = 2;
	private static final short ONUPDATE = 4;
	private static final short KEY_PART = 8;
	private static final short PRIMARY_KEY_PART = 16;
	private static final short UNIQUE_KEY_PART = 32;
	
	protected Type type;
	protected short flags = 0;
	protected ExpressionNode defaultValue = null;
	protected int dvposition = 0;
	protected int cdv_position = 0;
	protected PEAbstractTable<?> ofTable;
	protected int position = -1;
	
	private Comment comment = null;

	public static PEColumn buildColumn(SchemaContext sc, Name name, Type type, List<ColumnModifier> modifiers, Comment comment, List<ColumnKeyModifier> inlineKeys) {
		ExpressionNode defaultValue = null;
		short f = 0;
		boolean explicitNullability = false;
		for(Iterator<ColumnModifier> iter = modifiers.iterator(); iter.hasNext();) {
			ColumnModifier cm = iter.next();
			int was = f;
			if (cm.getTag() == ColumnModifierKind.AUTOINCREMENT) {
				f |= AUTO_INCREMENT;
			} else if (cm.getTag() == ColumnModifierKind.NOT_NULLABLE) {
				f |= NOT_NULLABLE;
				explicitNullability = true;
			} else if (cm.getTag() == ColumnModifierKind.NULLABLE) {
				iter.remove();
				explicitNullability = true;
			} else if (cm.getTag() == ColumnModifierKind.ONUPDATE) {
				f |= ONUPDATE;
			} else if (cm.getTag() == ColumnModifierKind.DEFAULTVALUE) {
				DefaultValueModifier dvm = (DefaultValueModifier) cm;
				defaultValue = dvm.getDefaultValue();
				iter.remove();
			}
			if (was != f)
				iter.remove();
		}
		if (!modifiers.isEmpty())
			throw new SchemaException(Pass.SECOND, "Unhandled column modifier: " + modifiers.get(0).getTag().getSQL());
		if (type.isTimestampType() && !explicitNullability) {
			f |= ONUPDATE;
			f |= NOT_NULLABLE;
		}
		if (type.isSerialPlaceholder()) {
			// serial = auto increment notn ull
			f |= NOT_NULLABLE;
			f |= AUTO_INCREMENT;
			type = ((SerialPlaceholderType)type).convert();
			if (inlineKeys != null)
				inlineKeys.add(new ColumnKeyModifier(ConstraintType.UNIQUE));
		}
		PEColumn e = new PEColumn(sc,name,type,f,defaultValue,comment);
		e.normalize();
		return e;
	}
	
	protected PEColumn(SchemaContext pc, Name name, Type type, short givenFlags, ExpressionNode defVal, Comment theComment) {
		super(null);
		setName(name);
		setPersistent(pc,null,null);
		this.type = type;
		this.flags = givenFlags;
		this.comment = theComment;
		this.defaultValue = defVal;
			
	}
	
	// for placeholder columns in temp tables
	public PEColumn(SchemaContext pc,Name name, Type type) {
		this(pc, name, type, (short)0, null, null);
	}
	
	public static PEColumn load(UserColumn persistent, SchemaContext pc, PETable enclosingTable) {
		PEColumn e = null;
		if (enclosingTable != null) {
			e = enclosingTable.lookup(pc, new UnqualifiedName(persistent.getName()));
		}
		if (e == null) {
			if (TenantColumn.TENANT_COLUMN.equals(persistent.getName()))
				e = new TenantColumn(persistent, pc);
			else {
				e = new PEColumn(persistent, pc,true);
			}
		}
		return e;
	}
	
	protected void setFlag(int f) {
		flags |= f;
	}
	
	protected void clearFlag(int f) {
		flags &= ~f;
	}
	
	protected boolean isSet(int f) {
		return (flags & f) != 0;
	}
	
	protected PEColumn(UserColumn persistent, SchemaContext pc, boolean loading) {
		super(null);
		if (loading)
			pc.startLoading(this, persistent);
		setName(new UnqualifiedName(persistent.getName(),true));
		this.type = BasicType.buildType(persistent, pc.getTypes());
		if (!persistent.isNullable())
			setFlag(NOT_NULLABLE);
		if (persistent.isAutoGenerated())
			setFlag(AUTO_INCREMENT);
		if (persistent.hasDefault()) {
			String defVal = persistent.getDefaultValue();
			ExpressionNode defLiteral = null;
			if (defVal == null)
				defLiteral = LiteralExpression.makeNullLiteral();
			else {
				if (!persistent.defaultValueIsConstant()) {
					// special case of current_timestamp or 0 is the only allowed non constant default value
					if (StringUtils.equals("0", defVal)) {
						defLiteral = new IdentifierLiteralExpression(new UnqualifiedName("0"));
					} else {
						defLiteral = new IdentifierLiteralExpression(new UnqualifiedName(CURRENT_TIMESTAMP));
					}
				} else {
					defLiteral = new ActualLiteralExpression(defVal,null);
				}
			}
			defaultValue = defLiteral;
		}
		if (persistent.hasOnUpdate())
			setFlag(ONUPDATE);
		this.dvposition = persistent.getHashPosition();
		this.cdv_position = persistent.getCDV_Position();
		
		if (loading) {
			setPersistent(pc,persistent,persistent.getId());
			pc.finishedLoading(this, persistent);
		}
	}
	
	// used in view support - build a transient column from a persistent one
	public static PEColumn build(SchemaContext sc, UserColumn uc) {
		return new PEColumn(uc,sc,false);
	}
	
	@Override
	public Type getType() { return type; }
	
	@Override
	public PEAbstractTable<?> getTable() { return ofTable; }
	@Override
	public void setTable(PEAbstractTable<?> t) {
		ofTable = t; 			
	}
	
	@Override
	public void setComment(Comment s) { comment = s; }
	@Override
	public Comment getComment() { return comment; }
	
	// needed for insert support
	public boolean isNullable() {
		return !isSet(NOT_NULLABLE);
	}

	// needed for primary key support - columns in primary keys are not nullable
	public void makeNotNullable() {
		setFlag(NOT_NULLABLE);
		// if the default value is null - clear that
		if (defaultValue instanceof LiteralExpression) {
			LiteralExpression litex = (LiteralExpression) defaultValue;
			if (litex.isNullLiteral())
				defaultValue = null;
		}
	}

	public boolean isNotNullable() {
		return isSet(NOT_NULLABLE);
	}
	
	public ExpressionNode getDefaultValue() {
		return defaultValue;
	}
	
	public boolean isAutoIncrement() {
		return isSet(AUTO_INCREMENT);
	}
	
	public boolean isOnUpdated() {
		return isSet(ONUPDATE);
	}
	
	public void makeAutoincrement() {
		setFlag(AUTO_INCREMENT);
	}
	
	public void clearAutoIncrement() {
		clearFlag(AUTO_INCREMENT);
	}
	
	// alter support
	public void setDefaultValue(LiteralExpression litex) {
		defaultValue = litex;
	}
	
	public int getDistributionValuePosition() { return dvposition; }
	public void setDistributionValuePosition(int v) { dvposition = v; }
	
	public boolean isPartOfDistributionVector() { return dvposition > 0; }
	
	public boolean isCompleteDistributionVector(SchemaContext sc) {
		return ofTable.getDistributionVector(sc).isComplete(sc,this);
	}
	
	public int getContainerDistributionValuePosition() { return cdv_position; }
	public void setContainerDistributionValuePosition(int v) { cdv_position = v; }
	public boolean isPartOfContainerDistributionVector() { return cdv_position > 0; }
	
	public boolean comparableForDistribution(SchemaContext sc, PEColumn other) {
		UserColumn backing = getPersistent(sc, false);
		UserColumn otherBacking = other.getPersistent(sc, false);
		if (backing != null && otherBacking != null) 
			return backing.comparableType(otherBacking);
		return getType().comparableForDistribution(other.getType());
	}
		
	@Override
	public boolean isTenantColumn() {
		return false;
	}
	
	public boolean isTempColumn() {
		return false;
	}
	
	public ExpressionNode buildProjection(TableInstance ti) {
		return new ColumnInstance(getName(), this, ti);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		PEColumn other = (PEColumn) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

	private boolean typeDiffers(SchemaContext sc, List<String> messages, PEColumn other, boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		if (maybeBuildDiffMessage(sc, messages, "type", getType().getName(), other.getType().getName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "has size", getType().declUsesSizing(), other.getType().declUsesSizing(), first, visited))
			return true;

		if (maybeBuildDiffMessage(sc, messages, "type size", getType().getSize(), other.getType().getSize(), first, visited))
			return true;
		
		return false;
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEColumn, UserColumn> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEColumn other = oth.get();
		
		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);

		if (maybeBuildDiffMessage(sc, messages, "name", getName(), other.getName(), first, visited))
			return true;
		
		if (typeDiffers(sc, messages, other, first, visited))
			return true;
		
		// mask out the derived flags
		int mask = ~(KEY_PART | PRIMARY_KEY_PART | UNIQUE_KEY_PART);
		
		int myFlags = flags & mask;
		int yourFlags = other.flags & mask;
		if (myFlags != yourFlags) {
			if (maybeBuildDiffMessage(sc, messages, "column modifiers", myFlags, yourFlags, first, visited)) {
				return true;
			}
		}

		if (maybeBuildDiffMessage(sc, messages, "position", this.getPosition(), other.getPosition(), first, visited)) {
			return true;
		}

		if (maybeBuildDiffMessage(sc, messages, "distribution vector position", 
				new Integer(getDistributionValuePosition()), new Integer(other.getDistributionValuePosition()), first, visited)) {
			return true;
		}
		
		if (maybeBuildDiffMessage(sc, messages, "default value", this.getDefaultValue(), other.getDefaultValue(), first, visited)) {
			return true;
		}
		
		return false;
	}

	@Override
	protected String getDiffTag() {
		return "Column " + getName().getSQL();
	}
	
	public static Map<Name, PEColumn> buildNameMap(Collection<PEColumn> in) {
		return Functional.buildMap(in, new Accessor<Name, PEColumn>() {

			@Override
			public Name evaluate(PEColumn object) {
				return object.getName();
			}
			
		});
	}

	@Override
	public Persistable<PEColumn, UserColumn> reload(
			SchemaContext usingContext) {
		throw new IllegalStateException("Cannot reload a lone column");
	}
	
	@Override
	public String toString() {
		return ofTable.toString() + "." + getName().get();
	}

	@Override
	protected int getID(UserColumn p) {
		return p.getId();
	}

	@Override
	protected UserColumn lookup(SchemaContext sc) throws PEException {
		UserTable ut = ofTable.persistTree(sc);
        String persName = Singletons.require(HostService.class).getDBNative().getEmitter().getPersistentName(this);
		return ut.getUserColumn(persName);
	}

	@Override
	protected Persistable<PEColumn, UserColumn> load(SchemaContext sc, UserColumn p)
			throws PEException {
		return new PEColumn(p,sc,true);
	}

	@Override
	protected UserColumn createEmptyNew(SchemaContext pc) throws PEException {
        String persName = Singletons.require(HostService.class).getDBNative().getEmitter().getPersistentName(this);
        UserColumn uc = Singletons.require(HostService.class).getDBNative().updateUserColumn(null, type);
		uc.setName(persName);
		pc.getSaveContext().add(this,uc);
		return uc;
	}

	/**
	 * @param sc
	 * @param uc
	 * @throws PEException
	 */
	private void setPersistentDefaultValue(SchemaContext sc, UserColumn uc) throws PEException {
		if (defaultValue == null) {
			MysqlType mt = MysqlType.toMysqlType(uc.getNativeTypeName());
			if (mt == MysqlType.TIMESTAMP) {
				uc.setHasDefault(Boolean.TRUE);
				if (uc.isNullable()) {
					uc.setDefaultValue(null);
					uc.setDefaultValueIsConstant(true);
				} else {
					if (isSet(ONUPDATE)) {
						uc.setDefaultValue("0");						
					} else {
						uc.setDefaultValue(CURRENT_TIMESTAMP);
					}
					uc.setDefaultValueIsConstant(false);
				}
			} else {
				uc.setHasDefault(Boolean.FALSE);
				uc.setDefaultValue(null);
				uc.setDefaultValueIsConstant(true);
			}
		} else {
			uc.setHasDefault(Boolean.TRUE);
			LiteralExpression le = (LiteralExpression)defaultValue;
			if (le.isNullLiteral()) {
				uc.setDefaultValue(null);
				uc.setDefaultValueIsConstant(true);
			} else {
				uc.setDefaultValueIsConstant(true);
				if (le instanceof IdentifierLiteralExpression) {
					uc.setDefaultValueIsConstant(false);
				}
				if ((uc.getDataType() == Types.TIMESTAMP) && 
						StringUtils.equals("0", le.getValue(sc).toString())) {
					uc.setDefaultValueIsConstant(false);
				}
				uc.setDefaultValue(le.getValue(sc).toString());
			}
		}		
	}
	
	public void takeCharsetSettings(CharsetTableModifier charset, CollationTableModifier collation, boolean update) {
		if (!isTempColumn() && hasStringType()) {
			final TextType typeAsText = type.toTextType();
			if (update || (typeAsText.getCharset() == null)) {
				typeAsText.setCharset(charset.getCharset());
			}
			if (update || (typeAsText.getCollation() == null)) {
				typeAsText.setCollation(collation.getCollation());
			}

			type = typeAsText;
		}
	}

	public boolean hasStringType() {
		return type.isStringType();
	}

	public UnqualifiedName getCharset() {
		if (type instanceof TextType) {
			TextType tt = (TextType) type;
			if (tt.getCharset() == null) return null;
			if (getTable() == null) return tt.getCharset();
			if (getTable().getCharset() == null) return tt.getCharset();
			if (tt.getCharset().equals(getTable().getCharset().getCharset())) return null;
			return tt.getCharset();
		}
		return null;
	}
	
	public UnqualifiedName getCollation() {
		if (type instanceof TextType) {
			TextType tt = (TextType)type;
			if (tt.getCollation() == null) return null;
			if (getTable() == null) return tt.getCollation();
			if (getTable().getCollation() == null) return tt.getCollation();
			if (tt.getCollation().equals(getTable().getCollation().getCollation())) return null;
			return tt.getCollation();
		}
		return null;
	}
	
	public void makeBinaryText() {
		if (type instanceof TextType) {
			TextType tt = (TextType) type;
			tt.makeBinaryText();
		}
	}
	
	protected void addColumnTypeModifiers(List<String> entries) {
		if (isOnUpdated()) {
			entries.add(ON_UPDATE_TAG);
		}
	}
	
	private void addTypeModifiers(UserColumn uc) {
		ArrayList<String> entries = new ArrayList<String>();
		addColumnTypeModifiers(entries);
		StringBuffer buf = new StringBuffer();
		String ntm = uc.getNativeTypeModifiers();
		if (ntm != null) { 
			buf.append(ntm);
			if (!entries.isEmpty())
				buf.append(" ");
		}
		buf.append(Functional.join(entries, " "));		
		if (buf.length() > 0)
			uc.setNativeTypeModifiers(buf.toString().trim());
	}
	
	@Override
	protected void populateNew(SchemaContext pc, UserColumn uc) throws PEException {
		UserTable ut = ofTable.persistTree(pc);
		if (isAutoIncrement()) {
			uc.setAutoGenerated(Boolean.TRUE);
			Long offset = getTable().asTable().getAutoIncOffset(pc);
			pc.getCatalog().addAutoIncrement(pc,getTable().asTable(), offset);
		}
		uc.setUserTable(ut);
		updateExisting(pc,uc);
	}

	@Override
	protected void updateExisting(SchemaContext pc, UserColumn uc) throws PEException {
		setPersistentDefaultValue(pc,uc);
		updateExistingInternal(pc,uc);
	}

	private void updateExistingInternal(SchemaContext pc, UserColumn uc) {
        String persName = Singletons.require(HostService.class).getDBNative().getEmitter().getPersistentName(this);
		uc.setName(persName);
		uc.setNullable(Boolean.valueOf(!isSet(NOT_NULLABLE)));
		uc.setOnUpdate(Boolean.valueOf(isSet(ONUPDATE)));
		uc.setHashPosition(dvposition);
		uc.setCDV_Position(cdv_position);
		addTypeModifiers(uc);
        Singletons.require(HostService.class).getDBNative().updateUserColumn(uc, type);
		
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return UserColumn.class;
	}

	@Override
	public PEColumn getIn(SchemaContext pc, PEAbstractTable<?> tab) {
		PEColumn e = tab.lookup(pc, getName());
		return e;
	}

	@Override
	public void take(SchemaContext pc, PEColumn targ) {
		if (!getName().getUnqualified().getUnquotedName().equals(targ.getName().getUnqualified().getUnquotedName())) {
			setName(targ.getName());
			if (ofTable != null) {
				ofTable.refreshColumnLookupTable();
			}
		}
		type = targ.getType();
		flags = targ.flags;
		defaultValue = targ.getDefaultValue();
	}
	
	public void setType(final Type type) {
		this.type = type;
	}

	@Override
	public Traversable copy(SchemaContext pc, CopyContext cc) {
		return new PEColumn(pc, getName(), getType(), flags, defaultValue, getComment());
	}	
	
	public boolean isPrimaryKeyPart() {
		return isSet(PRIMARY_KEY_PART);
	}
	
	public void setPrimaryKeyPart() {
		setFlag(PRIMARY_KEY_PART);
		setFlag(KEY_PART);
	}
	
	public boolean isUniquePart() {
		return isSet(UNIQUE_KEY_PART);
	}

	public void setUniqueKeyPart() {
		setFlag(UNIQUE_KEY_PART);
		setFlag(KEY_PART);
	}
	
	public boolean isKeyPart() {
		return isSet(KEY_PART);
	}

	public void setKeyPart() {
		setFlag(KEY_PART);
	}
	
	public ListSet<PEKey> getReferencedBy(SchemaContext sc) {
		ListSet<PEKey> out = new ListSet<PEKey>();
		if (!isKeyPart()) return out;
		for(PEKey pek : getTable().getKeys(sc)) {
			if (pek.getConstraint() == ConstraintType.FOREIGN) continue;
			for(PEKeyColumnBase pekc : pek.getKeyColumns()) {
				if (pekc.getColumn().equals(this)) {
					out.add(pek);
					break;
				}
			}
		}
		return out;
	}
	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int v) {
		position = v;
	}
	
	public void normalize() {
		this.normalize(false);
	}

	public void normalize(final boolean useImplicitNull) {
		type = type.normalize();
		if (defaultValue == null) {
			if (type.isTimestampType() && isNotNullable()) {
				if (isOnUpdated()) {
					defaultValue = new IdentifierLiteralExpression(new UnqualifiedName(CURRENT_TIMESTAMP));
				} else
					defaultValue = LiteralExpression.makeStringLiteral("0000-00-00 00:00:00");
			} else if (!type.supportsDefaultValue()) {

			} else if (!isNotNullable() && !useImplicitNull) {
				defaultValue = LiteralExpression.makeNullLiteral();
			}
		}
	}
	
	public Integer getIndexSize() {
		return type.getIndexSize();
	}

	@Override
	public String getPersistentName() {
		return getName().getUnquotedName().get();
	}

	@Override
	public String getAliasName() {
		return getPersistentName();
	}

	@Override
	public int getId() {
		return getPersistentID();
	}

	@Override
	public String getNativeTypeName() {
		return getType().getTypeName();
	}

	@Override
	public int getHashPosition() {
		return dvposition;
	}

	// for temporary tables
	
	public ResultRow buildRow(SchemaContext sc) {
		ResultRow rr = new ResultRow();
		sc.beginSaveContext();
		try {
			rr.addResultColumn(getName().getUnquotedName().get());
			UserColumn uc = new UserColumn();
			updateExistingInternal(sc,uc);
			rr.addResultColumn(Singletons.require(HostService.class).getDBNative().getDataTypeForQuery(uc));
			rr.addResultColumn(isNullable() ? "YES" : "NO");
			
			String kp = "";
			if (isPrimaryKeyPart())
				kp = "PRI";
			else if (isUniquePart())
				kp = "UNI";
			else if (isKeyPart())
				kp = "MUL";
			rr.addResultColumn(kp);
			
			if (defaultValue == null || ((LiteralExpression)defaultValue).isNullLiteral())
				rr.addResultColumn(null,true);
			else
				rr.addResultColumn(defaultValue.toString(sc));
			rr.addResultColumn(isAutoIncrement() ? "auto_increment" : "");
			return rr;
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to build temporary table show columns result set",pe);
		} finally {
			sc.endSaveContext();
		}
		
	}
}
