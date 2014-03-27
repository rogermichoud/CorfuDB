/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.microsoft.corfu;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtntInfo implements org.apache.thrift.TBase<ExtntInfo, ExtntInfo._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExtntInfo");

  private static final org.apache.thrift.protocol.TField META_FIRST_OFF_FIELD_DESC = new org.apache.thrift.protocol.TField("metaFirstOff", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField META_LENGTH_FIELD_DESC = new org.apache.thrift.protocol.TField("metaLength", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField FLAG_FIELD_DESC = new org.apache.thrift.protocol.TField("flag", org.apache.thrift.protocol.TType.I32, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ExtntInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ExtntInfoTupleSchemeFactory());
  }

  public long metaFirstOff; // required
  public int metaLength; // required
  /**
   * 
   * @see ExtntMarkType
   */
  public ExtntMarkType flag; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    META_FIRST_OFF((short)1, "metaFirstOff"),
    META_LENGTH((short)2, "metaLength"),
    /**
     * 
     * @see ExtntMarkType
     */
    FLAG((short)3, "flag");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // META_FIRST_OFF
          return META_FIRST_OFF;
        case 2: // META_LENGTH
          return META_LENGTH;
        case 3: // FLAG
          return FLAG;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __METAFIRSTOFF_ISSET_ID = 0;
  private static final int __METALENGTH_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.META_FIRST_OFF, new org.apache.thrift.meta_data.FieldMetaData("metaFirstOff", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.META_LENGTH, new org.apache.thrift.meta_data.FieldMetaData("metaLength", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.FLAG, new org.apache.thrift.meta_data.FieldMetaData("flag", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ExtntMarkType.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExtntInfo.class, metaDataMap);
  }

  public ExtntInfo() {
    this.flag = com.microsoft.corfu.ExtntMarkType.EX_FILLED;

  }

  public ExtntInfo(
    long metaFirstOff,
    int metaLength,
    ExtntMarkType flag)
  {
    this();
    this.metaFirstOff = metaFirstOff;
    setMetaFirstOffIsSet(true);
    this.metaLength = metaLength;
    setMetaLengthIsSet(true);
    this.flag = flag;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExtntInfo(ExtntInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.metaFirstOff = other.metaFirstOff;
    this.metaLength = other.metaLength;
    if (other.isSetFlag()) {
      this.flag = other.flag;
    }
  }

  public ExtntInfo deepCopy() {
    return new ExtntInfo(this);
  }

  @Override
  public void clear() {
    setMetaFirstOffIsSet(false);
    this.metaFirstOff = 0;
    setMetaLengthIsSet(false);
    this.metaLength = 0;
    this.flag = com.microsoft.corfu.ExtntMarkType.EX_FILLED;

  }

  public long getMetaFirstOff() {
    return this.metaFirstOff;
  }

  public ExtntInfo setMetaFirstOff(long metaFirstOff) {
    this.metaFirstOff = metaFirstOff;
    setMetaFirstOffIsSet(true);
    return this;
  }

  public void unsetMetaFirstOff() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __METAFIRSTOFF_ISSET_ID);
  }

  /** Returns true if field metaFirstOff is set (has been assigned a value) and false otherwise */
  public boolean isSetMetaFirstOff() {
    return EncodingUtils.testBit(__isset_bitfield, __METAFIRSTOFF_ISSET_ID);
  }

  public void setMetaFirstOffIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __METAFIRSTOFF_ISSET_ID, value);
  }

  public int getMetaLength() {
    return this.metaLength;
  }

  public ExtntInfo setMetaLength(int metaLength) {
    this.metaLength = metaLength;
    setMetaLengthIsSet(true);
    return this;
  }

  public void unsetMetaLength() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __METALENGTH_ISSET_ID);
  }

  /** Returns true if field metaLength is set (has been assigned a value) and false otherwise */
  public boolean isSetMetaLength() {
    return EncodingUtils.testBit(__isset_bitfield, __METALENGTH_ISSET_ID);
  }

  public void setMetaLengthIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __METALENGTH_ISSET_ID, value);
  }

  /**
   * 
   * @see ExtntMarkType
   */
  public ExtntMarkType getFlag() {
    return this.flag;
  }

  /**
   * 
   * @see ExtntMarkType
   */
  public ExtntInfo setFlag(ExtntMarkType flag) {
    this.flag = flag;
    return this;
  }

  public void unsetFlag() {
    this.flag = null;
  }

  /** Returns true if field flag is set (has been assigned a value) and false otherwise */
  public boolean isSetFlag() {
    return this.flag != null;
  }

  public void setFlagIsSet(boolean value) {
    if (!value) {
      this.flag = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case META_FIRST_OFF:
      if (value == null) {
        unsetMetaFirstOff();
      } else {
        setMetaFirstOff((Long)value);
      }
      break;

    case META_LENGTH:
      if (value == null) {
        unsetMetaLength();
      } else {
        setMetaLength((Integer)value);
      }
      break;

    case FLAG:
      if (value == null) {
        unsetFlag();
      } else {
        setFlag((ExtntMarkType)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case META_FIRST_OFF:
      return Long.valueOf(getMetaFirstOff());

    case META_LENGTH:
      return Integer.valueOf(getMetaLength());

    case FLAG:
      return getFlag();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case META_FIRST_OFF:
      return isSetMetaFirstOff();
    case META_LENGTH:
      return isSetMetaLength();
    case FLAG:
      return isSetFlag();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ExtntInfo)
      return this.equals((ExtntInfo)that);
    return false;
  }

  public boolean equals(ExtntInfo that) {
    if (that == null)
      return false;

    boolean this_present_metaFirstOff = true;
    boolean that_present_metaFirstOff = true;
    if (this_present_metaFirstOff || that_present_metaFirstOff) {
      if (!(this_present_metaFirstOff && that_present_metaFirstOff))
        return false;
      if (this.metaFirstOff != that.metaFirstOff)
        return false;
    }

    boolean this_present_metaLength = true;
    boolean that_present_metaLength = true;
    if (this_present_metaLength || that_present_metaLength) {
      if (!(this_present_metaLength && that_present_metaLength))
        return false;
      if (this.metaLength != that.metaLength)
        return false;
    }

    boolean this_present_flag = true && this.isSetFlag();
    boolean that_present_flag = true && that.isSetFlag();
    if (this_present_flag || that_present_flag) {
      if (!(this_present_flag && that_present_flag))
        return false;
      if (!this.flag.equals(that.flag))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(ExtntInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ExtntInfo typedOther = (ExtntInfo)other;

    lastComparison = Boolean.valueOf(isSetMetaFirstOff()).compareTo(typedOther.isSetMetaFirstOff());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMetaFirstOff()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metaFirstOff, typedOther.metaFirstOff);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMetaLength()).compareTo(typedOther.isSetMetaLength());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMetaLength()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metaLength, typedOther.metaLength);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFlag()).compareTo(typedOther.isSetFlag());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFlag()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.flag, typedOther.flag);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ExtntInfo(");
    boolean first = true;

    sb.append("metaFirstOff:");
    sb.append(this.metaFirstOff);
    first = false;
    if (!first) sb.append(", ");
    sb.append("metaLength:");
    sb.append(this.metaLength);
    first = false;
    if (!first) sb.append(", ");
    sb.append("flag:");
    if (this.flag == null) {
      sb.append("null");
    } else {
      sb.append(this.flag);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExtntInfoStandardSchemeFactory implements SchemeFactory {
    public ExtntInfoStandardScheme getScheme() {
      return new ExtntInfoStandardScheme();
    }
  }

  private static class ExtntInfoStandardScheme extends StandardScheme<ExtntInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExtntInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // META_FIRST_OFF
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.metaFirstOff = iprot.readI64();
              struct.setMetaFirstOffIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // META_LENGTH
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.metaLength = iprot.readI32();
              struct.setMetaLengthIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // FLAG
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.flag = ExtntMarkType.findByValue(iprot.readI32());
              struct.setFlagIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExtntInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(META_FIRST_OFF_FIELD_DESC);
      oprot.writeI64(struct.metaFirstOff);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(META_LENGTH_FIELD_DESC);
      oprot.writeI32(struct.metaLength);
      oprot.writeFieldEnd();
      if (struct.flag != null) {
        oprot.writeFieldBegin(FLAG_FIELD_DESC);
        oprot.writeI32(struct.flag.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExtntInfoTupleSchemeFactory implements SchemeFactory {
    public ExtntInfoTupleScheme getScheme() {
      return new ExtntInfoTupleScheme();
    }
  }

  private static class ExtntInfoTupleScheme extends TupleScheme<ExtntInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExtntInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetMetaFirstOff()) {
        optionals.set(0);
      }
      if (struct.isSetMetaLength()) {
        optionals.set(1);
      }
      if (struct.isSetFlag()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetMetaFirstOff()) {
        oprot.writeI64(struct.metaFirstOff);
      }
      if (struct.isSetMetaLength()) {
        oprot.writeI32(struct.metaLength);
      }
      if (struct.isSetFlag()) {
        oprot.writeI32(struct.flag.getValue());
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExtntInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.metaFirstOff = iprot.readI64();
        struct.setMetaFirstOffIsSet(true);
      }
      if (incoming.get(1)) {
        struct.metaLength = iprot.readI32();
        struct.setMetaLengthIsSet(true);
      }
      if (incoming.get(2)) {
        struct.flag = ExtntMarkType.findByValue(iprot.readI32());
        struct.setFlagIsSet(true);
      }
    }
  }

}

