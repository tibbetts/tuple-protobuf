package com.streambase.services.protobuf;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.streambase.sb.CompleteDataType;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;

public class ProtobufTuple {

    public static Message toProtobuf(Builder b, Tuple t) {
        Schema s = t.getSchema();
        Descriptor desc = b.getDescriptorForType();
        for (Schema.Field f : s.getFields()) {
            if (!t.isNull(f)) {
                FieldDescriptor fd = desc.findFieldByName(f.getName());
                if (fd != null) {
                    Object value = protobufValueForField(f.getCompleteDataType(), t.getField(f), b, fd);
                    b.setField(fd, value);
                }
            }
        }
        return b.build();
    }

    static final EnumSet<DataType> DIRECT_SETTABLE_TYPES = EnumSet.of(DataType.INT, DataType.LONG, DataType.BOOL, DataType.DOUBLE, DataType.STRING);
    // TODO: BLOB, Timestamp.
    
    private static Object protobufValueForField(CompleteDataType c, Object value, Builder b, FieldDescriptor fd) {
        if (DIRECT_SETTABLE_TYPES.contains(c.getDataType())) {
            return value;
        }
        switch (c.getDataType()) {
            case TUPLE:
                return submessageValueForField(value, b, fd);
            case LIST:
                CompleteDataType elementType = c.getElementType();
                if (DIRECT_SETTABLE_TYPES.contains(elementType.getDataType())) {
                    return value;
                } else {
                    return listValueForField(elementType, value, b, fd);
                }
            default:
                throw new RuntimeException("Unhandled data type " + c);
        }
    }

    private static Message submessageValueForField(Object v, Builder b, FieldDescriptor fd) {
        Tuple t = (Tuple)v;
        Builder subBuilder = b.newBuilderForField(fd);
        return toProtobuf(subBuilder, t);
    }

    private static List<Object> listValueForField(CompleteDataType elementType, Object value, Builder b,
            FieldDescriptor fd) {
        List<?> l = (List<?>)value;
        List<Object> ret = new ArrayList<Object>(l.size());
        for (Object v : l) {
            ret.add(protobufValueForField(elementType, v, b, fd));
        }
        return ret;
    }
    
    public static Tuple fromProtobuf(Schema s, Message m) {
        Tuple ret = s.createTuple();
        Descriptor desc = m.getDescriptorForType();
        try {
            for (Schema.Field f : s.getFields()) {
                FieldDescriptor fd = desc.findFieldByName(f.getName());
                if (fd != null) {
                    if (fd.isRepeated()) {
                        int count = m.getRepeatedFieldCount(fd);
                        if (count > 0) {
                            ArrayList<Object> list = new ArrayList<Object>(count);
                            for (int i = 0; i < count; ++i) {
                                list.add(m.getRepeatedField(fd, i));
                            }
                            ret.setList(f, list);
                        }
                    } else {
                        if (m.hasField(fd)) {
                            Object value = m.getField(fd);
                            if (f.getDataType() == DataType.TUPLE) {
                                Tuple tupleValue = fromProtobuf(f.getSchema(), (Message) value);
                                ret.setTuple(f, tupleValue);
                            } else {
                                ret.setField(f, value);
                            }
                        }
                    }
                }
            }
        } catch (TupleException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

}
