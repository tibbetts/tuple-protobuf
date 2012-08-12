package com.streambase.services.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.streambase.sb.CompleteDataType;
import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.test.SchemaMaker;
import com.streambase.services.protobuf.test.TestMessages;

public class ProtobufTupleTest {

    @Test
    public void testConversionToProtobuf() throws Exception {
        Schema s = new SchemaMaker().intField("i").boolField("b").makeSchema();
        Tuple t = s.createTuple();

        Message m = ProtobufTuple.toProtobuf(TestMessages.Foo.newBuilder(), t);
        assertEqualsTM(t, m);
        Tuple t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        t.setInt("i", 37);
        m = ProtobufTuple.toProtobuf(TestMessages.Foo.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        t.setBoolean("b", true);
        m = ProtobufTuple.toProtobuf(TestMessages.Foo.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);
    }

    @Test
    public void testComplexTypes() throws Exception {
        Schema fooSchema = new SchemaMaker().intField("i").boolField("b")
                .makeSchema();
        Schema s = new SchemaMaker().tupleField("f1", fooSchema)
                .listField("list", CompleteDataType.forInt())
                .listField("foolist", CompleteDataType.forTuple(fooSchema))
                .makeSchema();
        Tuple t = s.createTuple();

        Message m = ProtobufTuple.toProtobuf(TestMessages.Bar.newBuilder(), t);
        assertEqualsTM(t, m);
        Tuple t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        t.setList("list", Arrays.asList(1, 2, 3));
        m = ProtobufTuple.toProtobuf(TestMessages.Bar.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        t.setTuple("f1", fooSchema.createTuple());
        m = ProtobufTuple.toProtobuf(TestMessages.Bar.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        Tuple fooT = fooSchema.createTuple();
        fooT.setInt("i", 42);
        t.setTuple("f1", fooT);
        m = ProtobufTuple.toProtobuf(TestMessages.Bar.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

        t.setList("foolist", Arrays.asList(fooT));
        m = ProtobufTuple.toProtobuf(TestMessages.Bar.newBuilder(), t);
        assertEqualsTM(t, m);
        t2 = ProtobufTuple.fromProtobuf(s, m);
        assertEquals(t, t2);
        assertEqualsTM(t2, m);

    }

    private static void assertEqualsTM(Tuple expected, Message actual)
            throws Exception {
        Schema s = expected.getSchema();
        Descriptor d = actual.getDescriptorForType();
        for (Schema.Field f : s.getFields()) {
            FieldDescriptor fd = d.findFieldByName(f.getName());
            if (expected.isNull(f)) {
                if (fd.isRepeated()) {
                    assertEquals(0, actual.getRepeatedFieldCount(fd));
                } else {
                    assertFalse(actual.hasField(fd));
                }
            } else {
                assertTMValuesEqual(f.getCompleteDataType(),
                        expected.getField(f), fd, actual.getField(fd));
            }
        }
    }

    private static void assertTMValuesEqual(CompleteDataType cdt,
            Object sbValue, FieldDescriptor fd, Object pbValue)
            throws Exception {
        switch (cdt.getDataType()) {
        case TUPLE:
            assertEqualsTM((Tuple) sbValue, (Message) pbValue);
            break;
        case LIST:
            List<?> sbList = (List<?>) sbValue;
            List<?> pbList = (List<?>) pbValue;
            assertEquals(sbList.size(), pbList.size());
            for (int i = 0; i < sbList.size(); ++i) {
                assertTMValuesEqual(cdt.getElementType(), sbList.get(i), fd,
                        pbList.get(i));
            }
            break;
        default:
            assertEquals(sbValue, pbValue);
        }
    }

}
