/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.compact.record;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class AllTypesRecord {
    public final byte primitiveInt8;
    public final Byte objectInt8;
    public final char primitiveChar;
    public final Character objectCharacter;
    public final short primitiveInt16;
    public final Short objectInt16;
    public final int primitiveInt32;
    public final Integer objectInt32;
    public final long primitiveInt64;
    public final Long objectInt64;
    public final float primitiveFloat32;
    public final Float objectFloat32;
    public final double primitiveFloat64;
    public final Double objectFloat64;
    public final boolean primitiveBoolean;
    public final Boolean objectBoolean;
    public final String string;
    public final BigDecimal decimal;
    public final LocalTime time;
    public final LocalDate date;
    public final LocalDateTime timestamp;
    public final OffsetDateTime timestampWithTimezone;
    public final AnEnum anEnum;
    public final NestedRecord nestedRecord;
    public final byte[] primitiveInt8Array;
    public final Byte[] objectInt8Array;
    public final char[] primitiveCharArray;
    public final Character[] objectCharacterArray;
    public final short[] primitiveInt16Array;
    public final Short[] objectInt16Array;
    public final int[] primitiveInt32Array;
    public final Integer[] objectInt32Array;
    public final long[] primitiveInt64Array;
    public final Long[] objectInt64Array;
    public final float[] primitiveFloat32Array;
    public final Float[] objectFloat32Array;
    public final double[] primitiveFloat64Array;
    public final Double[] objectFloat64Array;
    public final boolean[] primitiveBooleanArray;
    public final Boolean[] objectBooleanArray;
    public final String[] stringArray;
    public final BigDecimal[] decimalArray;
    public final LocalTime[] timeArray;
    public final LocalDate[] dateArray;
    public final LocalDateTime[] timestampArray;
    public final OffsetDateTime[] timestampWithTimezoneArray;
    public final AnEnum[] anEnumArray;
    public final NestedRecord[] nestedRecordArray;
    public final List<BigDecimal> list;
    public final ArrayList<String> arrayList;
    public final Set<Byte> set;
    public final HashSet<Integer> hashSet;
    public final Map<Short, Character> map;
    public final HashMap<Long, Boolean> hashMap;

    public AllTypesRecord(
        byte primitiveInt8,
        Byte objectInt8,
        char primitiveChar,
        Character objectCharacter,
        short primitiveInt16,
        Short objectInt16,
        int primitiveInt32,
        Integer objectInt32,
        long primitiveInt64,
        Long objectInt64,
        float primitiveFloat32,
        Float objectFloat32,
        double primitiveFloat64,
        Double objectFloat64,
        boolean primitiveBoolean,
        Boolean objectBoolean,
        String string,
        BigDecimal decimal,
        LocalTime time,
        LocalDate date,
        LocalDateTime timestamp,
        OffsetDateTime timestampWithTimezone,
        AnEnum anEnum,
        NestedRecord nestedRecord,
        byte[] primitiveInt8Array,
        Byte[] objectInt8Array,
        char[] primitiveCharArray,
        Character[] objectCharacterArray,
        short[] primitiveInt16Array,
        Short[] objectInt16Array,
        int[] primitiveInt32Array,
        Integer[] objectInt32Array,
        long[] primitiveInt64Array,
        Long[] objectInt64Array,
        float[] primitiveFloat32Array,
        Float[] objectFloat32Array,
        double[] primitiveFloat64Array,
        Double[] objectFloat64Array,
        boolean[] primitiveBooleanArray,
        Boolean[] objectBooleanArray,
        String[] stringArray,
        BigDecimal[] decimalArray,
        LocalTime[] timeArray,
        LocalDate[] dateArray,
        LocalDateTime[] timestampArray,
        OffsetDateTime[] timestampWithTimezoneArray,
        AnEnum[] anEnumArray,
        NestedRecord[] nestedRecordArray,
        List<BigDecimal> list,
        ArrayList<String> arrayList,
        Set<Byte> set,
        HashSet<Integer> hashSet,
        Map<Short, Character> map,
        HashMap<Long, Boolean> hashMap
    ) {
        this.primitiveInt8 = primitiveInt8;
        this.objectInt8 = objectInt8;
        this.primitiveChar = primitiveChar;
        this.objectCharacter = objectCharacter;
        this.primitiveInt16 = primitiveInt16;
        this.objectInt16 = objectInt16;
        this.primitiveInt32 = primitiveInt32;
        this.objectInt32 = objectInt32;
        this.primitiveInt64 = primitiveInt64;
        this.objectInt64 = objectInt64;
        this.primitiveFloat32 = primitiveFloat32;
        this.objectFloat32 = objectFloat32;
        this.primitiveFloat64 = primitiveFloat64;
        this.objectFloat64 = objectFloat64;
        this.primitiveBoolean = primitiveBoolean;
        this.objectBoolean = objectBoolean;
        this.string = string;
        this.decimal = decimal;
        this.time = time;
        this.date = date;
        this.timestamp = timestamp;
        this.timestampWithTimezone = timestampWithTimezone;
        this.anEnum = anEnum;
        this.nestedRecord = nestedRecord;
        this.primitiveInt8Array = primitiveInt8Array;
        this.objectInt8Array = objectInt8Array;
        this.primitiveCharArray = primitiveCharArray;
        this.objectCharacterArray = objectCharacterArray;
        this.primitiveInt16Array = primitiveInt16Array;
        this.objectInt16Array = objectInt16Array;
        this.primitiveInt32Array = primitiveInt32Array;
        this.objectInt32Array = objectInt32Array;
        this.primitiveInt64Array = primitiveInt64Array;
        this.objectInt64Array = objectInt64Array;
        this.primitiveFloat32Array = primitiveFloat32Array;
        this.objectFloat32Array = objectFloat32Array;
        this.primitiveFloat64Array = primitiveFloat64Array;
        this.objectFloat64Array = objectFloat64Array;
        this.primitiveBooleanArray = primitiveBooleanArray;
        this.objectBooleanArray = objectBooleanArray;
        this.stringArray = stringArray;
        this.decimalArray = decimalArray;
        this.timeArray = timeArray;
        this.dateArray = dateArray;
        this.timestampArray = timestampArray;
        this.timestampWithTimezoneArray = timestampWithTimezoneArray;
        this.anEnumArray = anEnumArray;
        this.nestedRecordArray = nestedRecordArray;
        this.list = list;
        this.arrayList = arrayList;
        this.set = set;
        this.hashSet = hashSet;
        this.map = map;
        this.hashMap = hashMap;
    }

    public enum AnEnum {
        VALUE0,
        VALUE1,
    }

    public static final class NestedRecord {
        public final int primitiveInt32;
        public final String string;

        private NestedRecord(int primitiveInt32, String string) {
            this.primitiveInt32 = primitiveInt32;
            this.string = string;
        }

        public static NestedRecord create() {
            return new NestedRecord(42, "42");
        }
    }

    public static AllTypesRecord create() {
        return new AllTypesRecord(
                (byte) 42,
                (byte) -23,
                '\uabcd',
                '\ua0b0',
                (short) 12345,
                (short) -12345,
                0,
                98237123,
                1321213321L,
                -891329819321123L,
                42.42F,
                -42.42F,
                9876.54321D,
                12345.6789D,
                true,
                Boolean.FALSE,
                "lorem ipsum",
                BigDecimal.valueOf(1234567.8901),
                LocalTime.now(),
                LocalDate.now(),
                LocalDateTime.now(),
                OffsetDateTime.now(),
                AnEnum.VALUE1,
                NestedRecord.create(),
                new byte[]{1, 2, -3},
                new Byte[]{42, Byte.MAX_VALUE, Byte.MIN_VALUE, null},
                new char[]{'0'},
                new Character[]{Character.MAX_VALUE, '0', null, Character.MIN_VALUE, '\uf9ac'},
                new short[]{0},
                new Short[]{Short.MIN_VALUE, 0, null, Short.MIN_VALUE, Short.MAX_VALUE},
                new int[]{123, 456, 789, Integer.MIN_VALUE},
                new Integer[]{null, null, 0, Integer.MAX_VALUE},
                new long[]{123456789, -1},
                new Long[]{Long.MAX_VALUE, null, null},
                new float[]{42.3F, 3.42F},
                new Float[]{53.6F, -6.53F, null},
                new double[]{0},
                new Double[]{},
                new boolean[]{true, false, true},
                new Boolean[]{null, true, null, false},
                new String[]{"lorem", null, "ipsum"},
                new BigDecimal[]{BigDecimal.ONE, BigDecimal.ZERO},
                new LocalTime[]{LocalTime.now(), null},
                new LocalDate[]{LocalDate.now(), null},
                new LocalDateTime[]{null, LocalDateTime.now()},
                new OffsetDateTime[]{null, OffsetDateTime.now()},
                new AnEnum[]{AnEnum.VALUE0, null, AnEnum.VALUE1},
                new NestedRecord[]{null, NestedRecord.create()},
                new ArrayList<BigDecimal>() {{
                    add(BigDecimal.ONE);
                    add(null);
                    add(BigDecimal.TEN);
                }},
                new ArrayList<String>() {{
                    add("a");
                    add("b");
                    add("c");
                }},
                new HashSet<Byte>() {{
                    add((byte) 0);
                }},
                new HashSet<Integer>() {{
                    add(42);
                    add(null);
                }},
                new HashMap<Short, Character>() {{
                    put((short) 42, 'x');
                }},
                new HashMap<Long, Boolean>() {{
                    put(0L, false);
                    put(1L, true);
                }}
        );
    }

    public static AllTypesRecord createWithDefaultValues() {
        return new AllTypesRecord(
                (byte) 0,
                null,
                '\u0000',
                null,
                (short) 0,
                null,
                0,
                null,
                0,
                null,
                0,
                null,
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllTypesRecord that = (AllTypesRecord) o;
        return primitiveInt8 == that.primitiveInt8
                && primitiveChar == that.primitiveChar
                && primitiveInt16 == that.primitiveInt16
                && primitiveInt32 == that.primitiveInt32
                && primitiveInt64 == that.primitiveInt64
                && Float.compare(that.primitiveFloat32, primitiveFloat32) == 0
                && Double.compare(that.primitiveFloat64, primitiveFloat64) == 0
                && primitiveBoolean == that.primitiveBoolean
                && Objects.equals(objectInt8, that.objectInt8)
                && Objects.equals(objectCharacter, that.objectCharacter)
                && Objects.equals(objectInt16, that.objectInt16)
                && Objects.equals(objectInt32, that.objectInt32)
                && Objects.equals(objectInt64, that.objectInt64)
                && Objects.equals(objectFloat32, that.objectFloat32)
                && Objects.equals(objectFloat64, that.objectFloat64)
                && Objects.equals(objectBoolean, that.objectBoolean)
                && Objects.equals(string, that.string)
                && Objects.equals(decimal, that.decimal)
                && Objects.equals(time, that.time)
                && Objects.equals(date, that.date)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(timestampWithTimezone, that.timestampWithTimezone)
                && anEnum == that.anEnum
                && Objects.equals(nestedRecord, that.nestedRecord)
                && Arrays.equals(primitiveInt8Array, that.primitiveInt8Array)
                && Arrays.equals(objectInt8Array, that.objectInt8Array)
                && Arrays.equals(primitiveCharArray, that.primitiveCharArray)
                && Arrays.equals(objectCharacterArray, that.objectCharacterArray)
                && Arrays.equals(primitiveInt16Array, that.primitiveInt16Array)
                && Arrays.equals(objectInt16Array, that.objectInt16Array)
                && Arrays.equals(primitiveInt32Array, that.primitiveInt32Array)
                && Arrays.equals(objectInt32Array, that.objectInt32Array)
                && Arrays.equals(primitiveInt64Array, that.primitiveInt64Array)
                && Arrays.equals(objectInt64Array, that.objectInt64Array)
                && Arrays.equals(primitiveFloat32Array, that.primitiveFloat32Array)
                && Arrays.equals(objectFloat32Array, that.objectFloat32Array)
                && Arrays.equals(primitiveFloat64Array, that.primitiveFloat64Array)
                && Arrays.equals(objectFloat64Array, that.objectFloat64Array)
                && Arrays.equals(primitiveBooleanArray, that.primitiveBooleanArray)
                && Arrays.equals(objectBooleanArray, that.objectBooleanArray)
                && Arrays.equals(stringArray, that.stringArray)
                && Arrays.equals(decimalArray, that.decimalArray)
                && Arrays.equals(timeArray, that.timeArray)
                && Arrays.equals(dateArray, that.dateArray)
                && Arrays.equals(timestampArray, that.timestampArray)
                && Arrays.equals(timestampWithTimezoneArray, that.timestampWithTimezoneArray)
                && Arrays.equals(anEnumArray, that.anEnumArray)
                && Arrays.equals(nestedRecordArray, that.nestedRecordArray)
                && Objects.equals(list, that.list)
                && Objects.equals(arrayList, that.arrayList)
                && Objects.equals(set, that.set)
                && Objects.equals(hashSet, that.hashSet)
                && Objects.equals(map, that.map)
                && Objects.equals(hashMap, that.hashMap);
    }

    public byte primitiveInt8() {
        return this.primitiveInt8;
    }

    public Byte objectInt8() {
        return this.objectInt8;
    }

    public char primitiveChar() {
        return this.primitiveChar;
    }

    public Character objectCharacter() {
        return this.objectCharacter;
    }

    public short primitiveInt16() {
        return this.primitiveInt16;
    }

    public Short objectInt16() {
        return this.objectInt16;
    }

    public int primitiveInt32() {
        return this.primitiveInt32;
    }

    public Integer objectInt32() {
        return this.objectInt32;
    }

    public long primitiveInt64() {
        return this.primitiveInt64;
    }

    public Long objectInt64() {
        return this.objectInt64;
    }

    public float primitiveFloat32() {
        return this.primitiveFloat32;
    }

    public Float objectFloat32() {
        return this.objectFloat32;
    }

    public double primitiveFloat64() {
        return this.primitiveFloat64;
    }

    public Double objectFloat64() {
        return this.objectFloat64;
    }

    public boolean primitiveBoolean() {
        return this.primitiveBoolean;
    }

    public Boolean objectBoolean() {
        return this.objectBoolean;
    }

    public String string() {
        return this.string;
    }

    public BigDecimal decimal() {
        return this.decimal;
    }

    public LocalTime time() {
        return this.time;
    }

    public LocalDate date() {
        return this.date;
    }

    public LocalDateTime timestamp() {
        return this.timestamp;
    }

    public OffsetDateTime timestampWithTimezone() {
        return this.timestampWithTimezone;
    }

    public AnEnum anEnum() {
        return this.anEnum;
    }

    public NestedRecord nestedRecord() {
        return this.nestedRecord;
    }

    public byte[] primitiveInt8Array() {
        return this.primitiveInt8Array;
    }

    public Byte[] objectInt8Array() {
        return this.objectInt8Array;
    }

    public char[] primitiveCharArray() {
        return this.primitiveCharArray;
    }

    public Character[] objectCharacterArray() {
        return this.objectCharacterArray;
    }

    public short[] primitiveInt16Array() {
        return this.primitiveInt16Array;
    }

    public Short[] objectInt16Array() {
        return this.objectInt16Array;
    }

    public int[] primitiveInt32Array() {
        return this.primitiveInt32Array;
    }

    public Integer[] objectInt32Array() {
        return this.objectInt32Array;
    }

    public long[] primitiveInt64Array() {
        return this.primitiveInt64Array;
    }

    public Long[] objectInt64Array() {
        return this.objectInt64Array;
    }

    public float[] primitiveFloat32Array() {
        return this.primitiveFloat32Array;
    }

    public Float[] objectFloat32Array() {
        return this.objectFloat32Array;
    }

    public double[] primitiveFloat64Array() {
        return this.primitiveFloat64Array;
    }

    public Double[] objectFloat64Array() {
        return this.objectFloat64Array;
    }

    public boolean[] primitiveBooleanArray() {
        return this.primitiveBooleanArray;
    }

    public Boolean[] objectBooleanArray() {
        return this.objectBooleanArray;
    }

    public String[] stringArray() {
        return this.stringArray;
    }

    public BigDecimal[] decimalArray() {
        return this.decimalArray;
    }

    public LocalTime[] timeArray() {
        return this.timeArray;
    }

    public LocalDate[] dateArray() {
        return this.dateArray;
    }

    public LocalDateTime[] timestampArray() {
        return this.timestampArray;
    }

    public OffsetDateTime[] timestampWithTimezoneArray() {
        return this.timestampWithTimezoneArray;
    }

    public AnEnum[] anEnumArray() {
        return this.anEnumArray;
    }

    public NestedRecord[] nestedRecordArray() {
        return this.nestedRecordArray;
    }

    public List<BigDecimal> list() {
        return this.list;
    }

    public ArrayList<String> arrayList() {
        return this.arrayList;
    }

    public Set<Byte> set() {
        return this.set;
    }

    public HashSet<Integer> hashSet() {
        return this.hashSet;
    }

    public Map<Short, Character> map() {
        return this.map;
    }

    public HashMap<Long, Boolean> hashMap() {
        return this.hashMap;
    }
}
