package tr.com.rcesur.kafka.utils;

/**
 * Created by ramazancesur on 11/05/2020.
 */

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RandomDataUtil {

    private static final Logger LOGGER = Logger.getLogger(RandomDataUtil.class.getName());
    private Random rand = new Random();

    public <T extends Object> T generateDummyData(Class cls, T mainObj) throws Exception {
        Field[] fields = cls.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if (!field.toString().contains("static final ")) {
                Class type = field.getType();
                Object value = null;
                if (type.isPrimitive()) {
                    value = getRandomValueForPrimitiveTypeField(field);
                    field.set(mainObj, value);
                } else if (type.equals(CharSequence.class) || type.getName().contains("java.lang")
                        || type.getName().contains("java.math") || type.getName().contains("java.math") || type.isEnum()) {
                    value = getRandomValueForNonPrimitiveTypeField(field);
                    field.set(mainObj, value);
                } else {
                    LOGGER.info(cls.getSimpleName() + ": " + field.getName() + " is a " + field.getType().getSimpleName());
                    Class innerClass = field.getType();

                    if (Collection.class.isAssignableFrom(innerClass)) {
                        ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
                        innerClass = (Class) parameterizedType.getActualTypeArguments()[0];
                        LOGGER.info(innerClass.getName());

                        Object innerClassObject = innerClass.newInstance();
                        innerClassObject = generateDummyData(innerClass, innerClassObject);
                        try {
                            field.set(mainObj, new ArrayList<>(Arrays.asList(innerClassObject)));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        try {
                            Object innerClassObject = innerClass.newInstance();
                            innerClassObject = generateDummyData(innerClass, innerClassObject);
                            field.set(mainObj, innerClassObject);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            LOGGER.severe(ex.getMessage());
                        }
                    }
                }
            }
        }
        return mainObj;
    }

    private Object getRandomValueForPrimitiveTypeField(Field field) throws Exception {
        Class<?> type = field.getType();
        try {
            if (type.equals(Integer.TYPE)) {
                return 1 + rand.nextInt(5);
            } else if (type.equals(Long.TYPE)) {
                return Long.valueOf(rand.nextInt(Integer.MAX_VALUE));
            } else if (type.equals(Double.TYPE)) {
                return rand.nextDouble();
            } else if (type.equals(Float.TYPE)) {
                return rand.nextFloat();
            } else if (type.equals(Boolean.TYPE)) {
                rand = new Random(2);
                return rand.nextInt() == 1;
            }
        } catch (Exception ex) {
            LOGGER.severe(ex.getMessage());
        }
        return null;
    }

    private Object getRandomValueForNonPrimitiveTypeField(Field field) throws Exception {
        Class<?> type = field.getType();
        try {
            if (type.equals(CharSequence.class)) {
                return UUID.randomUUID().toString();
            } else if (type.isEnum()) {
                Object[] enumValues = type.getEnumConstants();
                return enumValues[rand.nextInt(enumValues.length)];
            } else if (type.equals(Integer.class)) {
                return rand.nextInt(10);
            } else if (type.equals(Long.class)) {
                return Long.valueOf(rand.nextInt(Integer.MAX_VALUE));
            } else if (type.equals(Double.class)) {
                return rand.nextDouble();
            } else if (type.equals(Float.class)) {
                return rand.nextFloat();
            } else if (type.equals(Boolean.class)) {
                rand = new Random(2);
                return rand.nextInt() == 1;
            } else if (type.equals(String.class)) {
                return UUID.randomUUID().toString();
            } else if (type.equals(BigInteger.class)) {
                return BigInteger.valueOf(rand.nextInt());
            } else if (type.equals(BigDecimal.class)) {
                return BigDecimal.valueOf(rand.nextInt());
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE,ex.getMessage());
        }
        return null;
    }

}
