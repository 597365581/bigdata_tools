package com.yongqing.common.bigdata.tool;


import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;


import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

@Log4j2
public class ReflectionUtils {

    /**
     * 获取一个类和其父类的所有属性
     *
     * @param clazz * @return
     */
    public static List<Field> findAllFieldsOfSelfAndSuperClass(Class clazz) {
        Field[] fields = null;
        List fieldList = new ArrayList();
        while (true) {
            if (clazz == null) {
                break;
            } else {
                fields = clazz.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    fieldList.add(fields[i]);
                }
                clazz = clazz.getSuperclass();
            }
        }
        return fieldList;
    }

    /**
     * 把一个Bean对象转换成Map对象</br>
     *
     * @param obj     对像
     * @param ignores 忽略属性
     * @return 对像map
     * @throws IllegalAccessException
     */
    public static Map<String, Object> convertBean2Map(Object obj, String[] ignores) {
        Map<String, Object> map = new HashMap<String, Object>();
        Class clazz = obj.getClass();
        List<Field> fieldList = findAllFieldsOfSelfAndSuperClass(clazz);
        Field field = null;
        try {
            for (int i = 0; i < fieldList.size(); i++) {
                field = fieldList.get(i);
                // 定义fieldName是否在拷贝忽略的范畴内
                boolean flag = false;
                if (ignores != null && ignores.length != 0) {
                    flag = isExistOfIgnores(field.getName(), ignores);
                }
                if (!flag) {
                    Object value = getProperty(obj, field.getName());
                    if (null != value && !StringUtils.EMPTY.equals(value.toString())) {
                        map.put(field.getName(), getProperty(obj, field.getName()));
                    }
                }
            }
        } catch (SecurityException e) {
            log.error("SecurityException",e);
        } catch (IllegalArgumentException e) {
            log.error("IllegalArgumentException",e);
        }
        return map;
    }

    /**
     * 把一个Bean对象转换成Map对象,空值不丢弃</br>
     *
     * @param obj     对像
     * @param ignores 忽略属性
     * @return 对像map
     * @throws IllegalAccessException
     */
    public static Map<String, Object> convertBean2MapWithEmptyVal(Object obj, String[] ignores) {
        Map<String, Object> map = new HashMap<String, Object>();
        Class clazz = obj.getClass();
        List<Field> fieldList = findAllFieldsOfSelfAndSuperClass(clazz);
        Field field = null;
        try {
            for (int i = 0; i < fieldList.size(); i++) {
                field = fieldList.get(i);
                // 定义fieldName是否在拷贝忽略的范畴内
                boolean flag = false;
                if (ignores != null && ignores.length != 0) {
                    flag = isExistOfIgnores(field.getName(), ignores);
                }
                if (!flag) {
                    map.put(field.getName(), getProperty(obj, field.getName()));
                }
            }
        } catch (SecurityException e) {
            log.error("SecurityException",e);
        } catch (IllegalArgumentException e) {
            log.error("IllegalArgumentException",e);
        }
        return map;
    }

    public static Object convertMapToBean(Class type, Map map) throws IntrospectionException, IllegalAccessException, InstantiationException, InvocationTargetException {
        BeanInfo beanInfo = Introspector.getBeanInfo(type); // 获取类属性
        Object obj = type.newInstance(); // 创建 JavaBean 对象
        Date date = new Date();
        // 给 JavaBean 对象的属性赋值
        PropertyDescriptor[] propertyDescriptors =  beanInfo.getPropertyDescriptors();
        for (int i = 0; i< propertyDescriptors.length; i++) {
            PropertyDescriptor descriptor = propertyDescriptors[i];
            String propertyName = descriptor.getName();
            if (map.containsKey(propertyName)) {
                Object value = map.get(propertyName);
                //判断如果为时间格式，做转换
                if(descriptor.getPropertyType().isInstance(date))   {
                 // 下面一句可以 try 起来，这样当一个属性赋值失败的时候就不会影响其他属性赋值。
                 value = DateCommonUtil.changeStringToDate((String)map.get(propertyName));
             }
                Object[] args = new Object[1];
                args[0] = value;

                descriptor.getWriteMethod().invoke(obj, args);
            }
        }
        return obj;
    }
    /**
     * 把一个Bean对象转换成Map对象</br>
     * *param clazz
     *
     * @return
     */
    public static Map<String, Object> convertBean2Map(Object obj) {
        return convertBean2Map(obj, null);
    }

    /**
     * 把一个Bean对象转换成Map对象,包含空属性</br>
     * *param clazz
     *
     * @return
     */
    public static Map<String, Object> convertBean2MapWithEmptyVal(Object obj) {
        return convertBean2MapWithEmptyVal(obj, null);
    }

    public static Map convertBean2MapForIngoreserialVersionUID(Object obj) {
        return convertBean2Map(obj, new String[]{"serialVersionUID"});
    }

    /**
     * 判断fieldName是否是ignores中排除的
     *
     * @param fieldName
     * @param ignores
     * @return
     */
    private static boolean isExistOfIgnores(String fieldName, String[] ignores) {
        boolean flag = false;
        for (String str : ignores) {
            if (str.equals(fieldName)) {
                flag = true;
                break;
            }
        }
        return flag;
    }

    /**
     * 获取属性描述符
     *
     * @param clazz        类
     * @param propertyName 属性名
     * @return 属性描述
     */
    public static PropertyDescriptor getPropertyDescriptor(Class clazz, String propertyName) {
        StringBuilder sb = new StringBuilder();// 构建一个可变字符串用来构建方法名称
        Method setMethod = null;
        Method getMethod = null;
        PropertyDescriptor pd = null;
        try {
            Field f = clazz.getDeclaredField(propertyName);// 根据字段名来获取字段
            if (f != null) {
                // 构建方法的后缀
                String methodEnd = propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
                sb.append("set" + methodEnd);// 构建set方法
                setMethod = clazz.getDeclaredMethod(sb.toString(),
                        new Class[]{f.getType()});
                sb.delete(0, sb.length());// 清空整个可变字符串
                sb.append("get" + methodEnd);// 构建get方法
                // 构建get 方法
                getMethod = clazz.getDeclaredMethod(sb.toString(), new Class[]{});
                // 构建一个属性描述器 把对应属性 propertyName 的 get 和 set 方法保存到属性描述器中
                pd = new PropertyDescriptor(propertyName, getMethod, setMethod);
            }
        } catch (Exception ex) {
           log.error("PropertyDescriptor cause Exception",ex);
        }
        return pd;
    }

    @SuppressWarnings("unchecked")
    public static void setProperty(Object obj, String propertyName, Object value) {
        Class clazz = obj.getClass();// 获取对象的类型
        PropertyDescriptor pd = getPropertyDescriptor(clazz, propertyName);
        // 获取clazz类型中的propertyName的属性描述器
        Method setMethod = pd.getWriteMethod();// 从属性描述器中获取 set 方法
        try {
            setMethod.invoke(obj, new Object[]{value});// 调用 set 方法将传入的value值保存属性中去
        } catch (Exception e) {
            log.error("setProperty cause Exception",e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object getProperty(Object obj, String propertyName) {
        Class clazz = obj.getClass();// 获取对象的类型
        PropertyDescriptor pd = getPropertyDescriptor(clazz, propertyName);// 获取 clazz  类型中的   propertyName    的属性描述器
        Method getMethod = pd.getReadMethod();// 从属性描述器中获取 get 方法
        Object value = null;
        try {
            value = getMethod.invoke(obj, new Object[]{});// 调用方法获取方法的返回值
        } catch (Exception e) {
            log.error("getProperty cause Exception",e);
        }
        return value;// 返回值
    }
}
