using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Aix.MessageBus.Utils
{
    public static class ObjectUtils
    {
        public static T Copy<T>(T obj) where T : class
        {
            if (obj == null) return default(T);

            return (T)CopyOjbect(obj);

            //using (MemoryStream memoryStream = new MemoryStream())
            //{
            //    BinaryFormatter formatter = new BinaryFormatter();
            //    formatter.Serialize(memoryStream, obj);
            //    memoryStream.Position = 0;
            //    return (T)formatter.Deserialize(memoryStream);
            //}
        }

        private static object CopyOjbect(object obj)
        {
            if (obj == null)
            {
                return null;
            }
            Object targetDeepCopyObj;
            Type targetType = obj.GetType();
            //值类型  
            if (targetType.IsValueType == true)
            {
                targetDeepCopyObj = obj;
            }
            //引用类型   
            else
            {
                targetDeepCopyObj = Activator.CreateInstance(targetType);   //创建引用对象   
                MemberInfo[] memberCollection = obj.GetType().GetMembers();

                foreach (MemberInfo member in memberCollection)
                {
                    //拷贝字段
                    if (member.MemberType == MemberTypes.Field)
                    {
                        FieldInfo field = (FieldInfo)member;
                        Object fieldValue = field.GetValue(obj);
                        if (fieldValue is ICloneable)
                        {
                            field.SetValue(targetDeepCopyObj, (fieldValue as ICloneable).Clone());
                        }
                        else
                        {
                            field.SetValue(targetDeepCopyObj, CopyOjbect(fieldValue));
                        }

                    }//拷贝属性
                    else if (member.MemberType == MemberTypes.Property)
                    {
                        PropertyInfo myProperty = (PropertyInfo)member;

                        MethodInfo info = myProperty.GetSetMethod(false);
                        if (info != null)
                        {
                            object propertyValue = myProperty.GetValue(obj, null);
                            if (propertyValue is ICloneable)
                            {
                                myProperty.SetValue(targetDeepCopyObj, (propertyValue as ICloneable).Clone(), null);
                            }
                            else
                            {
                                myProperty.SetValue(targetDeepCopyObj, CopyOjbect(propertyValue), null);
                            }
                        }
                    }
                }
            }
            return targetDeepCopyObj;
        }
    }
}
