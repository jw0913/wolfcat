package org.javassist.demo;

import javassist.*;
import javassist.bytecode.AccessFlag;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;


public class JavassistDemo {
    public static void main(String[] args) throws Exception {
        JavassistDemo j=new JavassistDemo();
        //j.generateClass();
        j.proxy();
    }
    public void generateClass(){
        ClassPool pool=ClassPool.getDefault();
        CtClass ct=pool.makeClass("org.test.GenerateClass");
        ct.setInterfaces(new CtClass[]{pool.makeInterface("java.lang.Cloneable")});
        try{
            //获得一个类型为int，名称为id的字段
            CtField f=new CtField(CtClass.intType, "id",ct);
            //将字段设置为public
            f.setModifiers(AccessFlag.PUBLIC);
            //将字段设置到类上
            ct.addField(f);
            //添加构造函数
            CtConstructor constructor=CtNewConstructor.make("public GeneratedClass(int pId){this.id=pId;}",ct);
            ct.addConstructor(constructor);
            //添加方法
            CtMethod helloM=CtNewMethod.make("public void hello(String des){ System.out.println(des);}",ct);
            ct.addMethod(helloM);
            //将生成的.class文件保存到磁盘
           // ct.writeFile();
            //下面的代码为验证代码
            Class clazz=ct.toClass();
            Field[] fields = clazz.getFields();
            System.out.println("属性名称：" + fields[0].getName() + "  属性类型：" + fields[0].getType());

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void proxy() throws Exception{
        // 创建代理工厂
        ProxyFactory proxyFactory = new ProxyFactory();

        // 设置被代理类的类型
        proxyFactory.setSuperclass(RayTest.class);

        // 创建代理类的class
        Class<ProxyObject> proxyClass = proxyFactory.createClass();

        // 创建对象
        RayTest proxyTest = (RayTest)proxyClass.newInstance();

        ((ProxyObject) proxyTest).setHandler(new MethodHandler() {
            // 真实主题
            RayTest test = new RayTest();

            public Object invoke(Object self, Method thisMethod,
                                 Method proceed, Object[] args) throws Throwable {
                String before = "before ";
                Object str = thisMethod.invoke(test, args);
                String after = " after";
                return before + str + after;
            }
        });
        String exe = proxyTest.exe();
        System.out.println(exe);

    }

    // 动态代码创建的例子
    //Javassist 的 API成功组织出代理类的一个子类，可以看出 添加构造函数，添加属性，
    // 添加方法，内容 都是通过字符串类型完成即可。 通过 Javassist 强大的字节生成能力可以达到动态
    // 增加类和实现动态代理的功能.
    public static void proxyJavassistDefineClass() throws Exception  {
        // 创建类池，true 表示使用默认路径
        ClassPool classPool = new ClassPool(true);

        String className = RayTest.class.getName();
        // 创建一个类 RayTestJavassistProxy
        CtClass ctClass = classPool.makeClass(className + "JavassistProxy");

        // 添加超类
        // 设置 RayTestJavassistProxy 的父类是 RayTest.
        ctClass.setSuperclass(classPool.get(RayTest.class.getName()));

        // 添加默认构造函数
        ctClass.addConstructor(CtNewConstructor.defaultConstructor(ctClass));

        // 添加属性
        ctClass.addField(CtField.make("public " + className + " real = new " +
                className + "();", ctClass));

        // 添加方法，里面进行动态代理 logic
        ctClass.addMethod(CtNewMethod.make("public String exe() { return \"before \" + real.exe() + \" after\";}",
                ctClass));
        Class<RayTest> testClass = ctClass.toClass();
        RayTest rayTest = testClass.newInstance();
        String exe = rayTest.exe();
        System.out.println(exe);


    }

}
