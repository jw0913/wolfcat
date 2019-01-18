package org.spring.demo.tag;

import org.spring.demo.tag.bean.TestBean;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import org.springframework.core.io.Resource;

public class AppTest {
    public static void main(String[] args) {
        Resource resource=new ClassPathResource("spring-config.xml");
        DefaultListableBeanFactory factory=new XmlBeanFactory(resource);
        System.out.println(factory.getBean("test"));
        System.out.println(factory.getBean(TestBean.class));
    }
}
