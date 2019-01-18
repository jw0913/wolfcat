package org.spring.demo.tag.parser;

import org.spring.demo.tag.bean.TestBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

public class CustomElementParser  implements BeanDefinitionParser {
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        RootBeanDefinition nodeWrapDefinition = new RootBeanDefinition();
        //设置BeanDefinition对应的类
        nodeWrapDefinition.setBeanClass(TestBean.class);
        String id=element.getAttribute("id");
        String name=element.getAttribute("name");
        String user=element.getAttribute("user");
        nodeWrapDefinition.getPropertyValues().addPropertyValue("id",id);
        nodeWrapDefinition.getPropertyValues().addPropertyValue("name",name);
        nodeWrapDefinition.getPropertyValues().addPropertyValue("user",new RuntimeBeanReference(user));

        parserContext.getRegistry().registerBeanDefinition(id,nodeWrapDefinition);
        return nodeWrapDefinition;
    }
}
