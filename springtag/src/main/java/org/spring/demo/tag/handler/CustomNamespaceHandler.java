package org.spring.demo.tag.handler;

import org.spring.demo.tag.parser.CustomElementParser;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

public class CustomNamespaceHandler  extends NamespaceHandlerSupport {
    public void init() {
            //注册用于解析自定义标签的解析器
            this. registerBeanDefinitionParser("test",new CustomElementParser());
    }
}
