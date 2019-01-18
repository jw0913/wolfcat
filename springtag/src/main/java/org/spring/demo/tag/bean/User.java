package org.spring.demo.tag.bean;

import lombok.Data;

@Data
public class User {
    private String name;
    private int age;

    @Override
    public String toString(){
        return "{name:"+this.name+","+"age:"+age+"}";
    }
}
