package org.spring.demo.tag.bean;

import lombok.Data;

@Data
public class TestBean {
    private String id;
    private String name;

    private User user;
    @Override
    public String toString(){
        return "{id:"+this.id+",name:"+name+",user:"+user+"}";
    }
}
