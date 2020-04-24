package com.zt.test;

import org.junit.Test;

public class SampleTest {

    @Test
    public void fun1() {
        Object bean = new Student();

        if (bean instanceof People) {
            People people = (People) bean;
            System.out.println("找到了");
            people.talk();
        } else {
            System.out.println("没有找到");
        }
    }

    public interface People {
        void talk();
    }

    public class Student implements People {

        @Override
        public void talk() {
            System.out.println("我要学习");
        }
    }

    public class Teacher implements People {

        @Override
        public void talk() {
            System.out.println("我要上课");
        }
    }

}
