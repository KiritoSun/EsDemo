package com.zt.common.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CheckFunction {
    String value();
    boolean isCheck() default true;
}