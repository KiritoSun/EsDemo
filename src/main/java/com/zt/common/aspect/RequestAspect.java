package com.zt.common.aspect;

import com.zt.common.annotation.CheckFunction;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@Aspect
@Component
@Slf4j
public class RequestAspect {

    @Pointcut("@annotation(com.zt.common.annotation.CheckFunction)))")
    public void pointcutFunction() {
        log.info("进入切入点...");
    }

    @Before("pointcutFunction()")
    public void before(JoinPoint joinPoint) throws Exception {
        log.info("目标方法名为:" + joinPoint.getSignature().getName());
        log.info("目标方法所属类的简单类名:" + joinPoint.getSignature().getDeclaringType().getSimpleName());
        log.info("目标方法所属类的类名:" + joinPoint.getSignature().getDeclaringTypeName());
        log.info("目标方法声明类型:" + Modifier.toString(joinPoint.getSignature().getModifiers()));
        //获取传入目标方法的参数
        Object[] args = joinPoint.getArgs();
        for (int i = 0; i < args.length; i++) {
            log.info("第" + (i+1) + "个参数为:" + args[i]);
        }
        log.info("被代理的对象:" + joinPoint.getTarget());
        log.info("代理对象自己:" + joinPoint.getThis());
        log.info("执行前置处理...");
    }

    @AfterThrowing("pointcutFunction()")
    public void afterThrowing() {
        log.error("出现异常了...");
    }

    @After("pointcutFunction()")
    public void after(JoinPoint joinPoint) throws Exception {
        Signature signature = joinPoint.getSignature();
        MethodSignature msg = (MethodSignature) signature;
        Object target = joinPoint.getTarget();
        //获取注解标注的方法
        Method method = target.getClass().getMethod(msg.getName(), msg.getParameterTypes());
        //通过方法获取注解
        CheckFunction checkFunction = method.getAnnotation(CheckFunction.class);
        log.info("checkFunction value：{}", checkFunction.value());
        log.info("执行后置处理...");
    }

}
