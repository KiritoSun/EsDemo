package com.zt.common.context;

import com.zt.common.annotation.ES;
import com.zt.service.EsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;

@Slf4j
@Component
public class TestBeanPostProcessor implements BeanPostProcessor {

    @Autowired
    private EsContext context;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ES es = AnnotationUtils.findAnnotation(bean.getClass(), ES.class);

        if (es == null || es.value().length <= 0) {
            return bean;
        }

        if (bean instanceof EsService) {
            for (String value : es.value()) {
                context.put(value, (EsService) bean);
            }
        }

        return bean;
    }
}
