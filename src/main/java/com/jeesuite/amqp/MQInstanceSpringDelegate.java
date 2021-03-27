package com.jeesuite.amqp;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

/**
 * 
 * <br>
 * Class Name   : MQInstanceSpringDelegate
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月13日
 */
public class MQInstanceSpringDelegate extends MQInstanceBaseDelegate implements InitializingBean, PriorityOrdered{

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE + 1;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		start();
	}

	
	
}
