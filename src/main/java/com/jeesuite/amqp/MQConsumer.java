/**
 * Confidential and Proprietary Copyright 2019 By 卓越里程教育科技有限公司 All Rights Reserved
 */
package com.jeesuite.amqp;

/**
 * 
 * <br>
 * Class Name   : Consumer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年7月11日
 */
public interface MQConsumer {

	public void start() throws Exception;
	
	public void shutdown();
}
