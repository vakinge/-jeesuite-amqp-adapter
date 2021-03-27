package com.jeesuite.amqp;

/**
 * 
 * <br>
 * Class Name : MQProducer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月13日
 */
public interface MQProducer {

	public void start() throws Exception;

	public String sendMessage(MQMessage message,boolean async);
	
	public void shutdown();
}
