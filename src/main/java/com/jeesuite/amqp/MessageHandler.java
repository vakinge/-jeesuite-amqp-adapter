package com.jeesuite.amqp;

/**
 * 
 * <br>
 * Class Name   : MessageHanlder
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月8日
 */
public interface MessageHandler {

	void process(MQMessage message) throws Exception;
}
