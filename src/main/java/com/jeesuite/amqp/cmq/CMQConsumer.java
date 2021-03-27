package com.jeesuite.amqp.cmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jeesuite.amqp.MQConsumer;
import com.jeesuite.amqp.MQMessage;
import com.jeesuite.amqp.MessageHandler;
import com.jeesuite.common.ThreadLocalContext;
import com.jeesuite.common.async.StandardThreadExecutor;
import com.jeesuite.common.async.StandardThreadExecutor.StandardThreadFactory;
import com.jeesuite.common.json.JsonUtils;
import com.jeesuite.common.util.ResourceUtils;
import com.qcloud.cmq.Message;

/**
 * 
 * <br>
 * Class Name   : CMQConsumer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年4月23日
 */
public class CMQConsumer implements MQConsumer {
	
	private static Logger logger = LoggerFactory.getLogger("com.zyframework.core.mq");

	private Map<String, MessageHandler> messageHandlers = new HashMap<>(); 

	private StandardThreadExecutor fetchExecutor;
	private StandardThreadExecutor defaultProcessExecutor;
	private Semaphore semaphore;
	
	private AtomicBoolean closed = new AtomicBoolean(false);

	public CMQConsumer(String groupId,Map<String, MessageHandler> messageHandlers) {
		this.messageHandlers = messageHandlers;
	}

	@Override
	public void start() throws Exception {
		//检查或者创建订阅关系
		Set<String> topicNames = messageHandlers.keySet();
		for (String topic : topicNames) {
		   CMQManager.createSubscriptionIfAbsent(topic);
		}
		fetchExecutor = new StandardThreadExecutor(messageHandlers.size(), messageHandlers.size(),0, TimeUnit.SECONDS, 1,new StandardThreadFactory("cmq-Fetch-Executor"));
		int maxThread = ResourceUtils.getInt("mq.consumer.processThreads", 20);
		semaphore = new Semaphore(maxThread);
		defaultProcessExecutor = new StandardThreadExecutor(1, maxThread,60, TimeUnit.SECONDS, 1,new StandardThreadFactory("cmq-defaultProcess-Executor"));
		fetchExecutor.execute(new Worker());
	}

	@Override
	public void shutdown() {
		closed.set(true);
		if(fetchExecutor != null) {
			fetchExecutor.shutdown();
		}
		if(defaultProcessExecutor != null) {
			defaultProcessExecutor.shutdown();
		}
	}
	
	private class Worker implements Runnable{
        
		final int batchSize = ResourceUtils.getInt("mq.cmq.consumer.batchSize", 1);
		@Override
		public void run() {
			while(!closed.get()){ 
				try {	
					List<Message> messages = CMQManager.getQueue().batchReceiveMessage(batchSize);
					if(messages == null || messages.isEmpty()){
						Thread.sleep(10);
						continue;
					}
					for (Message msg : messages) {
						processSingleMessage(msg);
					}
				} catch (Exception e) {
					if(e.getMessage() == null || !e.getMessage().endsWith("no message")) {						
						e.printStackTrace();
					}
				}
			}
		}
		
		
		private void processSingleMessage(Message msg) throws InterruptedException {
			if(logger.isDebugEnabled())logger.debug(">>received_cmq_message -> msgId:{},requestId:{},firstDequeueTime:{}",msg.msgId,msg.requestId,msg.firstDequeueTime);
			final MQMessage _message = MQMessage.build(msg.msgBody);
			MessageHandler messageHandler = messageHandlers.get(_message.getTopic());
			if(messageHandler == null) {
				try {CMQManager.getQueue().deleteMessage(msg.receiptHandle);} catch (Exception e) {}
				logger.warn("not_messageHandler_found -> topicName:{},messageId:{},DequeueCount:{}",_message.getTopic(),msg.msgId,msg.dequeueCount);
				return;
			}
			//信号量获取通行证
			semaphore.acquire();
			defaultProcessExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {	
						//多租户支持
						ThreadLocalContext.set(ThreadLocalContext.TENANT_ID_KEY, _message.getTenantId());
						messageHandler.process(_message);
						//处理成功，删除
						try {CMQManager.getQueue().deleteMessage(msg.receiptHandle);} catch (Exception e) {}
						logger.debug("MQ_MESSAGE_CONSUME_SUCCESS -> topicName:{},messageId:{},DequeueCount:{}",_message.getTopic(),msg.msgId,msg.dequeueCount);
					}catch (Exception e) {
						logger.error(String.format("MQ_MESSAGE_CONSUME_ERROR -> topicName:%s,msg:%s",_message.getTopic(),JsonUtils.toJson(msg)),e);
					} finally {
						ThreadLocalContext.unset();
						//释放信号量
						semaphore.release();
					}
				}
			});
		}
		
	}

}
