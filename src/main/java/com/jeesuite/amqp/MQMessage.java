package com.jeesuite.amqp;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.jeesuite.common.json.JsonUtils;
import com.jeesuite.common.util.BeanUtils;

/**
 * 
 * <br>
 * Class Name   : MQMessage
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月13日
 */
@JsonInclude(Include.NON_NULL)
public class MQMessage {

	private String requestId;
	private String tenantId;
	private String produceBy;
	private String topic;
	private String tag;
	private String bizKey;
	private Object body;
	
	public MQMessage() {}
	
	public static MQMessage build(String json) {
		MQMessage message = JsonUtils.toObject(json, MQMessage.class);
		if(!BeanUtils.isSimpleDataType(message.body.getClass())){
			message.setBody(JsonUtils.toJson(message.body));
		}
		return message;
	}
	

	public MQMessage(String topic, Object body) {
		this(topic, null, body);
	}
	
	public MQMessage(String topic, String bizKey,Object body) {
		this(topic, null, bizKey, body);
	}
	
	public MQMessage(String topic,String tag,String bizKey, Object body) {
		this.topic = topic;
		this.tag = tag;
		this.bizKey = bizKey;
		if(body instanceof byte[]){
			this.body = new String((byte[])body,StandardCharsets.UTF_8);
		}else{			
			this.body = body;
		}
	}
	
	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getProduceBy() {
		return produceBy;
	}

	public void setProduceBy(String produceBy) {
		this.produceBy = produceBy;
	}

	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}
	/**
	 * @param topic the topic to set
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	
	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	/**
	 * @return the bizKey
	 */
	public String getBizKey() {
		return bizKey;
	}

	/**
	 * @param bizKey the bizKey to set
	 */
	public void setBizKey(String bizKey) {
		this.bizKey = bizKey;
	}
	
	/**
	 * @return the body
	 */
	public Object getBody() {
		return body;
	}

	/**
	 * @param body the body to set
	 */
	public void setBody(Object body) {
		this.body = body;
	}
	
	public byte[] bodyAsBytes(){
		if(BeanUtils.isSimpleDataType(body.getClass())){
			return body.toString().getBytes(StandardCharsets.UTF_8);
		}else{
			return JsonUtils.toJson(body).getBytes(StandardCharsets.UTF_8);
		}
	}
	
	public String toJSON(){
		return JsonUtils.toJson(this);
	}

	public <T> T toObject(Class<T> clazz){
		return JsonUtils.toObject(body.toString(), clazz);
	}
	
	public <T> List<T> toList(Class<T> clazz){
		return JsonUtils.toList(body.toString(), clazz);
	}


	@Override
	public String toString() {
		return "MQMessage [topic=" + topic + ", tag=" + tag + ", requestId=" + requestId + ", tenantId="
				+ tenantId + ", produceBy=" + produceBy + ", bizKey=" + bizKey
				+ "]";
	}

	
	
}
