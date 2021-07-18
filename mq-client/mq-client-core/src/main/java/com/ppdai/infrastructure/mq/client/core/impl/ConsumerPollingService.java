package com.ppdai.infrastructure.mq.client.core.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ppdai.infrastructure.mq.client.core.IMqClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ppdai.infrastructure.mq.biz.MqEnv;
import com.ppdai.infrastructure.mq.biz.common.thread.SoaThreadFactory;
import com.ppdai.infrastructure.mq.biz.common.trace.TraceFactory;
import com.ppdai.infrastructure.mq.biz.common.trace.TraceMessage;
import com.ppdai.infrastructure.mq.biz.common.trace.TraceMessageItem;
import com.ppdai.infrastructure.mq.biz.common.trace.Tracer;
import com.ppdai.infrastructure.mq.biz.common.trace.spi.Transaction;
import com.ppdai.infrastructure.mq.biz.common.util.JsonUtil;
import com.ppdai.infrastructure.mq.biz.common.util.Util;
import com.ppdai.infrastructure.mq.biz.dto.client.GetConsumerGroupRequest;
import com.ppdai.infrastructure.mq.biz.dto.client.GetConsumerGroupResponse;
import com.ppdai.infrastructure.mq.client.MqClient;
import com.ppdai.infrastructure.mq.client.MqContext;
import com.ppdai.infrastructure.mq.client.core.IConsumerPollingService;
import com.ppdai.infrastructure.mq.client.core.IMqGroupExcutorService;
import com.ppdai.infrastructure.mq.client.factory.IMqFactory;
import com.ppdai.infrastructure.mq.client.resource.IMqResource;

public class ConsumerPollingService implements IConsumerPollingService {

	private Logger log = LoggerFactory.getLogger(ConsumerPollingService.class);
    /**
     * 线程池，拉取最新的 Consumer 的消费组信息
     */
	private ThreadPoolExecutor executor = null;
	private TraceMessage traceMsg = TraceFactory.getInstance("ConsumerPollingService");
	private AtomicBoolean startFlag = new AtomicBoolean(false);
    /**
     * 线程池集合，负责消费消息
     *
     * key：消费组名称
     */
    private Map<String, IMqGroupExcutorService> mqExecutors = new ConcurrentHashMap<>();
	private MqContext mqContext = null;
	private IMqResource mqResource;
	private IMqFactory mqFactory;
	private volatile boolean isStop = false;
	private volatile boolean runStatus = false;

	public ConsumerPollingService() {
		this(MqClient.getMqFactory().createMqResource(MqClient.getContext().getConfig().getUrl(), 32000, 32000));
	}

	public ConsumerPollingService(IMqResource mqResource) {
		this.mqContext = MqClient.getContext();
		this.mqResource = mqResource;
		this.mqFactory = MqClient.getMqFactory();
		this.mqContext.setMqPollingResource(mqResource);
	}

	@Override
	public void start() {
		if (startFlag.compareAndSet(false, true)) {
			isStop = false;
			runStatus = false;
			// 单线程池
			executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(100), // TODO 待读：为啥是 100？
					SoaThreadFactory.create("ConsumerPollingService", true),
					new ThreadPoolExecutor.DiscardOldestPolicy());
			executor.execute(() -> {
                while (!isStop) {
                    TraceMessageItem traceMessageItem = new TraceMessageItem();
                    runStatus = true;
                    try {
                        traceMessageItem.status = "suc";
                        // 执行长轮询
                        longPolling();
                    } catch (Throwable e) {
                        // e.printStackTrace();
                        traceMessageItem.status = "fail";
                        Util.sleep(1000);
                    }
                    traceMsg.add(traceMessageItem);
                    runStatus = false;
                }
            });
		}
	}

	protected void longPolling() {
		if (mqContext.getConsumerId() > 0 && mqContext.getConsumerGroupVersion() != null
				&& mqContext.getConsumerGroupVersion().size() > 0) {
			Transaction transaction = Tracer.newTransaction("mq-group", "longPolling");
			try {
			    // 获得 Consumer 对应的消费组
				GetConsumerGroupRequest request = new GetConsumerGroupRequest();
				request.setConsumerId(mqContext.getConsumerId());
				request.setConsumerGroupVersion(mqContext.getConsumerGroupVersion());
				GetConsumerGroupResponse response = mqResource.getConsumerGroup(request);
				if (response != null && response.getConsumerDeleted() == 1) {
					log.info("consumerid为" + request.getConsumerId());
				}
				// 处理拉取的结果
				handleGroup(response);
				transaction.setStatus(Transaction.SUCCESS);
			} catch (Exception e) {
				transaction.setStatus(e);
			} finally {
				transaction.complete();
			}
		} else {
			Util.sleep(1000);
		}
	}

	protected void handleGroup(GetConsumerGroupResponse response) {
		if (isStop) {
			return;
		}
		if (response != null) {
			mqContext.setBrokerMetaMode(response.getBrokerMetaMode());
			if (MqClient.getMqEnvironment() != null && MqClient.getMqEnvironment().getEnv() == MqEnv.FAT) {
				MqClient.getContext().setAppSubEnvMap(response.getConsumerGroupSubEnvMap());
			}
		}
	/*	if (response != null && response.getConsumerDeleted() == 1) {
			MqClient.reStart();
			Util.sleep(5000);
			return;
		} else*/
		if (response != null && response.getConsumerGroups() != null
				&& response.getConsumerGroups().size() > 0) {
			log.info("get_consumer_group_data,获取到的最新消费者组数据为：" + JsonUtil.toJson(response));
			TraceMessageItem item = new TraceMessageItem();
			item.status = "changed";
			item.msg = JsonUtil.toJson(response);
			// 遍历每个消费组，key = 消费组
			response.getConsumerGroups().forEach((key, value) -> {
                if (!isStop) {
                    // TODO 待读：如果有新的消费者组，则进行创建
                    if (!mqExecutors.containsKey(key)) {
                        mqExecutors.put(key, mqFactory.createMqGroupExcutorService());
                    }
                    log.info("consumer_group_data_change,消费者组" + key + "发生重平衡或者meta更新");
                    // 进行重平衡操作或者更新元数据信息
                    mqExecutors.get(key).rbOrUpdate(value, response.getServerIp());
                    // 更新本地
                    mqContext.getConsumerGroupVersion().put(key, value.getMeta().getVersion());
                }
            });
			traceMsg.add(item);
		}
		// 启动每个消费组
		mqExecutors.values().forEach(IMqClientService::start);
	}

	@Override
	public void close() {
		isStop = true;
		try {
			mqExecutors.values().forEach(IMqClientService::close);
			mqExecutors.clear();
		} catch (Exception e) {
			// TODO: handle exception
		}
		long start = System.currentTimeMillis();
		// 这是为了等待有未完成的任务
		while (runStatus) {
			Util.sleep(10);
			// System.out.println("closing...................."+isRunning);
			if (System.currentTimeMillis() - start > 10000) {
				break;
			}
		}
		try {
			executor.shutdown();
		} catch (Exception e) {
		}
		startFlag.set(false);
		isStop = true;
	}

	@Override
	public Map<String, IMqGroupExcutorService> getMqExecutors() {
		// TODO Auto-generated method stub
		return mqExecutors;
	}
}
