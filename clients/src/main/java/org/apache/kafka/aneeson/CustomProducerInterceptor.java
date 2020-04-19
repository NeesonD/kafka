package org.apache.kafka.aneeson;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daile
 * @version 1.0
 * @date 2020/4/19 11:53
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String,String> {

    private Logger logger = LoggerFactory.getLogger(CustomProducerInterceptor.class);

    private  AtomicLong sendSuccess = new AtomicLong();
    private  AtomicLong sendFailure = new AtomicLong();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if(record.value().length()<=0) {
            return null;
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess.getAndIncrement();
        } else {
            sendFailure.getAndIncrement();
        }
    }

    @Override
    public void close() {
        double successRatio = (double)sendSuccess.get() / (sendFailure.get() + sendSuccess.get());
        logger.info("[INFO] 发送成功率="+String.format("%f", successRatio * 100)+"%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
