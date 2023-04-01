package org.onepiece.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.onepiece.model.Notification;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static org.onepiece.constant.Constants.TOPIC;

@Component
@Slf4j
@EnableScheduling
@AllArgsConstructor
public class StreamProducer {
    private RedisTemplate<String, Object> redisTemplate;
    @Scheduled(fixedRate = 10000)
    public void send() {
        Notification notification = new Notification();
        notification.setDatasetId(2l);
        notification.setScenarioId(109761l);
        ObjectRecord<String, Notification> record =
                StreamRecords.newRecord()
                        .in(TOPIC)
                        .ofObject(notification)
                        .withId(RecordId.autoGenerate());
        RecordId recordId = redisTemplate.opsForStream()
                .add(record);
        log.info("record-id: [{}]", recordId);
    }
}
