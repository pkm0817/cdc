package com.example.membersync.adapter.in.kafka;

import com.example.membersync.application.port.in.SyncMemberUseCase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 인바운드 어댑터. 역직렬화만 하고 UseCase 를 호출한다 — 로직은 여기 두지 않는다.
 */
@Component
public class MemberCdcListener {

    private static final Logger log = LoggerFactory.getLogger(MemberCdcListener.class);

    private final ObjectMapper objectMapper;
    private final SyncMemberUseCase syncMemberUseCase;

    public MemberCdcListener(ObjectMapper objectMapper, SyncMemberUseCase syncMemberUseCase) {
        this.objectMapper = objectMapper;
        this.syncMemberUseCase = syncMemberUseCase;
    }

    @KafkaListener(topics = "${app.cdc.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onChange(ConsumerRecord<String, String> record) throws Exception {
        // delete 직후 Debezium 이 보내는 tombstone(value=null). 로그 압축용이므로 무시한다.
        if (record.value() == null) {
            log.debug("tombstone skipped key={}", record.key());
            return;
        }

        MemberChangeEvent event = objectMapper.readValue(record.value(), MemberChangeEvent.class);
        syncMemberUseCase.sync(event);
    }
}
