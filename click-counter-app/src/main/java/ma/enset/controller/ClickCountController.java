package ma.enset.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/clicks")
public class ClickCountController {

    private final Map<String, Long> clickCountsMap = new ConcurrentHashMap<>();

    @KafkaListener(topics = "click-counts", groupId = "click-counter-group")
    public void listen(ConsumerRecord<String, Long> record) {
        clickCountsMap.put(record.key(), record.value());
        System.out.println("User: " + record.key() + " -> Clicks: " + record.value());
    }

    @GetMapping("/count/all")
    public Map<String, Long> getAllClickCounts() {
        return clickCountsMap;
    }

    @GetMapping("/count/{userId}")
    public Long getUserClickCount(@PathVariable String userId) {
        return clickCountsMap.getOrDefault(userId, 0L);
    }
}
