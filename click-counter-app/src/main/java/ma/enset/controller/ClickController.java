package ma.enset.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Random;
import java.util.UUID;

@Controller
public class ClickController {
    private static final String TOPIC = "clicks";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/click")
    public String click(Model model) {
        String[] users = {"user1", "user2", "user3"};
        String userId = users[new Random().nextInt(users.length)];
        kafkaTemplate.send(TOPIC, userId, userId+":click");
        model.addAttribute("message", "Click sent by user: " + userId);
        return "index"; // returns index.html
    }

    @RequestMapping("/")
    public String home() {
        return "index";
    }
}
