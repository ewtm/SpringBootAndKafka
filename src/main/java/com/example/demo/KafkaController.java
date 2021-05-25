package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
	
	private KafkaTemplate<String, String> kafkaTemplate;
	private Gson jsonConverter;
	
	@Autowired
	public KafkaController(KafkaTemplate<String, String> kafkaTemplate, Gson jsonConverter) {
		this.kafkaTemplate = kafkaTemplate;
		this.jsonConverter = jsonConverter;
	}
	
	@PostMapping
	public void post(@RequestBody SimpleModel simpleModel) {
		kafkaTemplate.send("myTopic", jsonConverter.toJson(simpleModel));
	}
	
	@KafkaListener(topics = "myTopic")
	public void getFromKafka(String simpleModel) {
		
		SimpleModel  simpleModel1 = (SimpleModel) jsonConverter.fromJson(simpleModel, SimpleModel.class);
		
		System.out.println(simpleModel1.toString());
		
	}

}
