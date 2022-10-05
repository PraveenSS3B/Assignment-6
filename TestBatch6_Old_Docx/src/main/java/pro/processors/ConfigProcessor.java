package pro.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import pro.dao.Person;

@Configuration
public class ConfigProcessor {
	Logger log = LoggerFactory.getLogger(ConfigProcessor.class);


	@Bean
	public ItemProcessor<Person, Person> txtProceesor() {
		 return person -> person;
	}
	
	@Bean
	public ItemProcessor<Person, Person> db2TxtProcessor() {
		log.trace("only Processing people from DB who are male.");
		 return person -> {
			 if(person.getGender().equalsIgnoreCase("male"))
				 return person;
			 return null;
		 };
	}

}
