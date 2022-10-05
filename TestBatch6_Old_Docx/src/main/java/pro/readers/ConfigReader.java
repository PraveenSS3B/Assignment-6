package pro.readers;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import pro.dao.Person;

@Configuration
public class ConfigReader {

	@Autowired
	private DataSource dataSource;

	Logger log = LoggerFactory.getLogger(ConfigReader.class);


	@Bean
	public JdbcPagingItemReader<Person> readerJdbc() {
		JdbcPagingItemReader<Person> reader = new JdbcPagingItemReader<Person>();
		MySqlPagingQueryProvider query = new MySqlPagingQueryProvider();
		query.setSelectClause("SELECT ID, FIRST_NAME,LAST_NAME, GENDER");
		query.setFromClause("FROM PERSON");
		Map<String, Order> sortConfiguration = new HashMap<>();
		sortConfiguration.put("first_name", Order.ASCENDING);
		query.setSortKeys(sortConfiguration);
		reader.setDataSource(dataSource);
		reader.setQueryProvider(query);
		reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));
		return reader;
	}

	public LineMapper<Person> lineMapperTXT() {
		DefaultLineMapper<Person> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(" ");
		lineTokenizer.setNames(new String[] { "id", "firstName", "lastName", "gender" });
		BeanWrapperFieldSetMapper<Person> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<Person>();
		beanWrapperFieldSetMapper.setTargetType(Person.class);
		defaultLineMapper.setLineTokenizer(lineTokenizer);
		defaultLineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);
		return defaultLineMapper;
	}

	@Bean
	public FlatFileItemReader<Person> txtReader() {
		FlatFileItemReader<Person> flatFileItemReader = new FlatFileItemReader<Person>();
		flatFileItemReader.setResource(new ClassPathResource("/input/person.txt"));
		flatFileItemReader.setLineMapper(lineMapperTXT());
		log.trace("Reading From TXT File...");
		return flatFileItemReader;
	}


}
