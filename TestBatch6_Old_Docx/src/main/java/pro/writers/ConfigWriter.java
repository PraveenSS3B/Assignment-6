package pro.writers;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import pro.dao.Person;

@Configuration
public class ConfigWriter {
	Logger log = LoggerFactory.getLogger(ConfigWriter.class);

	@Autowired
	private DataSource dataSource;


	@Bean
	public JdbcBatchItemWriter<Person> jdbcWriterTxT2DB() {
		JdbcBatchItemWriter<Person> batchItemWriter = new JdbcBatchItemWriter<Person>();
		batchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		batchItemWriter.setSql(
				"INSERT INTO PERSON (ID, FIRST_NAME, LAST_NAME, GENDER) VALUES (:id, :firstName, :lastName, :gender)");
		batchItemWriter.setDataSource(dataSource);
		return batchItemWriter;

	}


	@Bean
	public FlatFileItemWriter<Person> db2txtItemWriter() {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
		writer.setResource(
				new FileSystemResource(System.getProperty("user.dir") + "/src/main/resources/output/DB2person.txt"));
		writer.setShouldDeleteIfExists(true);

		DelimitedLineAggregator<Person> delimitedLineAggregator = new DelimitedLineAggregator<Person>();
		delimitedLineAggregator.setDelimiter(" ");

		BeanWrapperFieldExtractor<Person> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Person>();
		beanWrapperFieldExtractor.setNames(new String[] { "id", "firstName", "lastName", "gender" });
		delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);
		writer.setLineAggregator(delimitedLineAggregator);
		log.trace("Writing into person.txt from Database....");
		return writer;
	}


}
