package com.org.kafkabatch.springbatchkafka.consumer;

import com.org.kafkabatch.springbatchkafka.dto.Customer;
import javafx.util.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.batch.core.Job;

import java.util.List;
import java.util.Properties;

@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
@Log4j2
public class ConsumerApplication {

    public static void main(String args[]) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
    private final KafkaProperties kafkaProperties;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    Job job() {
        return this.jobBuilderFactory
                .get("job-consumer")
                .incrementer(new RunIdIncrementer())
                .start(step())
                .build();
    }

    @Bean
    KafkaItemReader<Long , Customer> kafkaItemReader() {
        Properties properties = new Properties();
        properties.putAll(this.kafkaProperties.buildConsumerProperties());

        return new KafkaItemReaderBuilder<Long, Customer>()
                .partitions(0)
                .consumerProperties(properties)
                .name("customers-reader")
                .saveState(true)
                .topic("customers")
                .build();
    }

    @Bean
    Step step() {
        ItemWriter<Customer> writer = new ItemWriter<Customer>() {
            @Override
            public void write(List<? extends Customer> items) throws Exception {
                items.forEach(log::info);
            }
        };
        return this.stepBuilderFactory
                .get("step-consumer")
                .<Customer, Customer>chunk(10)
                .writer(writer)
                .reader(kafkaItemReader())
                .build();
    }

}
