package kh.com.im.temporaldemo.config

import org.apache.ibatis.session.SqlSessionFactory
import org.mybatis.spring.batch.MyBatisBatchItemWriter
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder
import org.slf4j.LoggerFactory
import org.springframework.batch.core.annotation.AfterWrite
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.Step
import org.springframework.batch.core.step.StepExecution
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.infrastructure.item.Chunk
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.core.retry.RetryPolicy
import org.springframework.transaction.PlatformTransactionManager
import java.time.Duration
import java.time.temporal.ChronoUnit

@Configuration
//@EnableJdbcJobRepository
class GenericBatchConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val sqlSessionFactory: SqlSessionFactory
) {

    @Bean
    @StepScope
    fun genericCsvReader(
        @Value("#{jobParameters}") allParams: Map<String, Any>? // Inject the whole map
    ): FlatFileItemReader<Map<String, Any>> {

        val path =
            allParams?.get("filePath").toString() ?: throw IllegalArgumentException("filePath job parameter is missing")

        return FlatFileItemReaderBuilder<Map<String, Any>>()
            .name("genericReader")
            .resource(FileSystemResource(path)) // Use the safe 'path' variable
            .linesToSkip(1)
            .lineMapper(DefaultLineMapper<Map<String, Any>>().apply {
                setLineTokenizer(DelimitedLineTokenizer().apply {
                    setNames("col1", "col2", "col3")
                    setStrict(false)
                })
                setFieldSetMapper { fieldSet ->
                    val row = mutableMapOf<String, Any>()
                    fieldSet.properties.stringPropertyNames().forEach { key ->
                        row[key] = fieldSet.properties.getProperty(key)
                    }
                    row
                }
            }).build()
    }

    @Bean
    @StepScope
    fun myBatisWriter(
        @Value("#{jobParameters['mybatisId']}") mybatisId: String?
    ): MyBatisBatchItemWriter<Map<String, Any>> {

        val statementId = mybatisId ?: throw IllegalArgumentException("mybatisId job parameter is missing")

        return MyBatisBatchItemWriterBuilder<Map<String, Any>>()
            .sqlSessionFactory(sqlSessionFactory)
            .statementId(statementId)
            .assertUpdates(false)
            .build()
    }

    @Bean
    fun genericStep(
        reader: FlatFileItemReader<Map<String, Any>>,
        writer: MyBatisBatchItemWriter<Map<String, Any>>
    ): Step {

        return StepBuilder("genericStep", jobRepository)
            .chunk<Map<String, Any>, Map<String, Any>>(100)
            .transactionManager(transactionManager)
            .reader(reader)
            .writer(writer)
            .listener(ModernOffsetLogger())
            .faultTolerant()
            .retryLimit(3)
            .retry(Exception::class.java)
            .retryPolicy(
                RetryPolicy.builder()
                    .includes(Exception::class.java)
                    .delay(Duration.of(5, ChronoUnit.SECONDS))
                    .multiplier(1.0)
                    .build()
            )
            .build()
    }

    @Bean(name = ["genericCsvJob"])
    fun genericCsvJob(genericStep: Step): Job {
        return JobBuilder("GENERIC_CSV_JOB", jobRepository)
            .start(genericStep)
            .build()
    }
}

class ModernOffsetLogger {

    private val logger = LoggerFactory.getLogger(this::class.java)
    private lateinit var stepExecution: StepExecution

    @BeforeStep
    fun init(stepExecution: StepExecution) {
        this.stepExecution = stepExecution
    }

    @AfterWrite
    fun logProgress(items: Chunk<*>) {
        val limit = items.size()
        val offset = stepExecution.readCount
        logger.info(">> Annotation Log | Offset: $offset | Limit: $limit")
    }

//    @BeforeWrite
//    fun beforeWrite(items: Chunk<*>) {
//        val limit = items.size()
//        val offset = stepExecution.readCount
//
//        logger.info(">> Before Write Log")
//    }
}