package kh.com.im.temporaldemo.config

import org.apache.ibatis.session.SqlSessionFactory
import org.mybatis.spring.batch.MyBatisBatchItemWriter
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.Step
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class GenericBatchConfig(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val sqlSessionFactory: SqlSessionFactory
) {

    @Bean
    @StepScope
    fun genericCsvReader(
        // FIX 1: Add '?' to make it nullable
        @Value("#{jobParameters['filePath']}") filePath: String?
    ): FlatFileItemReader<Map<String, Any>> {

        // FIX 2: Handle the null safely (required because filePath is now String?)
        val path = filePath ?: throw IllegalArgumentException("filePath job parameter is missing")

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

    // Do the same for the writer!
    @Bean
    @StepScope
    fun myBatisWriter(
        // FIX 1: Add '?'
        @Value("#{jobParameters['mybatisId']}") mybatisId: String?
    ): MyBatisBatchItemWriter<Map<String, Any>> {

        // FIX 2: Handle null
        val statementId = mybatisId ?: throw IllegalArgumentException("mybatisId job parameter is missing")

        return MyBatisBatchItemWriterBuilder<Map<String, Any>>()
            .sqlSessionFactory(sqlSessionFactory)
            .statementId(statementId)
            .build()
    }

    @Bean
    fun genericStep(
        reader: FlatFileItemReader<Map<String, Any>>,
        writer: MyBatisBatchItemWriter<Map<String, Any>>
    ): Step {

        return StepBuilder("genericStep", jobRepository)
            .chunk<Map<String, Any>, Map<String, Any>>(1000)
            .reader(reader)
            .writer(writer)
            .transactionManager(transactionManager)
            .build()
    }

    @Bean
    fun genericCsvJob(genericStep: Step): Job {
        return JobBuilder("GENERIC_CSV_JOB", jobRepository)
            .start(genericStep)
            .build()
    }
}