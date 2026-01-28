package kh.com.im.temporaldemo.config

import org.apache.ibatis.session.SqlSessionFactory
import org.mybatis.spring.batch.MyBatisBatchItemWriter
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder
import org.slf4j.LoggerFactory
import org.springframework.batch.core.annotation.AfterStep
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
import org.springframework.transaction.PlatformTransactionManager

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
        @Value("#{jobParameters['filePath']}") path: String?
    ): FlatFileItemReader<Map<String, Any>> {
        val filePath = path ?: throw IllegalArgumentException("filePath is missing")

        return FlatFileItemReaderBuilder<Map<String, Any>>()
            .name("genericReader")
            .resource(FileSystemResource(filePath))
            // DO NOT use linesToSkip(1) if you want to use the first line as headers
            .linesToSkip(1)
            .lineMapper(DefaultLineMapper<Map<String, Any>>().apply {
                setLineTokenizer(DelimitedLineTokenizer().apply {
                    // If you leave names empty here, you must map by index in the FieldSetMapper
                })
                setFieldSetMapper { fieldSet ->
                    val row = mutableMapOf<String, Any>()
                    // Map dynamically by index to ensure NO records are skipped as "headers"
                    for (i in 0 until fieldSet.fieldCount) {
                        val value = fieldSet.readString(i)
                        // You can use a generic key or lookup a header map if you loaded one
                        row["col${i + 1}"] = value ?: ""
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
            .skip(Exception::class.java)
            .skipLimit(Long.MAX_VALUE)
//            .retryLimit(3)
//            .retry(Exception::class.java)
//            .retryPolicy(
//                RetryPolicy.builder()
//                    .includes(Exception::class.java)
//                    .delay(Duration.of(5, ChronoUnit.SECONDS))
//                    .multiplier(1.0)
//                    .build()
//            )
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

    @AfterStep
    fun afterRead(stepExecution: StepExecution) {
        val readCount = stepExecution.readCount
        val writeCount = stepExecution.writeCount
        val readSkips = stepExecution.readSkipCount
        val processSkips = stepExecution.processSkipCount
        val writeSkips = stepExecution.writeSkipCount

        val totalProcessed = readCount + readSkips // Total records touched

        logger.info(
            """
            |
            |=== STEP SUMMARY: ${stepExecution.stepName} ===
            |Total Records Read:    $readCount
            |Total Records Written: $writeCount
            |Read Skips:            $readSkips
            |Process Skips:         $processSkips
            |Write Skips:           $writeSkips
            |-------------------------------------------
            |Total Count Verified:  $totalProcessed
            |===========================================
        """.trimMargin()
        )
    }


//    @BeforeWrite
//    fun beforeWrite(items: Chunk<*>) {
//        val limit = items.size()
//        val offset = stepExecution.readCount
//
//        logger.info(">> Before Write Log")
//    }
}