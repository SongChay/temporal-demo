package kh.com.im.temporaldemo.workflow

import io.temporal.workflow.Workflow
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import org.apache.ibatis.session.SqlSession
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.time.Duration

class UniversalJobWorkflowImpl : UniversalJobWorkflow {

    // 1. Default Stub: Used ONLY to fetch config (fast, standard retries)
    private val defaultActivities = Workflow.newActivityStub(
        JobActivities::class.java,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .build()
    )

    override fun executeJob(jobId: String, offset: Int) {
        // STEP 1: Fetch the Config from DB
        val config = defaultActivities.getJobConfig(jobId)

        // STEP 2: Build Dynamic RetryOptions based on DB data
        val dynamicRetry = RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(config.initialIntervalSeconds))
            .setMaximumAttempts(config.maxAttempts)
            .setBackoffCoefficient(config.backoffCoefficient)
            .build()

        // STEP 3: Create a NEW Activity Stub with these specific options
        val customActivities = Workflow.newActivityStub(
            JobActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(5)) // Keep timeout standard, or load from DB too
                .setRetryOptions(dynamicRetry) // <--- APPLY DB SETTINGS HERE
                .build()
        )

        // STEP 4: Run the Standard Loop using the 'customActivities' stub
        val batchSize = 1000
        var currentOffset = offset
        var rowsProcessed: Int

        println("Starting Job $jobId with MaxAttempts=${config.maxAttempts}")

        do {
            val params = mapOf("limit" to batchSize, "offset" to currentOffset)

            // Use the CUSTOM stub here
            rowsProcessed = customActivities.executeMyBatis(jobId, params)

            currentOffset += rowsProcessed

            // ... (Continue-As-New logic remains the same) ...

        } while (rowsProcessed == batchSize)
    }
}

data class JobConfig(
    val mybatisId: String = "", // <--- FIX: Added default value here
    val params: Map<String, Any> = emptyMap(),
    val maxAttempts: Int = 5,
    val initialIntervalSeconds: Long = 5,
    val backoffCoefficient: Double = 2.0
)

@Component
class JobActivitiesImpl(
    private val sqlSession: SqlSession,
    private val jdbcTemplate: JdbcTemplate
) : JobActivities {

    override fun getJobConfig(jobId: String): JobConfig {
        val sql = """
            SELECT mybatis_id, retry_max_attempts, retry_initial_interval_seconds, retry_backoff_coef 
            FROM job_definitions WHERE job_id = ?
        """

        return try {
            jdbcTemplate.queryForObject(sql, { rs, _ ->
                JobConfig(
                    mybatisId = rs.getString("mybatis_id"),
                    maxAttempts = rs.getInt("retry_max_attempts"),
                    initialIntervalSeconds = rs.getLong("retry_initial_interval_seconds"),
                    backoffCoefficient = rs.getDouble("retry_backoff_coef")
                )
            }, jobId)!!
        } catch (e: Exception) {
            throw RuntimeException("Job ID '$jobId' not found", e)
        }
    }

    override fun executeMyBatis(jobId: String, params: Map<String, Any>): Int {
        // 1. Get MyBatis ID (Now calls the private function below)
        val config = getJobConfig(jobId)

        // 2. Merge DB params (if any) with Workflow params (limit/offset)
        val allParams = config.params.toMutableMap()
        allParams.putAll(params)

        println("Executing MyBatis: ${config.mybatisId} with params: $allParams")

        // 3. Execute
        // We use 'update' which works for INSERT/UPDATE/DELETE
        return sqlSession.update(config.mybatisId, allParams)
    }
}