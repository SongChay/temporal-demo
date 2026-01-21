package kh.com.im.temporaldemo.config

import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.client.schedules.*
import io.temporal.client.WorkflowOptions
import kh.com.im.temporaldemo.workflow.UniversalJobWorkflow
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate

@Configuration
class JobStartupConfig(
    private val jdbcTemplate: JdbcTemplate,
    private val scheduleClient: ScheduleClient
) {

    @Bean
    fun scheduleLoader() = ApplicationRunner {
        println("🚀 Loading Job Schedules from Database...")

        // 1. Fetch all schedules
        // We join to ensure the job is still active
        val sql = """
            SELECT s.job_id, s.cron_expression 
            FROM job_schedules s
            JOIN job_definitions j ON s.job_id = j.job_id
            WHERE j.is_active = true
        """

        // 2. Map raw rows into a list of Pairs
        val allRows = jdbcTemplate.query(sql) { rs, _ ->
            Pair(rs.getString("job_id"), rs.getString("cron_expression"))
        }

        // 3. GROUP BY Job ID
        // Result: { "cbs-sync": ["0 8 * * *", "0 17 * * *"], "log-clean": ["0 0 * * *"] }
        val groupedSchedules = allRows.groupBy(
            { it.first },       // Key: job_id
            { it.second }       // Value: cron_expression
        )

        // 4. Register in Temporal
        groupedSchedules.forEach { (jobId, cronList) ->
            ensureScheduleExists(jobId, cronList) // Pass the LIST, not single string
        }

        println("✅ Loaded schedules for ${groupedSchedules.size} jobs.")
    }

    // Update signature to take a List<String>
    private fun ensureScheduleExists(jobId: String, crons: List<String>) {
        val workflowId = "job-$jobId"

        val schedule = Schedule.newBuilder()
            .setAction(
                ScheduleActionStartWorkflow.newBuilder()
                    .setWorkflowType(UniversalJobWorkflow::class.java)
                    .setArguments(jobId, 0)
                    .setOptions(
                        WorkflowOptions.newBuilder()
                            .setWorkflowId(workflowId)
                            .setTaskQueue("DATA_JOBS_QUEUE")
                            .build()
                    ).build()
            )
            // PASS THE FULL LIST HERE
            .setSpec(ScheduleSpec.newBuilder().setCronExpressions(crons).build())
            .build()

        try {
            scheduleClient.createSchedule(jobId, schedule, ScheduleOptions.newBuilder().build())
            println("   + Scheduled: $jobId (Crons: $crons)")
        } catch (_: ScheduleAlreadyRunningException) {
            // Update existing schedule with the new list
            scheduleClient.getHandle(jobId).update { input ->
                ScheduleUpdate(
                    Schedule.newBuilder(input.description.schedule)
                        .setSpec(ScheduleSpec.newBuilder().setCronExpressions(crons).build())
                        .build()
                )
            }
            println("   * Updated: $jobId (Crons: $crons)")
        } catch (e: Exception) {
            println("   ! Failed to load $jobId: ${e.message}")
        }
    }
}