package kh.com.im.temporaldemo.config

import io.temporal.client.WorkflowClient
import io.temporal.client.schedules.ScheduleClient
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.WorkerFactory
import kh.com.im.temporaldemo.activities.BatchActivitiesImpl
import kh.com.im.temporaldemo.workflow.BatchWorkflowImpl
import kh.com.im.temporaldemo.workflow.JobActivitiesImpl
import kh.com.im.temporaldemo.workflow.UniversalJobWorkflowImpl
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class TemporalConfig {

    private val taskQueue = "DATA_JOBS_QUEUE"

    @Bean
    fun serviceStubs(): WorkflowServiceStubs = WorkflowServiceStubs.newLocalServiceStubs()

    @Bean
    fun workflowClient(stubs: WorkflowServiceStubs): WorkflowClient = WorkflowClient.newInstance(stubs)

    @Bean
    fun scheduleClient(stubs: WorkflowServiceStubs): ScheduleClient = ScheduleClient.newInstance(stubs)

    @Bean
    fun workerFactory(client: WorkflowClient): WorkerFactory = WorkerFactory.newInstance(client)

    // FIX: Use ApplicationRunner instead of @PostConstruct
    @Bean
    fun startWorker(
        workerFactory: WorkerFactory,
        batchActivities: BatchActivitiesImpl, // Our new Spring Batch Bridge
        jobActivities: JobActivitiesImpl      // Your existing SQL Loop activities
    ): ApplicationRunner {
        return ApplicationRunner {

            val batchWorker = workerFactory.newWorker("BATCH_TASK_QUEUE")
            batchWorker.registerWorkflowImplementationTypes(BatchWorkflowImpl::class.java)
            batchWorker.registerActivitiesImplementations(batchActivities)

            // --- WORKER 2: For Standard/General Jobs ---
            val defaultWorker = workerFactory.newWorker("DATA_JOBS_QUEUE")
            defaultWorker.registerWorkflowImplementationTypes(UniversalJobWorkflowImpl::class.java)
            defaultWorker.registerActivitiesImplementations(jobActivities)

            // 3. Start the factory (this starts all workers defined above)
            workerFactory.start()

            println("🚀 Temporal Worker started successfully!")
            println("📍 Task Queue: $taskQueue")
            println("🛠️ Registered Activities: BatchActivities, JobActivities")
        }
    }
}