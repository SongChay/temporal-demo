package kh.com.im.temporaldemo.controller


import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.schedules.*
import kh.com.im.temporaldemo.workflow.BatchWorkflow
import kh.com.im.temporaldemo.workflow.UniversalJobWorkflow
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/jobs")
class JobController(
    private val client: WorkflowClient,
    private val scheduleClient: ScheduleClient,
) {

    // UI calls this to create/update a schedule
    @GetMapping("/schedule")
    fun scheduleJob(@RequestParam jobId: String, @RequestParam cron: String) {

        val workflowId = "job-$jobId"

        val schedule = Schedule.newBuilder()
            .setAction(
                ScheduleActionStartWorkflow.newBuilder()
                    .setWorkflowType(UniversalJobWorkflow::class.java)
                    .setArguments(jobId) // Pass ID to workflow
                    .setOptions(
                        WorkflowOptions.newBuilder()
                            .setWorkflowId(workflowId)
                            .setTaskQueue("DATA_JOBS_QUEUE")
                            .build()
                    ).build()
            )
            .setSpec(ScheduleSpec.newBuilder().setCronExpressions(listOf(cron)).build())
            .build()

        try {
            // Create new schedule
            scheduleClient.createSchedule(jobId, schedule, ScheduleOptions.newBuilder().build())
        } catch (_: Exception) {
            // If exists, update it
            scheduleClient.getHandle(jobId).update { _ -> ScheduleUpdate(schedule) }
        }
    }

    @GetMapping("/{jobId}/add-cron")
    fun addCronToJob(@PathVariable jobId: String, @RequestParam newCron: String) {
        scheduleClient.getHandle(jobId).update { input ->
            // 1. Unwrap the schedule
            val currentSchedule = input.description.schedule

            // 2. Get existing crons
            val existingCrons = currentSchedule.spec.cronExpressions.toMutableList()

            // 3. Add the new one (if not exists)
            if (!existingCrons.contains(newCron)) {
                existingCrons.add(newCron)
            }

            // --- FIX PART 1: Copy the existing Spec ---
            // Passing 'currentSchedule.spec' ensures we keep Timezones/Jitter/etc.
            val newSpec = ScheduleSpec.newBuilder(currentSchedule.spec)
                .setCronExpressions(existingCrons)
                .build()

            // --- FIX PART 2: Copy the existing Schedule ---
            // Passing 'currentSchedule' ensures we keep Memos/SearchAttributes/etc.
            ScheduleUpdate(
                Schedule.newBuilder(currentSchedule)
                    .setSpec(newSpec) // Overwrite only the Spec
                    .build()
            )
        }
    }

    @GetMapping("/{jobId}/run")
    fun runJobNow(@PathVariable jobId: String): String {
        val workflowId = "manual-run-$jobId-${System.currentTimeMillis()}"

        val options = WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue("DATA_JOBS_QUEUE")
            .build()

        val workflow = client.newWorkflowStub(UniversalJobWorkflow::class.java, options)

        // FIX: Pass '0' as the second argument (start at offset 0)
        WorkflowClient.start(workflow::executeJob, jobId, 0)

        return "Job triggered. Check Temporal UI for WorkflowID: $workflowId"
    }

    @GetMapping("/{jobId}/pause")
    fun pauseJob(@PathVariable jobId: String) {
        scheduleClient.getHandle(jobId).pause("Paused by User via UI")
    }

    @GetMapping("/{jobId}/unpause")
    fun unpauseJob(@PathVariable jobId: String) {
        scheduleClient.getHandle(jobId).unpause("Resumed by User via UI")
    }

    @GetMapping("/{jobId}/remove-cron")
    fun removeCronFromJob(@PathVariable jobId: String, @RequestParam cronToRemove: String) {
        scheduleClient.getHandle(jobId).update { input ->
            // 1. Unwrap the actual Schedule object from the Input -> Description
            val currentSchedule = input.description.schedule

            // 2. Now you can access .spec (and use .cronExpressions, not List)
            val existingCrons = currentSchedule.spec.cronExpressions.toMutableList()

            existingCrons.remove(cronToRemove)

            // 3. Rebuild the Spec
            val newSpec = ScheduleSpec.newBuilder()
                .setCronExpressions(existingCrons)
                .build()

            // 4. Return the Update
            ScheduleUpdate(
                Schedule.newBuilder(currentSchedule)
                    .setSpec(newSpec)
                    .build()
            )
        }
    }

    @GetMapping("/upload-csv")
    fun uploadCsv(@RequestParam filePath: String, @RequestParam mybatisId: String): String {
        val workflowId = "csv-import-${System.currentTimeMillis()}"

        val workflow = client.newWorkflowStub(
            BatchWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue("BATCH_TASK_QUEUE")
                .build()
        )

        // Start async and return immediately to the user
        WorkflowClient.start(workflow::startImport, filePath, mybatisId)

        return "Workflow Started: $workflowId"
    }
}