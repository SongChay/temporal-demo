package kh.com.im.temporaldemo.workflow

import io.temporal.workflow.Workflow
import kh.com.im.temporaldemo.activities.BatchActivities
import java.time.Duration

class BatchWorkflowImpl  : BatchWorkflow {
    private val activities = Workflow.newActivityStub(
        BatchActivities::class.java,
        io.temporal.common.RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(5))
            .setMaximumAttempts(3)
            .build()
            .let {
                io.temporal.activity.ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofHours(2))
                    .setRetryOptions(it)
                    .build()
            }
    )

    override fun startImport(filePath: String, mybatisId: String) {

        activities.deleteBatchJobData()
        activities.runSpringBatchJob(filePath, mybatisId)





    }
}