package kh.com.im.temporaldemo.workflow

import io.temporal.activity.ActivityInterface
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

// 1. The Workflow: The Manager
@WorkflowInterface
interface UniversalJobWorkflow {
    @WorkflowMethod
    fun executeJob(jobId: String, offset: Int)
}

// 2. The Activity: The Worker Tool
@ActivityInterface
interface JobActivities {
    // 1. Light method to get settings
    fun getJobConfig(jobId: String): JobConfig

    // 2. Heavy method to run SQL
    fun executeMyBatis(jobId: String, params: Map<String, Any>): Int
}