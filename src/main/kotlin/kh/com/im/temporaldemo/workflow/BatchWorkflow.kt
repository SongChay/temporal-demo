package kh.com.im.temporaldemo.workflow

import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod


@WorkflowInterface
interface BatchWorkflow {
    @WorkflowMethod
    fun startImport(filePath: String, mybatisId: String)
}