package kh.com.im.temporaldemo.activities


import io.temporal.activity.ActivityInterface

@ActivityInterface
interface BatchActivities {
    fun runSpringBatchJob(filePath: String, mybatisId: String): String
    fun deleteBatchJobData(): String
}
