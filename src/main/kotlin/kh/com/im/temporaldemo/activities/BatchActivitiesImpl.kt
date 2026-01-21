package kh.com.im.temporaldemo.activities

import org.apache.ibatis.session.SqlSession
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.batch.core.launch.JobOperator
import org.springframework.stereotype.Component


@Component
class BatchActivitiesImpl(
    private val jobOperator: JobOperator,
    private val batchJob: Job,
    private val sqlSession: SqlSession
) : BatchActivities {

    override fun runSpringBatchJob(filePath: String, mybatisId: String): String {
        val params = JobParametersBuilder()
            .addString("filePath", filePath)
            .addString("mybatisId", mybatisId)
            .addLong("timestamp", System.currentTimeMillis())
            .toJobParameters()

        val execution = jobOperator.start(batchJob, params)

        if (execution.status.isUnsuccessful) {
            throw RuntimeException("Batch Job Failed: ${execution.exitStatus.exitDescription}")
        }
        return "SUCCESS"
    }

    override fun deleteBatchJobData(): String {
        sqlSession.delete("TestJobs.deleteBatchCopy")

        return "SUCCESS"
    }
}