package kh.com.im.temporaldemo.activities

import org.apache.ibatis.session.SqlSession
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.stereotype.Component


//val baseParams = JobParametersBuilder()
//    .addString("batchDate", date)
//    .toJobParameters()
//
//// Use the incrementer to add a unique 'run.id'
//val params = JobParametersIncrementer().getNext(baseParams)
//
//// Now Spring Batch sees this as a new 'Execution' of the 'Instance'
//val execution = jobLauncher.run(cbsExtractJob, params)
//
//if (execution.status != BatchStatus.COMPLETED) {
//    throw RuntimeException("Batch failed: ${execution.status}")
//}
//
//return "Success"

@Component
class BatchActivitiesImpl(
    private val jobOperator: JobOperator,
    private val jobRepository: JobRepository,  // Used to find the failed execution
    private val batchJob: Job,
    private val sqlSession: SqlSession,
) : BatchActivities {

    override fun runSpringBatchJob(filePath: String, mybatisId: String): String {
        val params = JobParametersBuilder()
            .addString("filePath", filePath)
            .addString("mybatisId", mybatisId)
            .toJobParameters()

        // 1. Check if this Job Instance already exists and failed
        val lastExecution = jobRepository.getLastJobExecution("GENERIC_CSV_JOB", params)

        val execution = if (lastExecution != null && lastExecution.status.isUnsuccessful) {
            // RESUME: This tells Batch to look at the DB and start from the last successful commit (Record 101)
            jobOperator.restart(lastExecution)
        } else {
            // START: Fresh run
            jobOperator.start(batchJob, params)
        }

        while (execution.isRunning) {
            Thread.sleep(500)
        }

        // 2. Wait/Check for completion (Operator.start/restart is asynchronous)
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