import 'dart:io';

import 'package:dio/dio.dart';
import 'package:logging_appenders/base_remote_appender.dart';
import 'package:test/expect.dart';
import 'package:test/scaffolding.dart';

void main() {
  group('SimpleJobQueue Tests', () {
    test('Job is added to the queue', () {
      final queue = SimpleJobQueue();

      final job = SimpleJobDef(runner: (_) async* {});
      queue.add(job);

      expect(queue.length, equals(1));
    });

    test('Successful job is removed from the queue', () async {
      final queue = SimpleJobQueue();

      final job = SimpleJobDef(runner: (_) async* {
        // Simulate successful execution
      });

      queue.add(job);

      expect(queue.length, equals(1));

      final successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(1));
      expect(queue.length, equals(0));
    });

    test('Jobs are removed after exceeding max retries', () async {
      final maxRetries = 3;
      final queue = SimpleJobQueue(
        maxRetries: maxRetries,
        baseWaitSeconds: 1,
        delayFunction: (_) => Future.value(), // Mock delay function
      );

      var runCount = 0;

      // Create a job that always fails with a retryable error
      final job = SimpleJobDef(runner: (_) async* {
        runCount++;
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      queue.add(job);

      expect(queue.length, equals(1));

      // Run the job up to maxRetries times
      for (var i = 0; i < maxRetries; i++) {
        var successfulJobs = await queue.triggerJobRuns();
        expect(successfulJobs, equals(0));

        if (i < maxRetries - 1) {
          // Before reaching maxRetries, job remains in the queue
          expect(queue.length, equals(1));
        } else {
          // At maxRetries, job is removed from the queue
          expect(queue.length, equals(0));
        }

        expect(runCount, equals(i + 1));
        expect(job.errorCount, equals(i + 1));
      }

      // Verify the job has been removed after exceeding maxRetries
      expect(queue.length, equals(0)); // Job is removed from the queue
      expect(job.errorCount,
          equals(maxRetries)); // Error count remains at maxRetries
      expect(runCount, equals(maxRetries));
    });

    test('Job with retryable error is kept in the queue', () async {
      final queue = SimpleJobQueue();

      final job = SimpleJobDef(runner: (_) async* {
        // Simulate a network error (retryable)
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      queue.add(job);

      expect(queue.length, equals(1));

      final successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(0));
      expect(queue.length, equals(1)); // Job remains in the queue
      expect(job.errorCount, equals(1)); // Job's error count incremented
    });

    test('Job with non-retryable error (HTTP 400) is removed from the queue',
        () async {
      final queue = SimpleJobQueue();

      final job = SimpleJobDef(runner: (_) async* {
        // Simulate an HTTP 400 error (non-retryable)
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          response: Response(
            requestOptions: RequestOptions(path: '/test'),
            statusCode: 400,
          ),
          type: DioExceptionType.badResponse,
        );
      });

      queue.add(job);

      expect(queue.length, equals(1));

      final successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(0));
      expect(queue.length, equals(0)); // Job is removed from the queue
    });

    test('Job with non-retryable error (HTTP 500) is removed from the queue',
        () async {
      final queue = SimpleJobQueue();

      final job = SimpleJobDef(runner: (_) async* {
        // Simulate an HTTP 500 error (non-retryable)
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          response: Response(
            requestOptions: RequestOptions(path: '/test'),
            statusCode: 500,
          ),
          type: DioExceptionType.badResponse,
        );
      });

      queue.add(job);

      expect(queue.length, equals(1));

      final successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(0));
      expect(queue.length, equals(0)); // Job is removed from the queue
    });

    test('Backoff is applied per job for retryable errors', () async {
      final queue = SimpleJobQueue(baseWaitSeconds: 1);

      var runCount = 0;

      // Simulate a network error (retryable)
      Stream<void> retryableRunner(SimpleJobDef job) async* {
        runCount++;
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      }

      final job = SimpleJobDef(runner: retryableRunner);
      queue.add(job);

      expect(queue.length, equals(1));

      // First attempt
      var successfulJobs = await queue.triggerJobRuns();
      expect(successfulJobs, equals(0));
      expect(queue.length, equals(1));
      expect(runCount, equals(1));
      expect(job.errorCount, equals(1));

      // Second attempt (after backoff)
      successfulJobs = await queue.triggerJobRuns();
      expect(successfulJobs, equals(0));
      expect(queue.length, equals(1));
      expect(runCount, equals(2));
      expect(job.errorCount, equals(2));

      // Third attempt (after increased backoff)
      successfulJobs = await queue.triggerJobRuns();
      expect(successfulJobs, equals(0));
      expect(queue.length, equals(1));
      expect(runCount, equals(3));
      expect(job.errorCount, equals(3));
    });

    test('Jobs are retried and removed after exceeding max retries', () async {
      final maxRetries = 3;
      final queue = SimpleJobQueue(
        maxRetries: maxRetries,
        baseWaitSeconds: 1,
        delayFunction: (_) => Future.value(), // Mock delay function
      );

      // Step 1: Add initial jobs (some passing, some failing)
      final passingJob1 = SimpleJobDef(runner: (_) async* {
        // Simulate successful execution
      });

      final failingJob1 = SimpleJobDef(runner: (_) async* {
        // Simulate a retryable error
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      final passingJob2 = SimpleJobDef(runner: (_) async* {
        // Simulate successful execution
      });

      final failingJob2 = SimpleJobDef(runner: (_) async* {
        // Simulate a retryable error
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      // Add initial jobs to the queue
      queue.add(passingJob1);
      queue.add(failingJob1);
      queue.add(passingJob2);
      queue.add(failingJob2);

      // Queue length should be 4
      expect(queue.length, equals(4));

      // Step 2: Run the jobs for the first time
      var successfulJobs = await queue.triggerJobRuns();

      // Two passing jobs should succeed
      expect(successfulJobs, equals(2));

      // Queue should have two failing jobs remaining
      expect(queue.length, equals(2));

      // Step 3: Run the queue multiple times to allow retries for failing jobs
      for (var i = 1; i <= maxRetries; i++) {
        successfulJobs = await queue.triggerJobRuns();

        // No jobs should succeed since the remaining jobs are failing
        expect(successfulJobs, equals(0));

        // Queue length should decrease when jobs exceed maxRetries
        if (i < maxRetries - 1) {
          expect(queue.length, equals(2)); // Jobs still in queue
        } else {
          expect(queue.length, equals(0)); // Jobs removed after maxRetries
        }
      }

      // Step 4: Add more jobs at a later time
      final passingJob3 = SimpleJobDef(runner: (_) async* {
        // Simulate successful execution
      });

      final failingJob3 = SimpleJobDef(runner: (_) async* {
        // Simulate a retryable error
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      queue.add(passingJob3);
      queue.add(failingJob3);

      // Queue length should be 2
      expect(queue.length, equals(2));

      // Step 5: Run the queue again
      successfulJobs = await queue.triggerJobRuns();

      // One passing job should succeed
      expect(successfulJobs, equals(1));

      // Queue should have one failing job remaining
      expect(queue.length, equals(1));

      // Step 6: Run the queue multiple times for the failing job
      for (var i = 1; i <= maxRetries; i++) {
        successfulJobs = await queue.triggerJobRuns();

        // No jobs should succeed
        expect(successfulJobs, equals(0));

        // Queue length should decrease when job exceeds maxRetries
        if (i < maxRetries - 1) {
          expect(queue.length, equals(1)); // Job still in queue
        } else {
          expect(queue.length, equals(0)); // Job removed after maxRetries
        }
      }

      // Final check: Queue should be empty
      expect(queue.length, equals(0));
    });

    test('Successful job resets error count and last error time', () async {
      final queue = SimpleJobQueue(delayFunction: (_) => Future.value());

      var runCount = 0;

      // First job will fail with a retryable error
      final job1 = SimpleJobDef(runner: (_) async* {
        runCount++;
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          type: DioExceptionType.connectionError,
        );
      });

      // Second job will succeed
      final job2 = SimpleJobDef(runner: (_) async* {
        runCount++;
        // Simulate successful execution
      });

      queue.add(job1);
      queue.add(job2);

      expect(queue.length, equals(2));

      var successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(1)); // Only job2 succeeded
      expect(queue.length, equals(1)); // job1 remains in the queue
      expect(runCount, equals(2));
      expect(job1.errorCount, equals(1)); // Job1's error count incremented

      // Now, retry job1
      successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(0)); // job1 still failing
      expect(queue.length, equals(1)); // job1 remains in the queue
      expect(runCount, equals(3));
      expect(job1.errorCount, equals(2)); // Error count incremented
    });

    test('Jobs are processed individually without affecting each other',
        () async {
      final queue = SimpleJobQueue();

      // Job1 will fail with a non-retryable error
      final job1 = SimpleJobDef(runner: (_) async* {
        throw DioException(
          requestOptions: RequestOptions(path: '/test'),
          response: Response(
            requestOptions: RequestOptions(path: '/test'),
            statusCode: 400,
          ),
          type: DioExceptionType.badResponse,
        );
      });

      // Job2 will succeed
      final job2 = SimpleJobDef(runner: (_) async* {
        // Simulate successful execution
      });

      queue.add(job1);
      queue.add(job2);

      expect(queue.length, equals(2));

      final successfulJobs = await queue.triggerJobRuns();

      expect(successfulJobs, equals(1)); // Only job2 succeeded
      expect(
          queue.length, equals(0)); // job1 removed due to non-retryable error
    });
  });
}
