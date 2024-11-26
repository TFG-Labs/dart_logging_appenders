import 'dart:async';
import 'dart:collection';

import 'package:dio/dio.dart';
import 'package:intl/intl.dart';
import 'package:logging/logging.dart';
import 'package:logging_appenders/src/base_appender.dart';
import 'package:logging_appenders/src/internal/dummy_logger.dart';
import 'package:logging_appenders/src/logrecord_formatter.dart';
import 'package:meta/meta.dart';

final _logger = DummyLogger('logging_appenders.base_remote_appender');

/// Base appender for services which should buffer log messages before
/// handling them. (eg. because they use network traffic which make it
/// unfeasible to send every log line on it's own).
///
abstract class BaseLogSender extends BaseLogAppender {
  BaseLogSender({
    LogRecordFormatter? formatter,
    int? bufferSize,
  })  : bufferSize = bufferSize ?? 500,
        super(formatter);

  Map<String, String> _userProperties = {};

  /// Maximum number of log entries to buffer before triggering sending
  /// of log entries. (default: 500)
  final int bufferSize;

  List<LogEntry> _logEvents = <LogEntry>[];
  Timer? _timer;

  final SimpleJobQueue _sendQueue = SimpleJobQueue();

  set userProperties(Map<String, String> userProperties) {
    _userProperties = userProperties;
  }

  Future<void> log(Level logLevel, DateTime time, String line,
      Map<String, String> lineLabels) {
    return _logEvent(LogEntry(
      logLevel: logLevel,
      ts: time,
      line: line,
      lineLabels: lineLabels,
    ));
  }

  Future<void> _logEvent(LogEntry log) {
    _timer?.cancel();
    _timer = null;
    _logEvents.add(log);
    if (_logEvents.length > bufferSize) {
      _triggerSendLogEvents();
    } else {
      _timer = Timer(const Duration(seconds: 10), () {
        _timer = null;
        _triggerSendLogEvents();
      });
    }
    return Future.value(null);
  }

  @protected
  Stream<void> sendLogEvents(
      List<LogEntry> logEntries, Map<String, String> userProperties);

  Future<void> _triggerSendLogEvents() => Future(() {
        final entries = _logEvents;
        _logEvents = [];
        _sendQueue.add(SimpleJobDef(
          runner: (job) => sendLogEvents(entries, _userProperties),
        ));
        return _sendQueue.triggerJobRuns().then((val) {
          _logger.finest('Sent log jobs: $val');
          return null;
        });
      });

  @override
  void handle(LogRecord record) {
    final message = formatter.format(record);
    final lineLabels = {
      'lvl': record.level.name,
      'logger': record.loggerName,
    };
    if (record.error != null) {
      lineLabels['e'] = record.error.toString();
      lineLabels['eType'] = record.error.runtimeType.toString();
    }
    log(record.level, record.time, message, lineLabels);
  }

  Future<void> flush() => _triggerSendLogEvents();

  @override
  Future<void> dispose() async {
    try {
      await flush();
    } finally {
      await super.dispose();
    }
  }
}

/// Helper base class to handle Dio errors during network requests.
abstract class BaseDioLogSender extends BaseLogSender {
  BaseDioLogSender({
    super.formatter,
    super.bufferSize,
  });

  Future<void> sendLogEventsWithDio(List<LogEntry> entries,
      Map<String, String> userProperties, CancelToken cancelToken);

  @override
  Stream<void> sendLogEvents(
      List<LogEntry> logEntries, Map<String, String> userProperties) {
    final cancelToken = CancelToken();
    final streamController = StreamController<void>(onCancel: () {
      cancelToken.cancel();
    });
    streamController.onListen = () {
      sendLogEventsWithDio(logEntries, userProperties, cancelToken).then((val) {
        if (!streamController.isClosed) {
          streamController.add(null);
          streamController.close();
        }
      }).catchError((dynamic err, StackTrace stackTrace) {
        var message = err.runtimeType.toString();
        if (err is DioException) {
          if (err.response != null) {
            message = 'response:${err.response!.data}';
          }
          _logger.warning(
              'Error while sending logs. $message', err, stackTrace);
          if (!streamController.isClosed) {
            streamController.addError(err, stackTrace);
            streamController.close();
          }
        }
      });
    };
    return streamController.stream;
  }
}

class LogEntry {
  LogEntry({
    required this.logLevel,
    required this.ts,
    required this.line,
    required this.lineLabels,
  });

  static final DateFormat _dateFormat =
      DateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  final Level logLevel;
  final DateTime ts;
  final String line;
  final Map<String, String> lineLabels;

  String get tsFormatted => _dateFormat.format(ts.toUtc());
}

typedef SimpleJobRunner = Stream<void> Function(SimpleJobDef job);

class SimpleJobDef {
  SimpleJobDef({required this.runner});

  final SimpleJobRunner runner;

  int errorCount = 0;
  DateTime? lastError;
}

class SimpleJobQueue {
  SimpleJobQueue({
    this.baseExpBackoffSeconds = 10,
    this.maxRetries = 3,
    Future<void> Function(Duration)? delayFunction,
  }) : _delayFunction = delayFunction ?? Future.delayed;

  final int baseExpBackoffSeconds;
  final int maxRetries;

  final Future<void> Function(Duration) _delayFunction;

  final Queue<SimpleJobDef> _queue = Queue<SimpleJobDef>();

  @visibleForTesting
  Queue<SimpleJobDef> get queue => _queue;

  @visibleForTesting
  int get length => _queue.length;

  void add(SimpleJobDef job) {
    _queue.addLast(job);
  }

  Future<int> triggerJobRuns() async {
    _logger.finest('Triggering Job Runs. ${_queue.length}');
    var successfulJobs = 0;

    // Make a copy of the queue to avoid modification during iteration
    final jobsToProcess = List<SimpleJobDef>.from(_queue);

    for (final job in jobsToProcess) {
      // Implement per-job backoff strategy before processing
      if (job.errorCount > 0) {
        final backoffDuration = Duration(
            seconds:
                baseExpBackoffSeconds * (job.errorCount * job.errorCount + 1));

        final timeSinceLastError =
            DateTime.now().difference(job.lastError ?? DateTime.now());
        if (timeSinceLastError < backoffDuration) {
          final waitDuration = backoffDuration - timeSinceLastError;
          _logger.finest(
              'Job is backing off for ${waitDuration.inSeconds} seconds before retrying.');
          await _delayFunction(waitDuration);
        }
      }

      try {
        // Execute the job
        await job.runner(job).drain(null);

        // If successful, remove the job from the queue
        _queue.remove(job);
        successfulJobs++;
        _logger.finest(
            'Success job. Remaining: ${_queue.length}, Completed: $successfulJobs');

        // Reset job's error count and last error time
        job.errorCount = 0;
        job.lastError = null;
      } catch (error, stackTrace) {
        // Handle error per job
        _logger.warning('Error while executing job', error, stackTrace);

        bool shouldRetry = false;

        // Decide whether to retry based on error
        if (error is DioException) {
          if (error.type == DioExceptionType.connectionError ||
              error.type == DioExceptionType.sendTimeout ||
              error.type == DioExceptionType.receiveTimeout ||
              error.type == DioExceptionType.cancel) {
            // Network error, we can retry
            shouldRetry = true;
          } else if (error.type == DioExceptionType.badResponse) {
            // HTTP error, check status code
            final statusCode = error.response?.statusCode;
            if (statusCode != null && statusCode <= 500) {
              // Bad request or server error, do not retry
              shouldRetry = false;
            } else {
              // Retry on errors > 500
              shouldRetry = true;
            }
          }
        }

        if (!shouldRetry) {
          // Remove the job from the queue, since we don't want to retry
          _queue.remove(job);
          _logger.warning(
              'Removing job from queue due to non-retryable error (HTTP ${error is DioException ? error.response?.statusCode : 'unknown'}).');
        } else {
          // Update job's error count and last error time
          job.errorCount++;
          job.lastError = DateTime.now();

          // Check if job has reached max retries
          if (job.errorCount >= maxRetries) {
            _logger.warning(
                'Job has reached maximum retries ($maxRetries). Removing from queue.');
            _queue.remove(job);
          } else {
            _logger.info(
                'Keeping job in queue for retry. Error count: ${job.errorCount}');
          }
        }
      }
    }

    return successfulJobs;
  }
}
