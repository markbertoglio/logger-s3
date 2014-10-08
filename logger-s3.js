var knox = require('knox');

module.exports = {
  init: init,
  writeLog: writeLog
};

var awsPath;
var maxEntries;
var logQueue = [];
var writing = false;
var fileSuffix = '_' + (Math.floor(Math.random() * 1000000000)).toString(36) + '.json';
var client;

function init(config) {
  var knoxConfig = {
    key: config && config.awsKey || process.env.AWS_ACCESS_KEY, 
    secret: config && config.awsSecret || process.env.AWS_ACCESS_SECRET, 
    bucket: config && config.awsBucket || process.env.AWS_LOG_BUCKET, 
    region: config && config.awsRegion || process.env.AWS_LOG_REGION
  };
  maxEntries = config && config.maxEntriesPerFile || 500;
  awsPath = config && config.awsPath || process.env.AWS_LOG_PATH;
  if (awsPath[awsPath.length-1] != '/') awsPath += '/';
  client = knox.createClient(knoxConfig);
  setInterval(flushQueue, config.flushTime || 10000, true);
  return writeLog;
}

function writeLog(logData, done) {
  var now = Date.now();
  var timeDiff = (logData.date || now) - now;
  (logData.logs || []).forEach(function(logEntry) {
    logEntry.date = (logEntry.date || now) - timeDiff;
    logQueue.push(logEntry);
  });
  flushQueue(false);
  return done && done();

  function flushQueue(force) {
    if ((logQueue.length > maxEntries || force) && !writing && logQueue.length) {
      writing = true;
      return writeQueue(function(err) {
        writing = false;
      });
    }

    function writeQueue(queue, done) {
      var file = awsPath + Date.now() + fileSuffix;
      var data = JSON.stringify(logQueue);
      logQueue = [];
      var headers = {
        'Content-Length': data.length,
        'Content-Type': 'application/json',
      };
      var req = client.put(file, headers);
      req.on('response', function(res) {
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          // console.error(chunk);
        });
        if (res.statusCode != 200) console.error("Failed to write to AWS, status", res.statusCode);
        done();
      });
      req.on('error', function(err) {
        done(err);
      });
      req.end(data);
    }
  }
}


