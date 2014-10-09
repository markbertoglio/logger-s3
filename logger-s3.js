var knox = require('knox');

module.exports = {
  init: init,
  writeLog: writeLog
};

var awsPath;
var maxEntries;
var publicLogs = false;
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
  publicLogs = config && config.publicLogs || false;
  client = knox.createClient(knoxConfig);
}

function writeLog(logs, done) {
  var file = awsPath + Date.now() + fileSuffix;
  var data = JSON.stringify(logs);
  var headers = {
    'Content-Length': data.length,
    'Content-Type': 'application/json',
  };
  if (publicLogs) headers['x-amz-acl'] = 'public-read';
  var req = client.put(file, headers);
  req.on('response', function(res) {
    res.setEncoding('utf8');
    res.on('data', function(chunk) {
      // console.error(chunk);
    });
    var err = null;
    if (res.statusCode != 200) err = "Failed to write to AWS, status" + res.statusCode;
    done(err);
  });
  req.on('error', function(err) {
    done(err);
  });
  req.end(data);
}
