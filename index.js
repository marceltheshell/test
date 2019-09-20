var https = require('https');
var zlib = require('zlib');
var crypto = require('crypto');

var endpoint = 'logstash.elastic.aws.autodesk.com';
var port = 6000;

exports.handler = function(input, context) {
    // TODO: Verify DLQ is empty
    // Disable TLS validation (want to remove this for production)
    // TODO: Pin proper CA.
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    // decode input from base64
    var zippedInput = new Buffer(input.awslogs.data, 'base64');

    // decompress the input
    zlib.gunzip(zippedInput, function(error, buffer) {
        if (error) { context.fail(error); return; }

        // parse the input from JSON
        var awslogsData = JSON.parse(buffer.toString('utf8'));

        // transform the input to multiple JSON documents for Logstash
        var requestData = transform(awslogsData);

        // skip control messages
        if (!requestData) {
            console.log('Received a control message');
            context.succeed('Control message handled successfully');
            return;
        }

        // put documents in Logstash
        var results = [];
        function putLogs(element, index, array) {
            put(element, function(error, statusCode, responseBody) {
                console.log('Response: ' + JSON.stringify({
                    "statusCode": statusCode
                }));

                var status = {};

                if (error) {
                    status.error = true;
                    console.log('Error: ' + error);

                } else {
                    status.error = false;
                }

                results.push(status);
            });
        }
        requestData.forEach(putLogs);

        results.forEach(function(element) {
            if (element.error) {
                context.fail("Error posting message(s). See previous logs for more information.");
            }
        });
    });
};

function transform(payload) {
    if (payload.messageType === 'CONTROL_MESSAGE') {
        return null;
    }

    var requestBodies = [];

    payload.logEvents.forEach(function(logEvent) {
        var timestamp = new Date(1 * logEvent.timestamp);

        var source = buildSource(logEvent.message, logEvent.extractedFields);
        source.id = logEvent.id;
        source.timestamp = new Date(1 * logEvent.timestamp).toISOString();
        source.message = logEvent.message;
        source.owner = payload.owner;
        source.log_group = payload.logGroup;
        source.application = payload.logGroup;
        source.log_stream = payload.logStream;
        source.fields = {};
        source.fields.group = process.env.FIELDS_GROUP;
        source.type = process.env.LOG_INDEX;

        requestBodies.push(source);
    });
    return requestBodies;
}

function buildSource(message, extractedFields) {
    if (extractedFields) {
        var source = {};

        for (var key in extractedFields) {
            if (extractedFields.hasOwnProperty(key) && extractedFields[key]) {
                var value = extractedFields[key];

                if (isNumeric(value)) {
                    source[key] = 1 * value;
                    continue;
                }

                jsonSubString = extractJson(value);
                if (jsonSubString !== null) {
                    source['$' + key] = JSON.parse(jsonSubString);
                }

                source[key] = value;
            }
        }
        return source;
    }

    jsonSubString = extractJson(message);
    if (jsonSubString !== null) {
        return JSON.parse(jsonSubString);
    }

    return {};
}

function extractJson(message) {
    var jsonStart = message.indexOf('{');
    if (jsonStart < 0) return null;
    var jsonSubString = message.substring(jsonStart);
    return isValidJson(jsonSubString) ? jsonSubString : null;
}

function isValidJson(message) {
    try {
        JSON.parse(message);
    } catch (e) { return false; }
    return true;
}

function isNumeric(n) {
    return !isNaN(parseFloat(n)) && isFinite(n);
}

function put(data, callback) {
    var requestParams = {
        host: endpoint,
        port: port,
        method: 'PUT',
        path: '/',
        body: JSON.stringify(data),
        headers: {
            'Content-Type': 'application/json',
        },
    };

    var request = https.request(requestParams, function(response) {
        var responseBody = '';
        response.on('data', function(chunk) {
            responseBody += chunk;
        });
        response.on('end', function() {
            console.log("StatusCode " + response.statusCode);

            var error = response.statusCode !== 200;

            callback(error, response.statusCode, responseBody);
        });
    }).on('error', function(e) {
        callback(e);
    });

    //TODO: Pin the certificate
    //request.on('')
    request.end(requestParams.body);
}
