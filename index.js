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
        console.log('awslogsData', awslogsData);
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

        requestData.forEach(putLogs); //this is the fuction where RESULTS gets populated
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
    
    let payloadEvents = payload.logEvents.filter((event) => console.log(event.message));

    // payloadEvents.forEach(function(logEvent) {
    
    console.log('payloadEvents', JSON.stringify(payloadEvents));
    payload.logEvents.forEach(function(logEvent) {
        console.log(`Event in --- ${logEvent.message}` ); 
       
       

        let messageStr = filterLogEvents(logEvent.message);
        
        if(messageStr != ''){    
            let source = buildSourceSPOC(messageStr);
            console.log('src' + JSON.stringify(source));
            
            if (typeof source !== 'object') {
                const temp = source;
                source = {};
                source.bangalore = temp;
            }
            
            source.id = logEvent.id;
            source.timestamp = new Date(1 * logEvent.timestamp).toISOString();
            source.message = logEvent.message;
            source.owner = payload.owner;
            source.log_group = payload.logGroup;
            source.application = normaliseApplicationNames(payload.logGroup);
            source.log_stream = payload.logStream;
            source.fields = {};
            source.fields.group = process.env.FIELDS_GROUP;
            source.type = process.env.LOG_INDEX;

            console.log('ELK --- Message ----' + JSON.stringify(source));    
            requestBodies.push(source);
        }
    });
   
    return requestBodies;
}

function filterLogEvents(logevent){
    let logStmt = '';

    if(logevent.startsWith('info')){
        logStmt = JSON.stringify(makeJSON(logevent.replace('info: ','')));
    }else if(logevent.startsWith('debug')){
         logStmt = JSON.stringify(makeJSON(logevent.replace('debug: ','')));
    } else  if(logevent.startsWith(' error')){
        logStmt =  JSON.stringify(makeJSON(logevent.replace(' error: ','')));
    } else if(isValidJson(logevent)){
        logStmt = logevent;
    }
    
   
    return logStmt;
}

function buildSourceSPOC(message){
        let loggerStmt = JSON.parse(message);
        normaliseResponseBody(loggerStmt);
        normaliseRequestBody(loggerStmt);
        normaliseErrorResponse(loggerStmt);
    return loggerStmt;
}

function normaliseRequestBody(loggerStmt) {
     if (loggerStmt.request_body && isEmptyObject(loggerStmt.request_body)) {
        let request_body = loggerStmt.response_body;
        loggerStmt.request_body = stringifyJSON(request_body);
    } else if (loggerStmt.request_body && isObject(loggerStmt.request_body)) {
        let request_body = loggerStmt.response_body;
        loggerStmt.request_body = stringifyJSON(request_body);
    }


}

function normaliseResponseBody(loggerStmt) {

    if (loggerStmt.response_body && isValidJson(loggerStmt.response_body)) {
        let response_body = loggerStmt.response_body;
        loggerStmt.response_body = stringifyJSON(response_body);

    } else if (loggerStmt.response_body && isObject(loggerStmt.response_body)) {
        let response_body = loggerStmt.response_body;
        loggerStmt.response_body = stringifyJSON(response_body);
    }

}

function normaliseErrorResponse(loggerStmt){
       if (loggerStmt.errormessage && (isEmptyObject(loggerStmt.errormessage) || isObject(loggerStmt.errormessage))) {
        let errormessage = loggerStmt.errormessage;
        loggerStmt.errormessage = stringifyJSON(errormessage);
     }
}



function isValidJson(message) {
    try {
        JSON.parse(message);
    } catch (e) { return false; }
    return true;
}

function isEmptyObject(obj) {
    return JSON.stringify(obj) === '{}';
}

function isObject(obj) {
    return obj instanceof Object
}

function stringifyJSON(data) {

	let isString = typeof data === 'string';
	if(isString){
		return data;
	} else {
	return JSON.stringify(data);
    }
}


function normaliseApplicationNames(logGroupName){
    let formattedName = logGroupName;
    if(logGroupName.includes('APIGateway')){
        formattedName = 'APIGateway';
    }else if(logGroupName.includes('Payport')){
         formattedName = 'Payport';
    }else if(logGroupName.includes('PayportAgent')){
        formattedName = 'PayportAgent';
    }else if(logGroupName.includes('amart')){
        formattedName = 'amart';
    }
    
    return formattedName;
}

function makeJSON(logevent){
    let formattedStr = collapseArr(logevent)
    let messageArr = formattedStr.split(', ');
    return  messageArr.reduce((json, value ) => {
		if(value){
			let splitvalue = value.split('=')
		 json[splitvalue[0]] = splitvalue[1]; 
		}		
	 return json; 
	}, 
	{});
	
    
}

function collapseArr(logevent){
let openbraces = 0;
let start = false;
let innerstr = '';
let rootStr = '';
let lengthof = logevent.length;

for(let i=0;i<lengthof;i++){
	if(logevent.charAt(i) === '[' && logevent.charAt(i-1) === '=' ){
		openbraces++;
		start = true;
		innerstr+=logevent.charAt(i);
	}else if(start){
		innerstr+=logevent.charAt(i);
	} else {
		rootStr+=logevent.charAt(i);
	}

	if(start && logevent.charAt(i) === ']'){
		--openbraces;
		if(openbraces === 0 ){
			innerstr = innerstr.split(',').join('|');
			innerstr = innerstr.split('=').join('|');
			rootStr+=innerstr;
			innerstr = '';
			start = false;
		}
	}

}

let xmlstartIndex = rootStr.indexOf('<?');
let xmllastIndex = rootStr.lastIndexOf('>,');
let xmlvalue = rootStr.slice(xmlstartIndex,xmllastIndex);
xmlvalue = xmlvalue.split(',').join('|');
xmlvalue = xmlvalue.split('=').join('|');

rootStr = rootStr.slice(0,xmlstartIndex)+ xmlvalue  + rootStr.slice(xmllastIndex);
return rootStr;

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
