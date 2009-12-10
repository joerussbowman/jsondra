/* 
 * jsondra.js - library for interacting with jsondra node.js
 *
 * requires: node.js
 *
 * copyright 2009 Joseph Bowman
 */

var sys = require("sys"),
    http = require("http");

// If the application has already configured the host and port use it, otherwise
// defer to the settings in this file.
if (GLOBAL.jsondra_connection == undefined) {
    GLOBAL.jsondra_connection = {"host": "localhost", "port": 8001};
}

var get = function(key, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    uri = "/" + key["keyspace"] + "/" + key["columnfamily"] + "/"
    if (key["key"]) {
        uri += key["key"] + "/"
    }
    headers = {
        "Content-Length": 0
    }
    var request = jsondra.get(uri, headers);
    request.finish(function(response){
        response.setBodyEncoding("utf8");
        var responseBody = "";
        response.addListener("body", function (chunk) {
            responseBody += chunk;
        });
        response.addListener("complete", function() {
            if (response.statusCode == 200) {
            }
            else if (response.statusCode == 404) {
                responseBody = JSON.stringify(null)
            }
            else {
                responseBody = JSON.stringify({"error": "Invalid Jsondra status code: " + response.statusCode})
            }
            callback(responseBody);
        });
    });
};

var del = function(key, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    uri = "/" + key["keyspace"] + "/" + key["columnfamily"] + "/"
    if (key["key"]) {
        uri += key["key"] + "/"
    }
    headers = {
        "Content-Length": 0
    }
    var request = jsondra.del(uri, headers);
    request.finish(function(response){
        response.setBodyEncoding("utf8");
        var responseBody = "";
        response.addListener("body", function (chunk) {
            responseBody += chunk;
        });
        response.addListener("complete", function() {
            if (response.statusCode == 200) {
            }
            else if (response.statusCode == 404) {
                responseBody = JSON.stringify(false)
            }
            else {
                responseBody = JSON.stringify({"error": "Invalid Jsondra status code: " + response.statusCode})
            }
            callback(responseBody);
        });
    });
};

var _add_record = function(key, value, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    var val = JSON.stringify(value)
    uri = "/" + key["keyspace"] + "/" + key["columnfamily"] + "/"
    if (key["key"]) {
        uri += key["key"] + "/"
    }
    data = "v=" + escape(val);
    headers = {
        "Content-Length": data.length,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    var request = jsondra.post(uri, headers);
    request.sendBody(data);
    request.finish(function(response){
        response.setBodyEncoding("utf8");
        var responseBody = "";
        response.addListener("body", function (chunk) {
            responseBody += chunk;
        });
        response.addListener("complete", function() {
            if (response.statusCode == 200) {
            }
            else {
                responseBody = JSON.stringify({"error": "Invalid Jsondra status code: " + response.statusCode})
            }
            callback(responseBody);
        });
    });
};

exports.get = get
exports.del = del
exports.post = _add_record
exports.put = _add_record
