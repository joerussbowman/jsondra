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

var get = function(ks, cf, k, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    uri = "/" + escape(ks) + "/" + escape(cf) + "/" + escape(k) + "/";
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
                responseBody = {"Error": True}
            }
            callback(responseBody);
        });
    });
};

var del = function(ks, cf, k, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    uri = "/" + escape(ks) + "/" + escape(cf) + "/" + escape(k) + "/";
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
            callback(responseBody);
        });
    });
};

var post = function(ks, cf, k, v, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    var val = JSON.stringify(v)
    uri = "/" + escape(ks) + "/" + escape(cf) + "/" + escape(k) + "/";
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
            callback(responseBody);
        });
    });
};

var put = function(ks, cf, k, v, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    var val = JSON.stringify(v)
    uri = "/" + escape(ks) + "/" + escape(cf) + "/" + escape(k) + "/";
    data = "v=" + escape(val);
    headers = {
        "Content-Length": data.length,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    var request = jsondra.put(uri, headers);
    request.sendBody(data);
    request.finish(function(response){
        response.setBodyEncoding("utf8");
        var responseBody = "";
        response.addListener("body", function (chunk) {
            responseBody += chunk;
        });
        response.addListener("complete", function() {
            callback(responseBody);
        });
    });
};

exports.get = get
exports.del = del
exports.post = post
exports.put = put
