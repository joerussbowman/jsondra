/* 
 * jsondra.js - library for interacting with jsondra node.js
 *
 * requires: node.js
 *
 * copyright 2009 Joseph Bowman
 */

var sys = require("sys"),
    http = require("http");

var Jsondra = new process.EventEmitter();

exports.get = function(ks, cf, k, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    data = "ks=" + escape(ks) + "&cf=" + escape(cf) + "&k=" + escape(k);
    headers = {
        "Content-Length": data.length,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    var request = jsondra.post("/record/get/", headers);
    request.sendBody(data);
    request.finish(function(response){
        response.setBodyEncoding("utf8");
        var responseBody = "";
        response.addListener("body", function (chunk) {
            responseBody += chunk;
        });
        response.addListener("complete", function() {
            sys.puts("STATUS: " + response.statusCode);
            if (response.statusCode == 200) {
            }
            else if (response.statusCode == 404) {
                responseBody = JSON.stringify(null)
            }
            else {
                responseBody = {"Error": True}
            }
            callback(JSON.stringify(responseBody));
        });
    });
};

exports.del = function(ks, cf, k, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    data = "ks=" + escape(ks) + "&cf=" + escape(cf) + "&k=" + escape(k);
    headers = {
        "Content-Length": data.length,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    var request = jsondra.post("/record/delete/", headers);
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

exports.put = function(ks, cf, k, v, callback) {
    var jsondra = http.createClient(GLOBAL.settings["jsondra_connection"]["port"], GLOBAL.settings["jsondra_connection"]["host"]);
    var val = JSON.stringify(v)
    data = "ks=" + escape(ks) + "&cf=" + escape(cf) + "&k=" + escape(k) + "&v=" + escape(val);
    headers = {
        "Content-Length": data.length,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    var request = jsondra.post("/record/put/", headers);
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
