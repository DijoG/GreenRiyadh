var http = require('http');
var url = require('url');
var fs = require('fs');
var path = require('path');

// Create HTTP server
http.createServer(function (req, res) {
  var q = url.parse(req.url, true);
  var filename = "." + q.pathname;

  // Get the file extension
  var extname = String(path.extname(filename)).toLowerCase();

  // Define MIME types including .geojson
  var mimeTypes = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.geojson': 'application/geo+json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif'
  };

  // Default to octet-stream if type not found
  var contentType = mimeTypes[extname] || 'application/octet-stream';

  // Read file and serve it
  fs.readFile(filename, function(err, data) {
    if (err) {
      res.writeHead(404, {
        'Content-Type': 'text/html',
        'Access-Control-Allow-Origin': '*' // Add CORS
      });
      return res.end("404 Not Found");
    }

    // Add CORS for successful responses
    res.writeHead(200, {
      'Content-Type': contentType,
      'Access-Control-Allow-Origin': '*'
    });
    res.write(data);
    return res.end();
  });

}).listen(8080);

console.log("Server running with CORS at http://localhost:8080");
