from http.server import BaseHTTPRequestHandler,HTTPServer
import json

class myHandler(BaseHTTPRequestHandler):
    # return JSON for each request which is an encoded affine and bbox
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type','application/json')
        self.end_headers()
	# Send the html message
        self.wfile.write(json.dumps({"width": 1500, "height": 2000, "affine": [1, 0, 0, 1, 0, 1], "translation": [1, 0, 0, 1, 0, 1]}).encode())
        return

server = HTTPServer(('', 9000), myHandler)
print('Started httpserver on port 9000')

server.serve_forever()

    
