import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] in ("/", "/health"):
            body = b"OK bot is runing fine\n"
            self.send_response(200)
        else:
            body = b"Not Found\n"
            self.send_response(404)

        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_server():
    port = int(os.environ.get("PORT", "10000"))

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)

    print(f"✅ HTTP Server running on port {port}", flush=True)

    server.serve_forever()


if __name__ == "__main__":
    start_server()
