import requests
import webbrowser
import http.server
import socketserver
from spotify_secrets import * 

CLIENT_ID = clientId
CLIENT_SECRET = clientSecret
REDIRECT_URI = clientRedirectURI
AUTH_URL = f"https://accounts.spotify.com/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri={REDIRECT_URI}&scope=user-library-read%20user-read-private%20playlist-modify-public%20playlist-modify-private%20playlist-read-private"
PORT = clientPort


class RedirectHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body>Please close this window and return to the script.</body></html>")
        
        query_string = self.path.split("?")[1]
        code = query_string.split("=")[1]
        auth_response = requests.post("https://accounts.spotify.com/api/token", 
                                      data={
                                          "grant_type": "authorization_code",
                                          "code": code,
                                          "redirect_uri": REDIRECT_URI
                                      }, 
                                      auth=(CLIENT_ID, CLIENT_SECRET)
                                     )
        bearer_token = auth_response.json()["access_token"]
        self.server.bearer_token = bearer_token
        self.server.running = False


def getToken():
    webbrowser.open(AUTH_URL)
    with socketserver.TCPServer(("", PORT), RedirectHandler) as httpd:
        httpd.running = True
        while httpd.running:
            httpd.handle_request()
            
    bearer_token = httpd.bearer_token
    return bearer_token
