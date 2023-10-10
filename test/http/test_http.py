import http.client

conn = http.client.HTTPSConnection("localhost", 8080)
payload = ''
headers = {}
conn.request("POST", "/api/v1/hello", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))