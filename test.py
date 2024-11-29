import http.client

conn = http.client.HTTPSConnection("api.telegram.org")

payload = '\n{\n  "chat_id": "5746497552",\n  "text": "здарова"\n}'.encode("utf-8")

headers = {"Content-Type": "application/json"}

conn.request(
    "POST",
    "/bot7219050865:AAFuYYrlMdyNTOd2Ffy83sFY-byESBF7hwQ/sendMessage",
    payload,
    headers,
)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))
