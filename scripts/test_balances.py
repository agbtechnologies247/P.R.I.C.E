import time
import hmac
import hashlib
import requests
import urllib3.util.connection

# Force IPv4 socket resolution so requests egress from 82.29.164.106 instead of IPv6
urllib3.util.connection.HAS_IPV6 = False

key = "G6JzwDFRRWXGKmerdsi6uMfx8brpBp"
secret = "EytRiB4JalYeVukRNmH51enmtpy5sxWYzF34QivZPFjtXEDor7oy7Vux4Qym"
base_url = "https://api.india.delta.exchange"

ts = str(int(time.time()))
path = "/v2/wallet/balances"
data = "GET" + ts + path
sig = hmac.new(secret.encode(), data.encode(), hashlib.sha256).hexdigest()

headers = {
    "api-key": key,
    "signature": sig,
    "timestamp": ts,
    "User-Agent": "price-engine-rust/1.1",
    "Content-Type": "application/json"
}

res = requests.get(base_url + path, headers=headers)
print("Status:", res.status_code)
print("Response:", res.text)
