import requests

r = requests.get('https://www.daft.ie/for-sale/semi-detached-house-19-ticknock-dale-sandyford-dublin-18-sandyford-dublin-18/4091879', allow_redirects=False)
print(r.status_code, r.headers)
with open('file.html', 'w') as file:
    file.write(r.text)