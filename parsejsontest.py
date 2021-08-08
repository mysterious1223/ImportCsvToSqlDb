import json

f = open('dbconfig.json',)
data = json.load(f)


data = data['dbconfig'][0]

server = data['server']
    


f.close()