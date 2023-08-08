import requests
def sendWSP(message, apikey,gid=0):
    url = "https://whin2.p.rapidapi.com/send"
    headers = {
	"content-type": "application/json",
	"X-RapidAPI-Key": apikey,
	"X-RapidAPI-Host": "whin2.p.rapidapi.com"}
    try:
        if gid==0:
            return requests.request("POST", url, json=message, headers=headers)
        else: 
            url = "https://whin2.p.rapidapi.com/send2group"
            querystring = {"gid":gid}
            return requests.request("POST", url, json=message, headers=headers, params=querystring) 
    except requests.ConnectionError:
        return("Error: Connection Error")

# Testing Section
msg1 = {"text":"hello there fuker"}
msg2 = {"text":"this is a group message"}

myapikey = "4266cddf3fmsh649a65f07d21b12p1ca2d2jsnc6042b6b0643"
mygroup = "F0kAWtrldcO49c6fcWO2Ar"

sendWSP(msg1,myapikey)
sendWSP(msg2, myapikey,mygroup)