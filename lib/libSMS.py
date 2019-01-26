import urllib


def sendSMS(msgText, msgRecipients):
    if not (isinstance(msgText, str) and isinstance(msgRecipients, list)):
        print "Error: Parameters of wrong type"
        return
    # Make sure msgText is not longer than 450 characters (3 message parts)
    msgText = msgText[:450]

    # Explode msgTo for all recipients
    msgTo = ','.join(map(str, msgRecipients))
    # Build the url
    url = "https://api.clickatell.com/http/sendmsg?"
    url += "&to=" + msgTo
    url += "&text=" + msgText

    # Fire off the message using clickatell http API
    urllib.urlopen(url)
