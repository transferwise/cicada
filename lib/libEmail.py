# This could be greatly improved using
# https://docs.python.org/2/library/email-examples.html

import socket
import smtplib

SMTP_SERVER = "10.248.4.61"

hostname = socket.gethostname()
FROM_ADDR = "cicada@" + hostname + ".transferwise.com"


def sendEmail(msgSubject, msgBody, toAddrs=["infra@transferwise.com"]):
    msgSubject = "[Cicada] " + msgSubject

    # Prepare message header
    msgHeader = (
        "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n"
        % (FROM_ADDR, ", ".join(toAddrs), msgSubject)
    )

    msg = msgHeader + msgBody

    # Send the mail
    smtp_server = smtplib.SMTP(SMTP_SERVER)
    # smtp_server.set_debuglevel(1)
    smtp_server.sendmail(FROM_ADDR, toAddrs, msg)
    smtp_server.quit()
