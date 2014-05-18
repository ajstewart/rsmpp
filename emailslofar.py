#pysovo VOEvent Tools
#Tim Staley 2012
#Based on various web snippets.

import simplejson as simplejson
import os
import sys
import getpass
import smtplib
import quick_keys as keys
import base64
# import pysovo as ps
        
default_email_config_file = "~as24v07/.email_acc"

known_users={"mp":"Malgorzata.Pietka@astro.ox.ac.uk", "ag5g08":"ag5g08@soton.ac.uk", "jb34g09":"Jess.Broderick@astro.ox.ac.uk", 
"as24v07":"adam.stewart@astro.ox.ac.uk", "vh1n11":"V.Heesen@soton.ac.uk", "th2r11":"T.Hassall@soton.ac.uk", "cr1g12":"cr1g12@soton.ac.uk", 
"jharwood":"jeremy.harwood@askanastronomer.co.uk", "tmc1n12":"tmc1n12@soton.ac.uk"}
  
def prompt_for_config( config_filename = default_email_config_file ):
    default_smtp_server = "smtp.googlemail.com"
    default_smtp_port = 587
    # ps.utils.ensure_dir(config_filename)
    outputfile=open(config_filename, 'w')
    
    account = {}
    
    print "Please enter the smtp server address:" 
    print "(Default = {dserve})".format(dserve=default_smtp_server)
    account[keys.email_account.smtp_server]= raw_input(">")
    if account[keys.email_account.smtp_server]=="":
        account[keys.email_account.smtp_server] = default_smtp_server
        
    print "Please enter the smtp server port:"
    print "(Default = {dport})".format(dport=default_smtp_port)
    account[keys.email_account.smtp_port]= raw_input(">")
    if account[keys.email_account.smtp_port]=="":
        account[keys.email_account.smtp_port] = default_smtp_port
   
    print "Please enter the smtp username: (e.g. someone@gmail.com)"
    account[keys.email_account.username]= raw_input(">")
    
    print "Now please enter your password:"
    account[keys.email_account.password]= getpass.getpass()
    
    print "You entered:"
    print "Server", account[keys.email_account.smtp_server]
    print "Port", account[keys.email_account.smtp_port]
    print "User", account[keys.email_account.username]
    print "Pass", "(Not shown)"
    
    outputfile.write(simplejson.dumps(account))
    outputfile.close()
    print ""
    print "Account settings saved to:", config_filename
    
    chmod_command = "chmod go-rwx {file}".format(file=config_filename)
    os.system(chmod_command)
    
    return config_filename

def load_account_settings_from_file( config_filename = default_email_config_file):
    # print "Loading email acc from ", config_filename
    try:
        with open(config_filename, 'r') as config_file:
            account = simplejson.loads(config_file.read())
    except Exception:
        print "Error: Could not load email account from "+ config_filename
        raise 
    
    return account

def send_email( account,
                recipient_addresses,
                subject,
                body_text,
                verbose=False
                ):    
    if verbose:
        print "Loaded account, starting SMTP session"
    
    # recipient_addresses = raw_input()
    # recipient_addresses = ps.utils.listify(recipient_addresses)
    
    smtpserver = smtplib.SMTP(account[keys.email_account.smtp_server],
                              account[keys.email_account.smtp_port])
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    smtpserver.login(account[keys.email_account.username], 
                     base64.b64decode(account[keys.email_account.password]))
        
    sender = account[keys.email_account.username]
    
    recipients_str=str(recipient_addresses)
    if verbose:
        print "Logged in, emailing", recipients_str
    header = "".join( ['To: ',recipients_str,'\n',
                        'From: ',sender,'\n',
                        'Subject: ', subject,'\n'])
    
    msg = "".join( [header,'\n',
                    body_text,'\n\n'])
    smtpserver.sendmail(sender, recipient_addresses, msg)
    if verbose:
        print 'Message sent'
    smtpserver.close()
    pass
