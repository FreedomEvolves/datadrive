#!/usr/local/rvm/rubies/ruby-2.1.1/bin/ruby

# use tabsize 2 for optimal viewing

$testmode=0
$xtra_info=""

require 'viewpoint'
# require 'byebug'

include Viewpoint::EWS

def notify_nick(msg)
	log msg
	email "nspeare","","MSG from #{$PROGRAM_NAME}",msg
end

def email(em_to, em_cc, em_subject, em_body)

	# get credentials and log in
	creds=File.read('/home/nspeare/SystemAutomation/.mailcredentials').split("\n")
	Viewpoint::EWS::EWS.endpoint='https://mail.dixiegraphicsinc.com/ews/exchange.asmx'
	Viewpoint::EWS::EWS.set_auth creds[0], creds[1]

	eto=Array.new
	em_to.split(';').each do |e|
		if e !~ /(.*)@(.*)/ then
			e=e + "@dixiegraphicsinc.com"
		end
		eto << e
	end

	ecc=Array.new
	em_cc.split(';').each do |e|
		if e !~ /(.*)@(.*)/ then
			e=e + "@dixiegraphicsinc.com"
		end
		ecc << e
	end

		Viewpoint::EWS::Message.send(		\
			em_subject,										\
			em_body,											\
			eto,													\
			ecc
		)
 
end

