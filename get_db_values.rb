require 'tiny_tds'

def getdbvalues(sql)                  
                                      
  client=TinyTds::Client.new(     
    :username =>  'KTMExtract',   
    :password =>  'password',     
    :host     =>  'DGI-SRV-001'   
	)                                     
                                      
  rows=client.execute(sql)            
#  client.close # don't close connection. if try and use results after closing, will crash
  getdbvalues=rows                    

end                 

