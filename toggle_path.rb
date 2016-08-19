#!/usr/local/rvm/rubies/ruby-2.1.1/bin/ruby

# purpose of this is to toggle between windows and linux paths

def toggle_path(inppath)

	if inppath =~ /.*mnt.*/
		# input path is linux. convert to windows
		inppath.gsub!(47.chr,92.chr)							# convert to back slashes
		inppath.gsub!("mnt","\\\\Qnapnas-dixie")	# get rid of mnt
		inppath.gsub!(/datadrive/i,"Datadrive")		# ensure proper casing on datadrive
		toggle_path=inppath		
	else
		# input path is windows. convert to linux
		inppath.gsub!(92.chr,47.chr)						# convert windows backslashes to unix fwd slashes
		inppath.gsub!(/\/qnapnas-dixie/i,"mnt") # convert to unix path
		inppath.gsub!(/datadrive/i,"datadrive")	# ensure proper casing on datadrive (lower)
		inppath.gsub!(/10.100.100.100/i,"mnt")
		inppath.gsub!(/sftp/i, 'SFTP')
		inppath.gsub!(/homes/i,"new_sftp")
		inppath.gsub!(47.chr + 47.chr,47.chr)
		toggle_path=inppath
	end

end

