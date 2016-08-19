#!/usr/bin/env ruby

require '/home/nspeare/scripts/get_db_values.rb'
require '/home/nspeare/scripts/toggle_path.rb'
# def email(em_to, em_cc, em_subject, em_body)
require '/home/nspeare/SystemAutomation/email.rb'
require 'csv'
require 'byebug'
require 'sqlite3'

trap("INT") { puts "\nfine. i'm done"; abort }
# Archive all of the current day's data before doing anything
tm=Time.new.strftime("%Y-%m-%d-%H")
# d="/mnt/Projects/29385_PRC_Handout_Project/Production/Test/" # Use this for testing
d="/mnt/Projects/29385_PRC_Handout_Project/Production/DataCaptured/"
# commented out in favor of the arch_file function that runs on each processed file.
# `mkdir #{d}Archive/#{tm}` 
# `cp #{d}*.txt #{d}Archive/#{tm}`
# `cp /mnt/Projects/30538_*/Production/DataCaptured/PRC_*SVY_DATA.txt #{d}Archive/#{tm}`

# Archive PRC hospice data and re-map values
hd="/mnt/Projects/31498_PRC_PRC_Hospice_CAHPS_Scanning/Production/DataCaptured/"
`grep -i -v -h scandate #{hd}PRC_HOSPICE_CAHPS_ENG_SVY_DATA.txt >> #{hd}Master/PRC_HOSPICE_CAHPS_ENG_SVY_MASTER_DATA.txt`
`mkdir -p #{hd}Archive/#{tm}`
`cp #{hd}PRC_HOSPICE_CAHPS_ENG_SVY_DATA.txt #{hd}Archive/#{tm}/`
`mv #{hd}PRC_HOSPICE_CAHPS_ENG_SVY_DATA.txt /tmp/PRC_HOSPICE_#{tm}`
`head -1 /tmp/PRC_HOSPICE_#{tm} > #{hd}PRC_HOSPICE_CAHPS_ENG_SVY_DATA.txt`
`csvfix read_dsv /tmp/PRC_HOSPICE_#{tm} | csvfix map -f 5 -fv 1,2,3,4,5,6,7,8,9 -tv 3,4,5,6,7,8,9,10,11 | csvfix write_dsv > #{hd}/PRC_HOSPICE_CAHPS_ENG_SVY_DATA.txt`

data_out=Array.new
verbatims_out=Array.new
scan_dates=Hash.new

begin

# QR=0165 removed per Denise's email dated Fri 7/31/2015 10:17 AM
# 0200 added 2015-10-07 as replacement for 0168
# 0179 was ACO 'Mercy Hospital' and has been removed 2015-11-25
# 0153 changed to 0252 on Mon 2016-03-28
"0221 0228 0229 0194 0195 0212 0205 0204 0200 0142 0143 0144 0145 0146 0149 0150 0151 0152 0153 0154 0155 0156 0157 0158 0159 0160 0161 0162 0163 0164 0166 0167 0168 0169 0170 0171 0172 0185 0252 0276 0257".split.each do |qr|

	mdf=toggle_path(getdbvalues("SELECT MasterDataFile FROM ProjectInfo.dbo.Form WHERE FormID=#{qr}").first['MasterDataFile'])
	log "qr=#{qr}"

	if qr=="0179" then # PRC 'Mercy' off-cycle ACO
		`cat #{mdf} >> #{mdf.gsub('.txt','_MASTER.txt')}`
	end	
puts 'three'
	if File.exists?(mdf) then
   puts 'three b'
		log "mdf=#{mdf}"
		puts mdf
		hdr=File.open(mdf, &:readline)
		qty=`wc -l "#{mdf}"`.to_i
		if qty>1 then
puts 'three c'
			log "file has qty=#{qty}: #{mdf}"
      arch_file mdf
			fl=CSV.read(mdf, { :col_sep => '|', :quote_char => "`" })
			fields=fl[0].count-1
puts '3D'
			# replace header for those projects where
			# VAR variables were not put in place
			if qr=="0179" then
				replacement_header="{Data}|{ScanDate}|{Mailing}|{VAR011}|{VAR012}|{VAR013}|{VAR014}|{VAR015}|{VAR016}|{VAR017}|{VAR018}|{VAR019}|{VAR020}|{VAR021}|{VAR022}|{VAR023}|{VAR024}|{VAR025}|{VAR026}|{VAR027}|{VAR028}|{VAR029}|{VAR030}|{VAR031}|{VAR032}|{VAR033}|{VAR034}|{VAR035}|{VAR036}|{VAR037}|{VAR038}|{VAR039}|{VAR040}|{VAR041}|{VAR042}|{VAR043}|{VAR044}|{VAR045}|{VAR046}|{VAR047}|{VAR048}|{VAR049}|{VAR050}|{VAR051}|{VAR052}|{VAR053}|{VAR054}|{VAR055}|{VAR056}|{VAR057}|{VAR058}|{VAR059}|{VAR060}|{VAR061}|{VAR062}|{VAR063}|{VAR064}|{VAR065}|{VAR066}|{VAR067}|{VAR068}|{VAR069}|{VAR070}|{VAR071}|{VAR072}|{VAR073}|{VAR074}|{VAR075}|{VAR076}|{VAR077}|{VAR078}|{VAR079}|{VAR080}|{VAR081}|{VAR082}|{VAR083}|{VAR084}|{VAR085}|{VAR086}|{VAR087}|{VAR088}|{VAR089}|{VAR090}|{VAR091}|{VAR093}|{VAR094}|{VAR095}|{VAR096}|{VAR097}|{VAR098}|{VAR099}|{VAR101}|{VAR102}|{VAR103}|{VAR104}|{VAR106}|{VAR107}|{VAR108}|{VAR109}|{VAR110}|{VAR111}|{VAR112}|{Marginal1}|{Marginal2}|{Marginal3}|{Marginal4}|{Marginal5}|{Marginal6}|{Marginal7}|{Marginal8}"
				fl[0]=replacement_header.split("|")
				# Now we can proceed as normal
			end
puts 'four'
			(1..fl.size-1).each do |l| # for each line in the master data file
				barcode=fl[l][0]
				getcount(barcode)==0 ? duplicate = "" : duplicate = ((getdupenumb(fl[l][0]) || 0)+1).to_s
				(0..fields).each do |f| # go through every field
					begin
						field_name = fl[0][f].delete("{}").gsub("VAR","") # remove extraneous brackets and the "VAR" text
						field_value = ( fl[l][f] || "" ) # return value or blank if nil
						if field_value[/^\s+$/] then field_value="" end # replace blanks with nothing
						if field_name[/Scan/i] then # ScanDate
							scan_dates[fl[l][0]]=field_value
						elsif field_value=="" then
							# not verbatims
							fl[0][f][/VAR/] ? data_out << fl[l][0] + "|" + field_name + "|" + (field_value || "") + "|" + duplicate : false
						elsif (!field_value.is_number?) || (field_value.size>2) then # a crude way of identifying verbatims
							fl[0][f][/VAR/] ? verbatims_out << fl[l][0] + "|" + field_name + "|" + (field_value || "") + "|" + duplicate : false
						else
							# not verbatims
							fl[0][f][/VAR/] ? data_out << fl[l][0] + "|" + field_name + "|" + (field_value || "") + "|" + duplicate : false
						end
					rescue
						byebug
						retry
					end
				end # field
				addtodb barcode, duplicate
			end # line

		  writeit mdf,hdr
		end # if qty > 0
	else
		log "no such file! #{mdf}"
#		email "itsupport","","PRC_HO file not found: #{mdf}","PRC_HO file not found: #{mdf}"
	end
end

prc_proj_nums = {
'0142' => '2014227310',
'0143' => '2014227107',
'0144' => '2014227210',
'0145' => '2014227010',
'0146' => '2014226907',
'0149' => '2011130707',
'0150' => '2012105510',
'0151' => '2014219207',
'0152' => '2014219310',
'0153' => '2014250407',
'0154' => '2013017807',
'0155' => '2013038211',
'0156' => '2014203007',
'0157' => '2014202907',
'0158' => '2014244507',
'0159' => '2013054407',
'0160' => '2014216210',
'0161' => '2015031407',
'0162' => '2015031507',
'0163' => '2015031610',
'0164' => '2015031710',
'0165' => '2014223807',
'0166' => '2015030807',
'0167' => '2014214407',
'0168' => '2015030907',
'0200' => '2015030907',
'0169' => '2015031007',
'0185' => '2015031007',
'0170' => '2015031107',
'0171' => '2015031207',
'0172' => '2015031307',
'0179' => '2015049737',
'0204' => '2015074407',
'0885' => '2015077331',
'0884' => '2015077431',
'0883' => '2015077531',
'0882' => '2015077631',
'0881' => '2015077731',
'0228' => '2016042510',
'0229' => '2016042407',
'0194' => 'ACO09',
'0195' => 'ACO12',
'0212' => 'ACO12-SPAN',
'0221' => '2016045840',
'0252' => '2014250407',
'0276' => '2016045840',
'0257' => 'ICH2016'
}


# Create the summary file
summary_out=Array.new
data_out.map{|d|d.split("|")[0].split(" ")[1]}.uniq.each do |q| # for every qr code
	project = prc_proj_nums[q] || "UNKNOWN"	# get the prc project number
	dataUniqueIDCount=data_out.grep(/ #{q}/).map{|e|e.split("|")[0]}.uniq.count	# get number of unique barcodes in data
	verbatimUniqueIDCount=verbatims_out.grep(/ #{q}/).map{|f|f.split("|")[0]}.uniq.count # get num unique barcodes in verbatims
	verbatimTotalCount=verbatims_out.grep(/ #{q}/).count # get total number of verbatims per project

  #  looks like this line got jacked up not sure what it was meant to be
  #  me/nspeare/scripts/PRC_HO.log
	summary_out << project + "|" + dataUniqueIDCount.to_s + "|" + verbatimUniqueIDCount.to_s + "|" + verbatimTotalCount.to_s + "|" + Time.new.strftime("%m/%d/%Y") # write out in requested format
end
# puts summary_out # for debugging

# Create the "SurvIDInfo" file
surv_info=Array.new
data_out.map{|d|d.split("|")[0].split(" ")[1]}.uniq.each do |q| # for every qr code
	data_out.grep(/ #{q}/).map{|e|e.split("|")[0]}.uniq.each do |b| # for every unique barcode in outgoing data
		dgUniqueID=b
		project=prc_proj_nums[q]
		survid=b.split(" ")[0]
		receiveDate=scan_dates[b][4..5]+"/"+scan_dates[b][6..7]+"/"+scan_dates[b][0..3]
		dupenum=getdupenumb(b) 
		dupenum == 0 ? duplicate="" : duplicate=dupenum
		q=="0179" ? surveyVersion=b[-1] : surveyVersion="A" # see email from Janet Botkin dated 2015-03-31 13:40
		surv_info << dgUniqueID + "|" + project + "|" + survid + "|" + receiveDate + "|" + receiveDate + "|" + duplicate.to_s + "|" + surveyVersion
	end # barcode
end # qr code

tm=Time.new.strftime("%m%d%Y")
# Write out the files
File.open("/mnt/new_sftp/prc/HandoutSurvey/Data/#{tm}Data.txt", "a") do |f|
#	f.puts "DGUniqueID|Variable|Code|Duplicate"
#  f.puts(data_out)
end
File.open("/mnt/new_sftp/prc/HandoutSurvey/Data/#{tm}VerbatimResponses.txt", "a") do |f|
#	f.puts "DGUniqueID|Variable|Text|Duplicate"
 # f.puts(verbatims_out)
end
File.open("/mnt/new_sftp/prc/HandoutSurvey/Data/#{tm}Summary.txt", "a") do |f|
#	f.puts "Project|DataUniqueIDCount|VerbatimUniqueIDCount|VerbatimTotalCount|FileDate"
 # f.puts(summary_out)
end
File.open("/mnt/new_sftp/prc/HandoutSurvey/Data/#{tm}SurvIdInfo.txt", "a") do |f|
#	f.puts "DGUniqueID|Project|SURVID|ReceiveDate|ScanDate|Duplicate|SurveyVersion"
 # f.puts(surv_info)
end

rescue # $! = error

  err="#{$PROGRAM_NAME} died: #{$!.message}\n\n"
  msg = "*** IF THIS JOB DIES, IT IS PROBABLY BECAUSE " +
    "OF BAD BARCODE OR SCAN DATE IN SOURCE FILES. NEED TO: " +
    "(1) RESTORE FILES FROM 29385/ARCHIVE TO MAIN FOLDER, " +
    "(2) FIND AND FIX SOURCE PROBLEM, " +
    "(3) DELETE FROM BARCODES WHERE DATE = 'YYYY-MM-DD' " +
    "in ~/scripts/PRC_HO.db where YYYY-MM-DD = today " +
    "(4) RERUN PRC_HO.rb\n\n" +
    "It may also fail if an Archive directory is not present."
  backtrace="BACKTRACE BELOW\n\n" + $!.backtrace.join("\n")
#  email "itsupport","","ERROR: #{$PROGRAM_NAME}", msg + err + backtrace
  abort err

end


BEGIN {

class Object
  def is_number?
    self.to_f.to_s == self.to_s || self.to_i.to_s == self.to_s
  end
end

def log(msg)
  # time=Time.new
  time=Time.new.strftime('%Y-%m-%d %H:%M:%S ')
  open('/home/nspeare/scripts/PRC_HO.log', 'a') do |f|
    f.puts time + msg
  end
# puts msg
end

def writeit(afile,wot)
  open(afile, 'w') { |f|
    f.puts wot
  }
end

# CREATE TABLE barcodes(barcode varchar(20), date varchar(10), numb smallint );

def addtodb(barcode, numb)
	log "in addtodb"
  db=SQLite3::Database.open "/home/nspeare/scripts/PRC_HO.db"
	tm=Time.new.strftime("%Y-%m-%d")
  db.execute "insert into barcodes values ('#{barcode}', '#{tm}', #{numb.to_i})"
  db.close
end

def getcount(barcode)
	log "in getcount"
  db=SQLite3::Database.open "/home/nspeare/scripts/PRC_HO.db"
  mysql="select count(*) from barcodes where barcode='#{barcode}'"
  getcount=db.execute(mysql)[0][0] # first item of returned array
end

def getdupenumb(barcode)
  begin
	log "in getdupenumb"
  db=SQLite3::Database.open "/home/nspeare/scripts/PRC_HO.db"
  mysql="select numb from barcodes where barcode='#{barcode}' order by numb desc limit 1"
  getdupenumb=db.execute(mysql)[0][0] # first item of returned array
  rescue
    sleep 1 
    retry
  end
end

def arch_file(fpath)
  tm=Time.new.strftime("%Y-%m-%d-%H")

  fname = `basename #{fpath}`.chomp
  dirname = `dirname #{fpath}`.chomp

  #create the directory archive if if is not there
  if !File.directory?("#{dirname}/Archive/#{tm}")
    system("mkdir #{dirname}/Archive/#{tm}") 
    if $? != 0 then
      raise "Could not create Archive directory #{dirname}/Archive/#{tm}"
    end
  end

  #check to see if a file of the same name already exists in archive
  #if so then create a new archive directory with minutes and seconds
  if File.file?("#{dirname}/Archive/#{tm}/#{fname}")
    tm=Time.new.strftime("%Y-%m-%d-%H-%M-%S")
    system("mkdir #{dirname}/Archive/#{tm}")
    if $? != 0 then
      raise "Could not create Archive directory #{dirname}/Archive/#{tm} for #{fname}"
    end
  end

  system("cp #{fpath} #{dirname}/Archive/#{tm}")
  if $? != 0 then
    raise "Could not copy data file #{fpath} to #{dirname}/Archive/#{tm}"
  end 
end

}
