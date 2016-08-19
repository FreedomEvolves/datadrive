require '/home/nspeare/scripts/get_db_values.rb'
require '/home/nspeare/scripts/toggle_path.rb'
require '/home/nspeare/SystemAutomation/email.rb'

require 'csv'
require 'byebug'
require 'tiny_tds'

puts "Success"

"0221 0228 0229".split.each do |qr|
  mdf = toggle_path(getdbvalues("SELECT MasterDataFile FROM ProjectInfo.dbo.Form WHERE FormID=#{qr}".first(['MasterDataFile'])
  puts mdf
end
