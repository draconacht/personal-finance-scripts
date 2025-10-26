let schemes = http get "https://api.mfapi.in/mf"

# loads all transactions
let txdata = open ~/Desktop/A.json | get Transaction | where 'Transaction Type' == 'Net Purchase'| first 100 |
select Date 'Scheme Name' ISIN Amount Price 'No of units' 'Balance Units' NAV |
str replace -r '\((.*)\)' '-$1' Amount | str replace -r ',' '' Amount | into float Amount |
str replace -r '\((.*)\)' '$1' 'No of units' |
insert 'MFAPI Code' {|row| $schemes | where isinGrowth == $row.ISIN | get schemeCode.0 } |
update Date {|row| $row.Date | into datetime --format '%d/%m/%Y'| format date '%Y/%m/%d' } | sort-by Date -r

# load NAV over time, only once to minimise HTTP requests
let navs = $txdata | select 'MFAPI Code' | uniq |
insert NAV {|row| http get ('https://api.mfapi.in/mf/' + ($row.'MFAPI Code' | into string)) | get data | first 1000 |
pdate date {|row| $row.date | into datetime --format '%d-%m-%Y'| format date '%Y/%m/%d'} |
}

# transaction metadata to reduce processing duplicacy
let tx_meta = $txdata | group-by 'MFAPI Code' --to-table |
insert MaxDate {|row| $row.items | get Date | math max} |
insert MinDate {|row| $row.items | get Date | math min} |
insert ISIN {|row| $row.items | get ISIN.0} | reject items

# balance, profit, loss, cost and market value over time
let val = $navs | join $tx_meta 'MFAPI Code' | update NAV {
 |row| $row.NAV | where 'date' >= $row.MinDate |
 insert units { |data| $txdata | where 'MFAPI Code' == $row.'MFAPI Code' and Date <= $data.date | get 'Balance Units'.0 } |
 insert cost { |data| $txdata | where 'MFAPI Code' == $row.'MFAPI Code' and Date <= $data.date | get Amount | math sum } |
 insert market { |data| ($data.nav | into float) * ($data.units | into float)} |
 insert profit { |data| $data.market - $data.cost } |
} | reject MaxDate MinDate

# weekly profit and loss report, may be imported into a budgeting or tracking tool
let pnl = $val |
update NAV {|row| $row.NAV |
 group-by --to-table { |data| $data.date | parse '{y}/{m}/{d}' | update d {|x| ($x.d | into int) // 7} | format pattern '{y}/{m}/{d}' | get 0 } |
 where closure_0 =~ '\/[0123]$' | each {|tbl| $tbl.items | first} | window 2 |
 each { |pair| {
  date: $pair.0.date
  profit: (($pair.0.profit - $pair.1.profit) | math round --precision 2)
 }}
}

$val | flatten NAV | flatten NAV | to csv | save values.csv -f
$pnl | flatten NAV | flatten NAV | insert notes "pnl" | to csv | save pnl.csv -f
$txdata | select Date Amount ISIN | save txn.csv -f
