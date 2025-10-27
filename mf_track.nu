let schemes = http get "https://api.mfapi.in/mf"

let txdata = open ~/Desktop/A.json | get Transaction | where 'Transaction Type' == 'Net Purchase'| first 100 |
select Date 'Scheme Name' ISIN Amount Price 'No of units' 'Balance Units' NAV |
str replace -r '\((.*)\)' '-$1' Amount | str replace -r ',' '' Amount | into float Amount |
str replace -r '\((.*)\)' '$1' 'No of units' |
insert 'MFAPI Code' {|row| $schemes | where isinGrowth == $row.ISIN | get schemeCode.0 } |
update Date { into datetime --format '%d/%m/%Y' | format date '%Y/%m/%d' } | sort-by Date -r

let navs = $txdata | select 'MFAPI Code' | uniq |
insert NAV { |row| http get ('https://api.mfapi.in/mf/' + ($row.'MFAPI Code' | into string)) | get data | first 1000 |
    update date { into datetime --format '%d-%m-%Y' | format date '%Y/%m/%d'} |
}

let tx_meta = $txdata | group-by 'MFAPI Code' --to-table |
insert MaxDate { get items.Date | math max} |
insert MinDate { get items.Date | math min} |
insert ISIN { get items.ISIN.0 } | reject items

let val = $navs | join $tx_meta 'MFAPI Code' | update NAV {
    |row| $row.NAV | where 'date' >= $row.MinDate |
    insert units { |data| $txdata | where 'MFAPI Code' == $row.'MFAPI Code' and Date <= $data.date | get 'Balance Units'.0 } |
    insert cost { |data| $txdata | where 'MFAPI Code' == $row.'MFAPI Code' and Date <= $data.date | get Amount | math sum } |
    insert market { ($in.nav | into float) * ($in.units | into float)} |
    insert profit { $in.market - $in.cost } |
} | reject MaxDate MinDate

let pnl = $val |
update NAV {|row|
    group-by --to-table { |data| $data.date | parse '{y}/{m}/{d}' | update d {($in | into int) // 7} | format pattern '{y}/{m}/{d}' | get 0 } |
    where closure_0 =~ '\/[0123]$' | each { $in.items | first} | window 2 |
    each {{
        date: $in.0.date
        profit: (($in.0.profit - $in.1.profit) | math round --precision 2)
    }}
}
