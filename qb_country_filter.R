list.of.packages <- c("data.table", "anytime", "dplyr", "reshape2","splitstackshape","stringr", "readr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd = "/home/alex/git/iati_qb_country_filter/output/"
setwd(wd)

recipient_country = "NG"
start_date = "2015-12-31"

# 1. Query filtered transactions ####

url = paste0(
  "https://iatidatastore.iatistandard.org/search/transaction?q=((transaction_recipient_country_code:(",
  recipient_country,
  ") OR activity_recipient_country_code:(",
  recipient_country,
  ")) AND transaction_date_iso_date:[",
  start_date,
  "T00:00:00Z TO *])&wt=csv&tr=transaction-csv.xsl&rows=100000"
)
trans_filename = paste0(recipient_country,"_transactions.csv")

if(!file.exists(trans_filename)){
  download.file(
    URLencode(url),
    destfile=trans_filename
  )
}

trans <- read.table(
  trans_filename,
  header=T,
  sep=",",
  quote=c("\""),
  na.strings="",
  stringsAsFactors=FALSE,
  flush=T,
  fill=T
)
write_excel_csv(trans,paste0(recipient_country,"_transactions_utf8.csv"),na="")

# 2. Filter by transaction type, join activity level data
trans = subset(trans,transaction_type %in% c(3, 4))
drop = names(trans)[which(startsWith(names(trans),"activity_"))]
drop = c(drop,"X_version_","title_lang","id","reporting_org_secondary_reporter")
trans[,drop] = NULL

big_url = paste0(
  "https://iatidatastore.iatistandard.org/search/activity?q=recipient_country_code:(",
  recipient_country,
  ") AND activity_date_start_actual_f:[",
  start_date,
  "T00:00:00Z TO *]&wt=csv&tr=activity-csv.xsl&rows=100000"
)

act_filename = paste0(recipient_country,"_act1.csv")

if(!file.exists(act_filename)){
  download.file(
    URLencode(big_url),
    destfile=act_filename
  )
}

act <- read.table(
  act_filename,
  header=T,
  sep=",",
  quote=c("\""),
  na.strings="",
  stringsAsFactors=FALSE,
  flush=T,
  fill=T
)

trans_ids = unique(trans$iati_identifier)

act = subset(act,iati_identifier %in% trans_ids)

act_ids = unique(act$iati_identifier)
missing_ids = setdiff(trans_ids,act_ids)

search_terms = missing_ids

search_fields = c(
  "iati_identifier"
)

search_query = ""
search_grid = expand.grid(search_fields, search_terms, stringsAsFactors = F)
search_strs = paste0(search_grid$Var1, ':"', search_grid$Var2, '"')
chunk_seq = round(seq(1,length(search_strs),length.out=50))


act_list = list()
for(i in 1:(length(chunk_seq)-1)){
  message(i)
  start_i = chunk_seq[i]
  end_i = chunk_seq[i+1]
  chunk_strs = search_strs[start_i:end_i]
  search_query = paste0(chunk_strs,collapse=" OR ")
  search_url = paste0(
    'https://iatidatastore.iatistandard.org/search/activity?q=(',
    search_query,
    ')&wt=csv&tr=activity-csv.xsl&rows=10000'
  )
  act_filename = paste0(recipient_country,"_act",i+1,".csv")
  if(!file.exists(act_filename)){
    download.file(
      URLencode(search_url),
      destfile=act_filename
    )
  }
  tmp <- read.table(
    act_filename,
    header=T,
    sep=",",
    quote=c("\""),
    na.strings="",
    stringsAsFactors=FALSE,
    flush=T,
    fill=T
  )
  act_list[[i]] = tmp
}

act_addition = rbindlist(act_list)
act = rbind(act,act_addition)
save(act,file=paste0(recipient_country,"_activities.RData"))
