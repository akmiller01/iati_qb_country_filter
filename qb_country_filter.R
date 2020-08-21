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

# 2. Filter by transaction type, join activity level data ####
trans = subset(trans,transaction_type %in% c(3, 4))
drop = names(trans)[which(startsWith(names(trans),"activity_"))]
drop = c(drop,"X_version_","title_lang","id","reporting_org_secondary_reporter")
trans[,drop] = NULL


if(!file.exists(paste0(recipient_country,"_activities.RData"))){
  big_url = paste0(
    "https://iatidatastore.iatistandard.org/search/activity?q=recipient_country_code:(",
    recipient_country,
    ") AND activity_date_start_actual_f:[",
    start_date,
    "T00:00:00Z TO *]&wt=xslt&tr=activity-csv.xsl&rows=100000"
  )
  
  act_filename = paste0(recipient_country,"_act1.csv")
  
  if(!file.exists(act_filename)){
    download.file(
      URLencode(big_url),
      destfile=act_filename
    )
  }
  act <- fread(act_filename)
  
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
  chunk_seq = round(seq(1,length(search_strs),length.out=200))
  
  
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
      ')&wt=xslt&tr=activity-csv.xsl&rows=10000'
    )
    act_filename = paste0(recipient_country,"_act",i+1,".csv")
    if(!file.exists(act_filename)){
      download.file(
        URLencode(search_url),
        destfile=act_filename
      )
    }
    tmp <- fread(act_filename,colClasses = c(rep("character", 221)))
    act_list[[i]] = tmp
  }
  
  act_addition = rbindlist(act_list,fill=T)
  act = rbind(act,act_addition,fill=T)
  save(act,file=paste0(recipient_country,"_activities.RData"))
}else{
  load(paste0(recipient_country,"_activities.RData"))
}

setdiff(unique(act$iati_identifier),unique(trans$iati_identifier))
setdiff(unique(trans$iati_identifier),unique(act$iati_identifier))

drop = names(act)[which(startsWith(names(act),"fss"))]
drop = c(drop, names(act)[which(startsWith(names(act),"result"))])
drop = c(drop, names(act)[which(startsWith(names(act),"crs"))])
drop = c(drop, names(act)[which(startsWith(names(act),"document"))])

drop = c(drop,"V221")
drop = unique(drop)
act[,drop] = NULL
overlapping_names = intersect(names(act),names(trans))
overlapping_names = overlapping_names[which(overlapping_names!="iati_identifier")]
act[,overlapping_names] = NULL

all = merge(trans,act,by="iati_identifier")
fwrite(all,paste0(recipient_country,"_transactions_merge.csv"))

# 3. Split recipient country ####
all$unique_transaction_id = c(1:nrow(all))
all_recip_transaction_level = subset(all,transaction_recipient_country_code==recipient_country)
all_recip_activity_level = subset(all,recipient_country_code==paste0(recipient_country,",") & !(unique_transaction_id %in% all_recip_transaction_level$unique_transaction_id))
accounted_ids = c(all_recip_transaction_level$unique_transaction_id, all_recip_activity_level$unique_transaction_id)
all_recip_multi_activity_level = subset(all,!(unique_transaction_id %in% accounted_ids))

pre = sum(all_recip_multi_activity_level$transaction_value,na.rm=T)
all_recip_multi_activity_level$row.id = c(1:nrow(all_recip_multi_activity_level))
names(all_recip_multi_activity_level) = gsub("_",".",names(all_recip_multi_activity_level))
original_names = names(all_recip_multi_activity_level)
agg.split = cSplit(all_recip_multi_activity_level,c("recipient.country.code", "recipient.country.percentage"),",")
new_names = setdiff(names(agg.split),original_names)
agg.split.long = reshape(agg.split, varying=new_names, direction="long", sep="_")
agg.split.long$transaction.value = as.numeric(agg.split.long$transaction.value)
agg.split.long$recipient.country.percentage = as.numeric(agg.split.long$recipient.country.percentage)
agg.split.long[ , `:=`( max_count = .N , count = 1:.N), by = row.id ]
agg.split.long=subset(agg.split.long, !is.na(recipient.country.code) | max_count==1 | count==1)
agg.split.long$recipient.country.percentage[which(is.na(agg.split.long$recipient.country.percentage))] = 100
agg.split.long[ , `:=`( sum_percent=sum(recipient.country.percentage, na.rm=T) ) , by = row.id ]
agg.split.long$transaction.value.split=(agg.split.long$recipient.country.percentage/agg.split.long$sum_percent)*agg.split.long$transaction.value
agg.split.long$transaction.value.split[which(is.nan(agg.split.long$transaction.value.split))] = 0
agg.split.long$country.transaction.value = agg.split.long$transaction.value.split
agg.split.long[,c("transaction.value.split", "max_count", "count", "row.id", "id", "time", "sum_percent")] = NULL
post = sum(agg.split.long$country.transaction.value,na.rm=T)
pre == post
all_recip_multi_activity_level_split = subset(agg.split.long,recipient.country.code==recipient_country)
names(all_recip_multi_activity_level_split) = gsub(".","_",names(all_recip_multi_activity_level_split),fixed=T)

all_recip_transaction_level$recipient_country_percentage = 100
all_recip_transaction_level$x_recipient_country_code = all_recip_transaction_level$transaction_recipient_country_code
all_recip_transaction_level$country_transaction_value = all_recip_transaction_level$transaction_value
all_recip_activity_level$x_recipient_country_code = recipient_country
all_recip_activity_level$recipient_country_percentage = 100
all_recip_activity_level$country_transaction_value = all_recip_activity_level$transaction_value
all_recip_multi_activity_level_split$x_recipient_country_code = all_recip_multi_activity_level_split$recipient_country_code
all = rbind(all_recip_transaction_level, all_recip_activity_level,all_recip_multi_activity_level_split)

# 4. Split by sector ####

single_vocabulary = function(row){
  codes = as.character(row$x_sector_code)
  percentages = as.character(row$x_sector_percentage)
  vocabularies = as.character(row$x_sector_vocabulary)
  
  code_split = str_split(codes,",")[[1]]
  if(length(code_split)==1 & length(percentages)==0){
    percentages = "100"
  }
  perc_split = str_split(percentages,",")[[1]]
  vocab_split = str_split(vocabularies,",")[[1]]
  if(length(code_split)!=length(perc_split) |
     length(perc_split)!=length(vocab_split) |
     length(vocab_split)!=length(code_split)
  ){
    row$x_sector_code = ""
    row$x_sector_percentage = ""
    row$x_sector_vocabulary = ""
    return(row)
  }
  row_df = data.frame(code=code_split,percent=perc_split,vocab=vocab_split)
  if("1" %in% vocab_split){
    row_df = subset(row_df,vocab=="1")
  }else if("2" %in% vocab_split){
    row_df = subset(row_df,vocab=="2")
  }else if("98" %in% vocab_split){
    row_df = subset(row_df,vocab=="98")
  }else if("99" %in% vocab_split){
    row_df = subset(row_df,vocab=="99")
  }else if("DAC" %in% vocab_split){
    row_df = subset(row_df,vocab=="DAC")
  }else{
    row_df = subset(row_df,is.na(vocab))
  }
  row$x_sector_code = paste0(row_df$code,collapse=",")
  row$x_sector_percentage = paste0(row_df$percent,collapse=",")
  row$x_sector_vocabulary = paste0(row_df$vocab,collapse=",")
  return(row)
}

all$x_sector_code = as.character(all$transaction_sector_code)
all$x_sector_vocabulary = all$transaction_sector_vocabulary
all$x_sector_percentage = "100"
all$x_sector_vocabulary = as.character(all$x_sector_vocabulary)
all$x_sector_vocabulary[which(is.na(all$x_sector_code))] = all$sector_vocabulary[which(is.na(all$x_sector_code))]
all$x_sector_percentage[which(is.na(all$x_sector_code))] = all$sector_percentage[which(is.na(all$x_sector_code))]
all$x_sector_code[which(is.na(all$x_sector_code))] = all$sector_code[which(is.na(all$x_sector_code))]
pre = sum(all$country_transaction_value,na.rm=T)

all.sector = data.table(all[,c("x_sector_code","x_sector_vocabulary","x_sector_percentage")])
pb = txtProgressBar(max=nrow(all.sector),style=3)
for(i in 1:nrow(all.sector)){
  setTxtProgressBar(pb,i)
  all.sector[i,] = single_vocabulary(all.sector[i,])
}
close(pb)
all$x_sector_code = all.sector$x_sector_code
all$x_sector_percentage = all.sector$x_sector_percentage
all$x_sector_vocabulary = all.sector$x_sector_vocabulary
all$transaction.id = c(1:nrow(all))
names(all) = gsub("_",".",names(all))
original_names = names(all)
agg.split = cSplit(all,c("x.sector.code", "x.sector.percentage", "x.sector.vocabulary"),",")
new_names = setdiff(names(agg.split),original_names)
agg.split.long = reshape(agg.split, varying=new_names, direction="long", sep="_")
agg.split.long$x.sector.percentage = as.numeric(agg.split.long$x.sector.percentage)
agg.split.long$x.sector.percentage[which(is.na(agg.split.long$x.sector.percentage))] = 100
agg.split.long$x.sector.percentage[which(is.na(agg.split.long$x.sector.code))] = NA
agg.split.long[ , `:=`( max_count = .N , count = 1:.N, sum_percent=sum(x.sector.percentage, na.rm=T)) , by = .(transaction.id) ]
agg.split.long=subset(agg.split.long, !is.na(x.sector.code) | max_count==1 | count==1)

agg.split.long$transaction.value.split=(agg.split.long$x.sector.percentage/agg.split.long$sum_percent)*agg.split.long$country.transaction.value
agg.split.long$transaction.value.split[which(is.na(agg.split.long$transaction.value.split))] = agg.split.long$country.transaction.value[which(is.na(agg.split.long$transaction.value.split))]
agg.split.long$country.sector.transaction.value = agg.split.long$transaction.value.split
setdiff(unique(agg.split.long$transaction.id),c(1:nrow(all)))
agg.split.long[,c("max_count", "count", "transaction.id", "id", "time", "transaction.value.split" ,"sum_percent")] = NULL

all = agg.split.long
names(all) = gsub(".","_",names(all),fixed=T)
post = sum(all$country_sector_transaction_value,na.rm=T)
pre == post
