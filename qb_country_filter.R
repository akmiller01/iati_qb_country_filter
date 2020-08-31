list.of.packages <- c("data.table", "anytime", "dplyr", "reshape2","splitstackshape","stringr", "readr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd = "/home/alex/git/iati_qb_country_filter/output/"
setwd(wd)

recipient_country = "TD"
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

drop = c(drop,"V221","transaction_type_code")
drop = unique(drop)
act[,drop] = NULL
overlapping_names = intersect(names(act),names(trans))
overlapping_names = overlapping_names[which(overlapping_names!="iati_identifier")]
act[,overlapping_names] = NULL

act = unique(act)
all = merge(trans,act,by="iati_identifier")
setnames(all,"transaction_type", "transaction_type_code")
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
agg.split.long$transaction.value.split=(agg.split.long$recipient.country.percentage/100)*agg.split.long$transaction.value
agg.split.long$transaction.value.split[which(is.nan(agg.split.long$transaction.value.split))] = 0
agg.split.long$country.transaction.value = agg.split.long$transaction.value.split
agg.split.long[,c("transaction.value.split", "max_count", "count", "row.id", "id", "time", "sum_percent")] = NULL
post = sum(agg.split.long$country.transaction.value,na.rm=T)
pre == post
all_recip_multi_activity_level_split = subset(agg.split.long,recipient.country.code==recipient_country)
names(all_recip_multi_activity_level_split) = gsub(".","_",names(all_recip_multi_activity_level_split),fixed=T)

all_recip_transaction_level$recipient_split_type = paste0(recipient_country," only")
all_recip_transaction_level$recipient_country_percentage = 100
all_recip_transaction_level$x_recipient_country_code = all_recip_transaction_level$transaction_recipient_country_code
all_recip_transaction_level$country_transaction_value = all_recip_transaction_level$transaction_value
all_recip_activity_level$x_recipient_country_code = recipient_country
all_recip_activity_level$recipient_country_percentage = as.numeric(gsub(",","",all_recip_activity_level$recipient_country_percentage))
all_recip_activity_level$recipient_country_percentage[which(is.na(all_recip_activity_level$recipient_country_percentage))] = 100
all_recip_activity_level$country_transaction_value = all_recip_activity_level$transaction_value * (all_recip_activity_level$recipient_country_percentage/100)
all_recip_activity_level$recipient_split_type = "Multi-country"
all_recip_activity_level$recipient_split_type[which(all_recip_activity_level$recipient_country_percentage>=100)] = paste0(recipient_country," only")
all_recip_multi_activity_level_split$x_recipient_country_code = all_recip_multi_activity_level_split$recipient_country_code
all_recip_multi_activity_level_split$recipient_split_type = "Multi-country"
all_recip_multi_activity_level_split$recipient_split_type[which(all_recip_multi_activity_level_split$recipient_country_percentage>=100)] = paste0(recipient_country," only")
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

agg.split.long$transaction.value.split=(agg.split.long$x.sector.percentage/100)*agg.split.long$country.transaction.value
agg.split.long$transaction.value.split[which(is.na(agg.split.long$transaction.value.split))] = agg.split.long$country.transaction.value[which(is.na(agg.split.long$transaction.value.split))]
agg.split.long$country.sector.transaction.value = agg.split.long$transaction.value.split
setdiff(unique(agg.split.long$transaction.id),c(1:nrow(all)))
agg.split.long$x_sector_percentage_sum = agg.split.long$sum_percent
agg.split.long[,c("max_count", "count", "transaction.id", "id", "time", "transaction.value.split" ,"sum_percent")] = NULL

all = agg.split.long
names(all) = gsub(".","_",names(all),fixed=T)
post = sum(all$country_sector_transaction_value,na.rm=T)
pre == post

# 5. Recode, join codelists, convert to USD ####

org_id_imp = fread("../IATIOrganisationIdentifier.csv")
org_id_imp = org_id_imp[,c("code","name")]
names(org_id_imp) = c("ref","recode")

implementers = function(row){
  org_roles = as.character(row$participating_org_role)
  org_narratives = as.character(row$participating_org_narrative)
  org_types = as.character(row$participating_org_type)
  org_refs = as.character(row$participating_org_ref)
  
  role_split = str_split(org_roles,",")[[1]]
  narr_split = str_split(org_narratives,",")[[1]]
  type_split = str_split(org_types,",")[[1]]
  ref_split = str_split(org_refs,",")[[1]]
  max_len = max(length(role_split),length(narr_split),length(type_split),length(ref_split))
  if(length(role_split)<max_len){
    lendiff = max_len - length(role_split)
    role_split = c(role_split, rep("",lendiff))
  }
  if(length(narr_split)<max_len){
    lendiff = max_len - length(narr_split)
    narr_split = c(narr_split, rep("",lendiff))
  }
  if(length(type_split)<max_len){
    lendiff = max_len - length(type_split)
    type_split = c(type_split, rep("",lendiff))
  }
  if(length(ref_split)<max_len){
    lendiff = max_len - length(ref_split)
    ref_split = c(ref_split, rep("",lendiff))
  }
  row_df = data.frame(role=role_split,narr=narr_split,type=type_split,ref=ref_split)
  row_df = subset(row_df,role=="4")
  row_df = merge(row_df,org_id_imp,by="ref",all.x=T)
  row_df$narr[which(is.na(row_df$narr))] = row_df$recode[which(is.na(row_df$narr))]
  row$implementing_narrative = paste0(row_df$narr,collapse=",")
  return(row)
}

sectors = fread("../Sector.csv")
sectors = sectors[,c("code","name")]
names(sectors) = c("x_sector_code","x_sector_name")
all$x_sector_code[which(!(all$x_sector_vocabulary %in% c(1,2)))] = NA
all$x_sector_vocabulary[which(!(all$x_sector_vocabulary %in% c(1,2)))] = NA
all$x_sector_code = as.numeric(as.character(all$x_sector_code))
all$x_sector_vocabulary[which(is.na(all$x_sector_code))] = 1
all$x_sector_percentage[which(is.na(all$x_sector_code))] = 100
all$x_sector_code[which(is.na(all$x_sector_code))] = 99810
all = merge(all,sectors,all.x=T)

sector_cats = fread("../SectorCategory.csv")
sector_cats = sector_cats[,c("code","name")]
names(sector_cats) = c("x_sector_cat_code","x_sector_cat_name")
all$x_sector_cat_code = as.numeric(substr(as.character(all$x_sector_code),1,3))
all = merge(all,sector_cats,by="x_sector_cat_code",all.x=T)

all$x_currency = all$transaction_value_currency
all$x_currency[which(is.na(all$x_currency))] = all$default_currency[which(is.na(all$x_currency))] 

all$x_aid_type_code = all$transaction_aid_type_code
all$x_aid_type_code = as.character(all$x_aid_type_code)
all$x_aid_type_vocabulary = all$transaction_aid_type_vocabulary
all$x_aid_type_vocabulary = as.character(all$x_aid_type_vocabulary)
all$x_aid_type_vocabulary[which(is.na(all$x_aid_type_code))] = all$default_aid_type_vocabulary[which(is.na(all$x_aid_type_code))]
all$x_aid_type_code[which(is.na(all$x_aid_type_code))] = all$default_aid_type_code[which(is.na(all$x_aid_type_code))]

all$x_finance_type_code = all$transaction_finance_type_code
all$x_finance_type_code[which(is.na(all$x_finance_type_code))] = all$default_finance_type_code[which(is.na(all$x_finance_type_code))]

all_implementing = all[,c("participating_org_role","participating_org_narrative","participating_org_type","participating_org_ref")]
all_implementing$implementing_narrative = NA
all_implementing = data.frame(all_implementing)
pb = txtProgressBar(max=nrow(all_implementing),style=3)
for(i in 1:nrow(all_implementing)){
  setTxtProgressBar(pb,i)
  all_implementing[i,] = implementers(all_implementing[i,])
}
close(pb)
all_implementing = cSplit(all_implementing,c("implementing_narrative"),",")
all_implementing[,c("participating_org_role","participating_org_narrative","participating_org_type","participating_org_ref")] = NULL
all_implementing[,1][which(is.na(all_implementing[,1]))] = "Not specified"

all = cbind(all,all_implementing)

all$x_transaction_provider_org = all$transaction_provider_org_narrative
all$x_transaction_provider_org = as.character(all$x_transaction_provider_org)

org_id = fread("../IATIOrganisationIdentifier.csv")
org_id = org_id[,c("code","name")]
names(org_id) = c("transaction_provider_org_ref","x_transaction_provider_org_recode")
all = merge(all,org_id,by="transaction_provider_org_ref",all.x=T)

all$x_transaction_provider_org[which(is.na(all$x_transaction_provider_org))] = all$x_transaction_provider_org_recode[which(is.na(all$x_transaction_provider_org))]
all$x_transaction_provider_org_recode = NULL
all$x_transaction_provider_org[which(is.na(all$x_transaction_provider_org))] = "Not specified"

all$x_transaction_receiver_org = all$transaction_receiver_org_narrative
all$x_transaction_receiver_org = as.character(all$x_transaction_receiver_org)
names(org_id) = c("transaction_receiver_org_ref","x_transaction_receiver_org_recode")
all = merge(all,org_id,by="transaction_receiver_org_ref",all.x=T)
all$x_transaction_receiver_org[which(is.na(all$x_transaction_receiver_org))] = all$x_transaction_receiver_org_recode[which(is.na(all$x_transaction_receiver_org))]
all$x_transaction_receiver_org_recode = NULL
all$x_transaction_receiver_org[which(is.na(all$x_transaction_receiver_org))] = "Not specified"

transaction_types = fread("../TransactionType.csv")
transaction_types = transaction_types[,c("code","name")]
names(transaction_types) = c("transaction_type_code","transaction_type_name")
all$transaction_type_code = as.numeric(all$transaction_type_code)
all = merge(all,transaction_types,by="transaction_type_code",all.x=T)

organisation_types = fread("../OrganisationType.csv")
organisation_types = organisation_types[,c("code","name")]
names(organisation_types) = c("reporting_org_type_code","reporting_org_type_name")
all$reporting_org_type_code = as.numeric(all$reporting_org_type_code)
all = merge(all,organisation_types,by="reporting_org_type_code",all.x=T)

finance_types = fread("../FinanceType.csv")
finance_types = finance_types[,c("code","name")]
names(finance_types) = c("x_finance_type_code","x_finance_type_name")
all$x_finance_type_code = as.numeric(all$x_finance_type_code)
all = merge(all,finance_types,by="x_finance_type_code",all.x=T)

aid_types = fread("../AidType.csv")
aid_types = aid_types[,c("code","name")]
names(aid_types) = c("x_aid_type_code","x_aid_type_name")
all$x_aid_type_code = as.character(gsub(",","",all$x_aid_type_code))
all = merge(all,aid_types,by="x_aid_type_code",all.x=T)

ex_rates = fread("../ex_rates.csv")
all$year = as.numeric(substr(as.character(all$transaction_date_iso_date),1,4))
names(ex_rates) = c("year","x_currency","ex_rate")
setdiff(unique(all$x_currency),unique(ex_rates$x_currency))
all$x_currency[which(all$x_currency=="FKP")] = "GBP"
all = merge(all,ex_rates,by=c("year","x_currency"), all.x=T)
all$country_sector_transaction_value_usd = all$country_sector_transaction_value * all$ex_rate

all$title_narrative[which(is.na(all$title_narrative))] = "No title reported"
all$description_narrative[which(is.na(all$description_narrative))] = "No description reported"
all = subset(all,!is.na(transaction_value) & transaction_value!=0)
all$x_finance_type_name[which(is.na(all$x_finance_type_name))] = "Not specified"
all$x_aid_type_name[which(is.na(all$x_aid_type_name))] = "Not specified"
all$x_sector_name[which(is.na(all$x_sector_name))] = all$x_sector_cat_name[which(is.na(all$x_sector_name))] 

fwrite(all,paste0(recipient_country,"_transaction_split_recode.csv"))
