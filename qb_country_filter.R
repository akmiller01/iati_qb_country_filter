list.of.packages <- c("data.table", "anytime", "dplyr", "reshape2","splitstackshape","stringr", "readr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd = "/home/alex/git/iati_qb_country_filter/output/"
setwd(wd)

recipient_country = "NG"

# 1. Activities filtered by activity-level recipient country ####

url = paste0(
  "https://iatidatastore.iatistandard.org/search/activity?q=recipient_country_code:(",
  recipient_country,
  ")&wt=xslt&tr=activity-csv.xsl&rows=10000"
)
afa_filename = paste0(recipient_country,"_activity_filtered_activities.csv")

if(!file.exists(afa_filename)){
  data_file = download.file(
    url,
    destfile=afa_filename
  )
}

afa <- read.table(
  afa_filename,
  header=T,
  sep=",",
  quote=c("\""),
  na.strings="",
  stringsAsFactors=FALSE,
  flush=T
)
write_excel_csv(afa,paste0(recipient_country,"_activity_filtered_activities_utf8.csv"),na="")


# 2. Split by transaction ####

t_names = c("transaction.type.code","transaction.date.iso.date","transaction.value.currency","transaction.value.date","transaction.value","transaction.provider.org.provider.activity.id","transaction.provider.org.type","transaction.provider.org.ref","transaction.provider.org.narrative","transaction.receiver.org.receiver.activity.id","transaction.receiver.org.type","transaction.receiver.org.ref","transaction.receiver.org.narrative","transaction.disburstment.channel.code","transaction.sector.vocabulary","transaction.sector.code","transaction.recipient.country.code","transaction.recipient.region.code","transaction.recipient.region.vocabulary","transaction.flow.type.code","transaction.finance.type.code","transaction.aid.type.code","transaction.aid.type.vocabulary","transaction.tied.status.code")
afa$activity.number = c(1:nrow(afa))
names(afa) = gsub("_",".",names(afa))
original_names = names(afa)
afa.split = cSplit(afa,t_names,",")
new_names = setdiff(names(afa.split),original_names)
afa.split.long = reshape(afa.split, varying=new_names, direction="long", sep="_")
afa.split.long[ , `:=`( max_count = .N , count = 1:.N ) , by = .(activity.number) ]
afa.split.long=subset(afa.split.long, (!is.na(transaction.type.code) & !is.na(transaction.value)) | max_count==1 | count==1)
afa.split.long[,c("max_count", "count", "activity.number", "id", "time")] = NULL

afa = afa.split.long
names(afa) = gsub(".","_",names(afa),fixed=T)
afa$transaction_date_iso_date = anydate(afa$transaction_date_iso_date)
afa = subset(afa,transaction_date_iso_date >= as.Date("2016-01-01"))
afa$transaction_value = as.numeric(as.character(afa$transaction_value))
write_excel_csv(afa,paste0(recipient_country,"_activity_filtered_activities_split_t.csv"), na="")
