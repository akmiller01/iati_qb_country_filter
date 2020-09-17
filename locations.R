list.of.packages <- c("data.table", "anytime", "dplyr", "reshape2","splitstackshape","stringr", "httr","XML","jsonlite","sp", "rgdal")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd = "/home/alex/git/iati_qb_country_filter/output/"
setwd(wd)

split_transactions = fread("TD_transaction_split_recode.csv")
split_transactions$transaction_date = anydate(split_transactions$transaction_date_iso_date)
split_transactions$quarter = quarter(split_transactions$transaction_date)
split_transactions$transaction_quarter = paste0(split_transactions$year, "_Q", split_transactions$quarter)

location_vars = c(
  "location_administrative_code",
  "location_administrative_level",
  "location_administrative_vocabulary",
  "location_name_narrative",
  "location_exactness_code",
  "location_point_pos"
)

activity_level_vars = c(
  "reporting_org_narrative",
  "reporting_org_ref",
  "reporting_org_type_name",
  "title_narrative",
  "iati_identifier",
  location_vars
)
transaction_level_vars = c(
  "transaction_type_name",
  "transaction_quarter"
)
transaction_value_var = "country_sector_transaction_value_usd"
keep = c(
  activity_level_vars,
  transaction_level_vars,
  transaction_value_var
)

split_transactions = split_transactions[,keep,with=F]
split_transactions_sum = data.table(split_transactions)[,.(value_sum=sum(country_sector_transaction_value_usd,na.rm=T)),by=c(activity_level_vars,transaction_level_vars)]
split_transactions_sum = split_transactions_sum[order(split_transactions_sum$transaction_quarter),]
activities = dcast(
  split_transactions_sum,
  reporting_org_narrative+reporting_org_ref+reporting_org_type_name+title_narrative+iati_identifier+location_administrative_code+location_administrative_level+location_administrative_vocabulary+location_name_narrative+location_exactness_code+location_point_pos~transaction_type_name+transaction_quarter,
  value.var = "value_sum"
)
activities$location_point_pos = gsub("),",")|",activities$location_point_pos,fixed=T)
activities$location_point_pos = gsub(",","",activities$location_point_pos,fixed=T)
activities$location_point_pos = gsub("|",",",activities$location_point_pos,fixed=T)
activities$location_point_pos = gsub(")","",activities$location_point_pos,fixed=T)
activities$location_point_pos = gsub("(","",activities$location_point_pos,fixed=T)

value_vars = setdiff(names(activities),names(split_transactions_sum))

names(activities) = gsub("_",".",names(activities))
dot_location_names = gsub("_",".",location_vars)
original_names = names(activities)
act.split = cSplit(activities,dot_location_names,",")
new_names = setdiff(names(act.split),original_names)
act.split.long = reshape(act.split, varying=new_names, direction="long", sep="_")
# act.split.long$all.miss = apply(is.na(act.split.long[,c("location.administrative.code","location.point.pos"),with=F]),1,all)
act.split.long$all.miss = apply(is.na(act.split.long[,c("location.point.pos"),with=F]),1,all)
act.split.long=subset(act.split.long, all.miss==F)
act.split.long[,c("time","id","all.miss")] = NULL

all_locations = act.split.long
names(all_locations) = gsub(".","_",names(all_locations),fixed=T)
rm(act.split,act.split.long,activities,split_transactions,split_transactions_sum)

coords = cSplit(all_locations, "location_point_pos", " ")
coordinates(coords)=~location_point_pos_1+location_point_pos_2

td = readOGR("../tcd_adm1/tcd_admbnda_adm1_ocha.shp")
proj4string(coords) = proj4string(td)
over_dat = over(coords,td)
coords$location_point_pos_adm1_name = over_dat$admin1Name

# plot(td)
td_coords = subset(coords,!is.na(location_point_pos_adm1_name))
# points(td_coords)
td_coords = data.table(data.frame(td_coords))
td_coords[,"optional"] = NULL
td_coords[ , `:=`( location_count = .N), by = iati_identifier ]
td_coords[,value_vars] = td_coords[,value_vars,with=F] / td_coords$location_count

non_value_vars = setdiff(names(td_coords),value_vars)
td_coords = td_coords[,c(non_value_vars,value_vars),with=F]

fwrite(td_coords,"td_coords.csv")
