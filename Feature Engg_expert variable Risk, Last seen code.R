library(dplyr)
library(lubridate)
library(data.table)

df <- read.csv('ApplicationsFrivLRemo.csv')
#df <- df[1:10000,]
summary(df)

raw_data <- df
rm(raw_data)

# Creat groups

## nameDOB

df$nameDOB <- paste(as.character(df$firstname), as.character(df$lastname), as.character(df$dob), sep = '_')


## full_address


df$fulladdress <- paste(as.character(df$address), as.character(df$zip5), sep = '_')

names(df)[1] = paste('record')

#adjusting columns order

col <- c("record", "ssn", "fulladdress", "nameDOB", "homephone", "date" , 
         "fraud_label", "firstname", "lastname")
df <- df[, col]
df$date <- ymd(df$date)
df1 <- df[df$date <= '2016-10-31',]
rm(col)

getwd()

#combinations <- list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'), c('ssn','firstname'),c('ssn','lastname'), c('nameDOB', 'homephone'), c('fulladdress', 'homephone'), c('fulladdress', 'nameDOB'))


#entities <- c('ssn','fulladdress', 'nameDOB', 'homephone')

combinations <- combn(c('ssn', 'fulladdress', 'firstname', 'lastname', 'nameDOB', 'homephone'), m = 2, simplify = FALSE)
entities <- c('ssn', 'fulladdress', 'nameDOB', 'homephone')

# Create variables

## last_time

days_since_last <- function(groupby){
  last_time <- df %>%
    arrange_(c(groupby, 'date')) %>%
    group_by_(groupby) %>%
    mutate(rank = row_number() - 1)
  last_time$rank_1 <- last_time$rank + 1
  temp_1 <- last_time %>%
    select(c(groupby, 'rank', 'date'))
  temp_2 <- last_time %>% 
    select(c(groupby, 'rank_1', 'date'))
  temp_3 <- merge(temp_1, 
                  temp_2, 
                  by.x = c(groupby, 'rank'), 
                  by.y = c(groupby, 'rank_1'), 
                  all = TRUE)
  rm(temp_1, temp_2)
  temp_3 <- temp_3 %>%
    filter(!(is.na(date.x))) %>%
    mutate(diff_date = date.x - date.y) %>%
    select(c(groupby, 'date.x', 'rank', 'diff_date'))
  last_time <- merge(last_time, 
                     temp_3, 
                     by.x = c(groupby, 'date', 'rank'), 
                     by.y = c(groupby, 'date.x', 'rank')) %>%
    select(c('record', 'diff_date')) 
  rm(temp_3)
  return(last_time)
}


for(entity in entities){
  assign(paste0(entity, '_last_day_since'), days_since_last(entity))
}

for(comb in combinations){
  assign(paste0(comb[1], '_', comb[2]), days_since_last(comb))
}


last_time <- data.frame(record = df$record)
last_time$diff_date <- -1
for(entity in entities){
  temp <- get(paste0(entity, '_last_day_since'))
  last_time <- merge(last_time, temp, by = 'record', suffixes=c('', paste0('.', entity)))
}
for(comb in combinations){
  temp <- get(paste0(comb[1], '_', comb[2]))
  last_time <- merge(last_time, temp, by = 'record', suffixes=c('', paste0('.', paste0(comb[1], '_', comb[2]))))
}
for(entity in entities){
  rm(list = paste0(entity, '_last_day_since'))
}
rm(entity)
for(comb in combinations){
  rm(list = paste0(comb[1], '_', comb[2]))
}
rm(comb)
last_time <- last_time %>%
  select(-diff_date)

write.csv(last_time, 'DataLast.csv')

#RiskTable

#temp <- df1 %>%
  #group_by(ssn) %>%
  #summarise(risk = mean(fraud_label))

risk_table <- function(we, entity){
  temp <- we %>%
    group_by_(entity) %>%
    summarise(risk = mean(fraud_label))
  return(temp)
}

for(entity in entities){
  assign(paste0('rt_', entity), risk_table(df1, entity))
}

risk_table <- function(we, entity){
  temp <- we %>%
    group_by_(entity[1], entity[2]) %>%
    summarise(risk = mean(fraud_label))
  return(temp)
}

for(comb in combinations){
  assign(paste0('rt_', comb[1], comb[2]), risk_table(df1, comb))
}

entities <- c('ssn','fulladdress', 'nameDOB', 'homephone')

setDT(df)
setDT(rt_ssn)
#risk_all <- df[rt_ssn, on = 'ssn'] # can't be used due to unbalanced data

risk_all <- merge(x = df, y = rt_ssn, by = 'ssn',all.x = TRUE)
#risk_all$risk[is.na(risk_all$risk)] = 0
sum(is.na(risk_all$risk)) # finding total missing values 
names(risk_all)[names(risk_all) == "risk"] = "risk_ssn"

setDT(rt_fulladdress)
risk_all <- merge(x = risk_all, y = rt_fulladdress, by = 'fulladdress',all.x = TRUE)
#risk_all$risk.y[is.na(risk_all$risk.y)] = 0
#sum(is.na(risk_all$risk.y)) # finding total missing values 
names(risk_all)[names(risk_all) == "risk"] = "risk_fulladdress"

setDT(rt_nameDOB)
#risk_all <- risk_all[rt_nameDOB, on = 'nameDOB']
risk_all <- merge(x = risk_all, y = rt_nameDOB, by = 'nameDOB',all.x = TRUE)
#risk_all$risk[is.na(risk_all$risk)] = 0
#sum(is.na(risk_all$risk))
names(risk_all)[names(risk_all) == "risk"] = "risk_nameDOB"

setDT(rt_homephone)
#risk_all <- risk_all[rt_homephone, on = 'homephone']
risk_all <- merge(x = risk_all, y = rt_homephone, by = 'homephone',all.x = TRUE)
#risk_all$risk[is.na(risk_all$risk)] = 0
#sum(is.na(risk_all$risk))
names(risk_all)[names(risk_all) == "risk"] = "risk_homephone"

setDT(rt_ssnfulladdress)
risk_all <- merge(x = risk_all, y = rt_ssnfulladdress, by = c('ssn','fulladdress'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_ssnfulladdress"

setDT(rt_ssnnameDOB)
#risk_all <- risk_all[rt_ssnnameDOB, on = c('ssn','nameDOB')]
risk_all <- merge(x = risk_all, y = rt_ssnnameDOB, by = c('ssn','nameDOB'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_ssnnameDOB"

setDT(rt_ssnhomephone)
#risk_all <- risk_all[rt_ssnhomephone, on = c('ssn','homephone')]
risk_all <- merge(x = risk_all, y = rt_ssnhomephone, by = c('ssn','homephone'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_ssnhomephone"

setDT(rt_ssnfirstname)
#risk_all <- risk_all[rt_ssnfirstname, on = c('ssn','firstname')]
risk_all <- merge(x = risk_all, y = rt_ssnfirstname, by = c('ssn','firstname'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_ssnfirstname"

setDT(rt_ssnlastname)
#risk_all <- risk_all[rt_ssnlastname, on = c('ssn','lastname')]
risk_all <- merge(x = risk_all, y = rt_ssnlastname, by = c('ssn','lastname'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_ssnlastname"

setDT(rt_nameDOBhomephone)
#risk_all <- risk_all[rt_nameDOBhomephone, on = c('nameDOB','homephone')]
risk_all <- merge(x = risk_all, y = rt_nameDOBhomephone, by = c('nameDOB','homephone'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_nameDOBhomephone"

setDT(rt_fulladdresshomephone)
#risk_all <- risk_all[rt_fulladdresshomephone, on = c('fulladdress','homephone')]
risk_all <- merge(x = risk_all, y = rt_fulladdresshomephone, by = c('fulladdress','homephone'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_fulladdresshomephone"

setDT(rt_fulladdressnameDOB)
#risk_all <- risk_all[rt_fulladdressnameDOB, on = c('fulladdress','nameDOB')]
risk_all <- merge(x = risk_all, y = rt_fulladdressnameDOB, by = c('fulladdress','nameDOB'),all.x = TRUE)
names(risk_all)[names(risk_all) == "risk"] = "risk_fulladdressnameDOB"

#colnames(risk_all) <- c(colnames(risk_all)[1:9], 'ssn_risk', 'fulladdress_risk', 'nameDOB_risk', 'homephone_risk',
   #                     'ssn_fulladdress_risk', 'ssn_nameDOB_risk', 'ssn_homephone_risk', 'ssn_firstname_risk', 'ssn_lastname_risk', 'nameDOB_homephone_risk', 'fulladdress_homephone_risk', 'fulladdress_nameDOB_risk')

summary(risk_all)
is.na(risk_all)
risk_all[is.na(risk_all)] = 0

temp <- risk_all %>%
  select(-c('ssn', 'fulladdress', 'nameDOB', 'homephone', 'date', 'fraud_label', 'firstname', 'lastname'))

write.csv(temp, 'DataRisk.csv')

last_time <- read.csv('DataLast.csv')
last_time <- last_time %>%
  select(-X)

#velocity <- read.csv('Data/all_data.csv')
#velocity <- velocity %>%
  #select(-X)

risk <- read.csv('DataRisk.csv')
risk <- risk %>%
  select(-X)

setDT(last_time)
setDT(risk)
#setDT(velocity)
variables <- last_time[risk, on = 'record']
#variables <- variables[velocity, on = 'record']

colnames(variables)

write.csv(variables, 'DataVariables.csv')

variables <- read.csv('DataVariables.csv')
