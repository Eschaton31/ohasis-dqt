install.packages("dplyr")
install.packages("writexl")
install.packages("readxl")
install.packages("haven")
library(readxl)
library(haven)
play_data.orig<-read_xlsx("C:\\Users\\FrancesGiulYap\\Downloads\\play_data.xlsx")
play_data<-play_data.orig

variable.names<-c('id','last_name','first_name','middle_name','sex','bdate')
colnames(play_data)[colnames(play_data)=='id']<-'id'
colnames(play_data)[colnames(play_data)=='last_name']<-'lastName'
colnames(play_data)[colnames(play_data)=='first_name']<-'firstName'
colnames(play_data)[colnames(play_data)=='middle_name']<-'middleName'
colnames(play_data)[colnames(play_data)=='sex']<-'sex'
colnames(play_data)[colnames(play_data)=='bdate']<-'bdate'

play_data$sex[is.na(play_data$sex)]<-'no data'
play_data$sex[play_data$sex=='Male']<-'M'
play_data$sex[play_data$sex=='MALE']<-'M'
play_data$sex[play_data$sex=='Feml']<-'F'

for (x in play_data[,2]){
  play_data[,2]=toupper(x)
}

MI<-substr(play_data$middleName,1,1)
first_last<-paste(play_data$lastName,play_data$firstName,sep=", ")
play_data$FULL_NAME<-paste(first_last,MI)

write_dta(play_data,file.path(getwd(),"verynewfile.dta"))

play_data$bdates<-substr(play_data$bdate,1,4)









