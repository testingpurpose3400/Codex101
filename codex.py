
# coding: utf-8

# In[157]:


########Getting data from 2 CSV formatted files ##########################
import pandas as pd
import dateutil
from random import *
from scipy import stats
import scipy as scipy
#date initialization for date filterring---
#fraction of data to pull--50%
fraction=.50
datefrom='2010-11-21 00:00:00'
dateto='2018-11-28 00:00:00'
#header for date 
table1datename="Date"
table2datename="Creation Date"
datevariable="Date"
#getting first Data from CSV files1
sub_data1 = pd.read_csv("sampleLHF2.csv",encoding="UTF-8")
#rename datevariable to common name "datevariable"
sub_data1.rename(columns={table1datename:datevariable}, inplace=True)
#converting date variable to datetime from string format
sub_data1[datevariable] = sub_data1[datevariable].apply(dateutil.parser.parse,dayfirst=False)
#filtering data with the given datefrom and dateto
mask = (sub_data1[datevariable] >= datefrom) & (sub_data1[datevariable] <= dateto)
#sub_data1=sub_data1.loc[mask]
#getting random data with fraction %
rnumber=randint(1,200) 
#sub_data1 = sub_data1.sample(frac=fraction, random_state=rnumber)
#Reseting index numbers
sub_data1=sub_data1.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
#getting second Data from CSV files
sub_data2=pd.read_csv("SAMPLEDATA2.csv",encoding="UTF-8")
#rename datevariable 
sub_data2.rename(columns={table2datename:datevariable}, inplace=True)
#converting date variable to datetime from string format
sub_data2[datevariable] = sub_data2[datevariable].apply(dateutil.parser.parse,dayfirst=False)
#filtering data with the given datefrom and dateto
mask = (sub_data2[datevariable] >= datefrom) & (sub_data2[datevariable] <= dateto)
sub_data2=sub_data2.loc[mask]
#getting random data with fraction %
sub_data2= sub_data2.sample(frac=fraction, random_state=rnumber)
#Reseting index numbers
sub_data2=sub_data2.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
print(sub_data1)
print(sub_data2)


# In[158]:


#Declaration for data collection purposes
#significance level alpha set to 5% acceptance level
#creating output dataframe
#Initializations
#for fitness test

alpha=.05
mycolumns = ['Header1/Value_1','Header2/Value_2', 'Tau-Correlation','P-Value']
output = pd.DataFrame(columns=mycolumns)
headerf1=list(sub_data1)
headerf1.remove(datevariable)
headerf2=list(sub_data2)
headerf2.remove(datevariable)
if 'Description' in headerf2:
    headerf2.remove('Description')
size1=len(headerf1)
size2=len(headerf2)
#print(size1)
#print(size2)
datasize=size1+size2
print(datasize)
rowcount=0
rowcount1=0
#checking for duplicate fields
if 'Description' in headerf2:
    headerf2.remove('Description')
#print(headertotal)
headertotal=headerf1 + list(set(headerf2) - set(headerf1))
chkdataheader=len(headertotal)
print(chkdataheader)
if(datasize==chkdataheader):
    print("Processing")
    #looping scenario
    #Reset rowcount
    #Getting table size
    #Split 1st field
        #Getting each value from 1st field
        #Splitting Second Field
            #Getting each value from second field
            #Filtering Data and Proccess possible Correlaion between 1st field values with second field values
            #Creating output
    for step in range(size1):
        #reset valuesindicator of field2 to 0
        rowcount2=0
        for step in range(size2):
            rawdata1=sub_data1.groupby([datevariable,headerf1[rowcount1]]).size().reset_index(name='Count')
            rawdata1=rawdata1.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    #removing duplicate values for looping purposes
            value1=headerf1[rowcount1]
            datalist1=rawdata1.drop_duplicates(subset=value1, keep="last")
            datalist1=datalist1.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    #print(rawdata1)
    #getting values for merging
            sortedrawdata=datalist1.groupby(value1).size().reset_index(name='Count')
    #getting number of items for number of loops
            sortedrawdatalenght1=datalist1.shape[0]
    #getting individual value for condition purpose
            value01=datalist1.iloc[:sortedrawdatalenght1,1]
            count1=0
            valuec=0
    #getting headername for condition  purpose,will be using 2nd index (headerval[1])
            headerval=list(rawdata1)
            for step in range(sortedrawdatalenght1):
    #For selecting specific Row Category value of first table
                fvaluedata1=rawdata1.loc[rawdata1[headerval[1]] == value01[count1]]
    #Reset Index
                fvaluedata1=fvaluedata1.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    ##################################################################################
    #getting specific header
                value2=headerf2[rowcount2]
    #grouping second field value by date and current header
                rawdata2=sub_data2.groupby([datevariable,value2]).size().reset_index(name='Count')
    #reset index    
                rawdata2=rawdata2.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    #removing duplicate values            
                datalist2=rawdata2.drop_duplicates(subset=value2, keep="last")
    #reset index
                datalist2=datalist2.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
                sortedrawdatalenght2=datalist2.shape[0]
                rawdata2header=list(rawdata2)
                sortedrawdata2=rawdata2.groupby(rawdata2header[1]).size().reset_index(name='Count')
                rawdatalenght2=sortedrawdata2.shape[0]
                headerval2=list(rawdata2)
                value2=datalist2.iloc[:rawdatalenght2,1]
                count2=0
                while sortedrawdatalenght2>count2:
                #########################for aggregation second value table ######################    
                #For selecting specific Row Category value of second table
                    fvaluedata2=rawdata2.loc[rawdata2[headerval2[1]] == value2[count2]]
                    fvaluedata2=fvaluedata2.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
                    right=fvaluedata1
                    left=fvaluedata2
############################################################################################# 
                    mergeddata=pd.merge(right, left, on=datevariable, how='inner')
                    #mergeddata=mergeddata.fillna(0)
                    #print(headerval[1]) df.dropna()
                    #print(headerval2[1])   
                    #kendall=mergeddata[['Count_x','Count_y']].corr(method='kendall', min_periods=1)
            #Function to get Tau value and 2 sided P_value-----
                    tau, p_value = stats.kendalltau(mergeddata[['Count_x']], mergeddata[['Count_y']])
                    #print(mergeddata)
                    if(p_value<=alpha):
                        Item1=headerval[1]+"/"+str(value01[count1])
                        Item2=headerval2[1]+"/"+str(value2[count2])
                        #adding value to final output
                        value=pd.DataFrame([[Item1,Item2,tau,p_value]],columns=mycolumns)
                        output=output.append(value, ignore_index=True)
                    count2=count2+1
                    valuec=valuec+1
                count1=count1+1
            rowcount2=rowcount2+1    
        rowcount1=rowcount1+1
    #removing No value "NAN" correlation
    output=output.dropna(how='any')
    #reset index
    output=output.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    print(output)
else:
    print("Data Invalid, has duplicate fields")


    
    


# In[159]:


my_df = pd.DataFrame(output)
my_df.to_csv('Data.csv', index=False, header=True)

