### Responsiveness = (Time Accepted or Time Declined - Time Dispatched ) - if rejected: (Time Submitted - time rejected) else:(Time submitted - Initial appointment date - Time accepted)


import pandas as pd
import time
from datetime import datetime

df = pd.read_csv('Partnerkpis7.0.csv')


pattern = '%m/%d/%Y %I:%M %p'
date_time = '8/4/2017 4:58 PM'
epoch = int(time.mktime(time.strptime(date_time, pattern)))

# string all the dates!

df['Time Dispatched'] = df['Time Dispatched'].astype(str)

df['Time Accepted'] = df['Time Accepted'].astype(str)

df['Time Declined'] = df['Time Declined'].astype(str)

df['Initial Appointment Date'] = df['Initial Appointment Date'].astype(str)

df['Time Submitted'] = df['Time Submitted'].astype(str)

df['Time Rejected'] = df['Time Rejected'].astype(str)

#count all the dispatches!

df["Dispatches_Total"] = df.groupby('Selected Partner Name')['Selected Partner Name'].transform('count')


#FSDs stright up accepted

df_clean =df[df['Time Dispatched']!='nan']

df_cleanAcpt =df_clean[(df_clean['Time Accepted']!='nan') & (df_clean['Time Declined']=='nan')]


#FSDs declined

df_declined = df_clean[df_clean['Time Declined']!='nan']

#FSDs that weren't accepted or declined

df_dispatched1 = df[df['Time Accepted'] == 'nan']

df_dispatched2 = df_dispatched1[df_dispatched1['Time Dispatched'] != 'nan']

df_dispatched = df_dispatched2[df_dispatched2['Time Declined'] == 'nan']




#FSDs that were accepted

df_cleanAcpt['Time Dispatched'] = df_cleanAcpt['Time Dispatched'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))


df_cleanAcpt['Time Accepted'] = df_cleanAcpt['Time Accepted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

### declined FSDs

df_declined['Time Dispatched'] = df_declined['Time Dispatched'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_declined['Time Declined'] = df_declined['Time Declined'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_declined["Time_To_Accept"] = df_declined['Time Declined'] - df_declined['Time Dispatched']

df_declined["Time_To_Accept"] = df_declined["Time_To_Accept"].apply(lambda x: x // 86400)

#Accepted FSDS

df_cleanAcpt["Time_To_Accept"] = df_cleanAcpt['Time Accepted'] - df_cleanAcpt['Time Dispatched']

#df_clean["Time_To_Decline"] = df_clean['Time Declined'] - df_clean['Time Dispatched']

df_cleanAcpt["Time_To_Accept"] = df_cleanAcpt["Time_To_Accept"].apply(lambda x: x // 86400)

#FSDs that weren't accepted or declined

epoch_time = int(time.time())

df_dispatched['Time Dispatched'] = df_dispatched['Time Dispatched'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_dispatched["Time_To_Accept"] = epoch_time - df_dispatched['Time Dispatched']

df_dispatched["Time_To_Accept"] = df_dispatched["Time_To_Accept"].apply(lambda x: x // 86400)


frames1 = [df_cleanAcpt,df_declined,df_dispatched]

df_acpt_Dec = pd.concat(frames1)

df_clean_mean = df_acpt_Dec.join(df_acpt_Dec.groupby('Selected Partner Name')["Time_To_Accept"].mean().round(decimals=0, out=None), on='Selected Partner Name', rsuffix='_mean')

df_mean_accept = df_clean_mean.groupby('Selected Partner Name')["Time_To_Accept","Dispatches_Total"]


df_mean_accept_csv = df_mean_accept.mean().round(decimals=0, out=None).reset_index()

df_mean_accept_csv['Time_To_Accept'] = df_mean_accept_csv['Time_To_Accept'].astype(int)

df_mean_accept_csv['Dispatches_Total'] = df_mean_accept_csv['Dispatches_Total'].astype(int)

df_mean_accept_csv.to_csv('Partner_Accept_Time.csv', index=False)


###################################################################

#### Second part of the analysis how long it took stuff to get done

# time submitted - (appointment time -time accpeted) - (time rejected - time accepted)

# Case 1 accpted & appointment & not rejected & Submitted
# best case sub

df_appoint =df_clean[(df_clean['Initial Appointment Date']!='nan') & (df_clean['Time Rejected']=='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']!='nan')]

#df_test2 = df_appoint.groupby('Selected Partner Name')


df_appoint['Initial Appointment Date'] = df_appoint['Initial Appointment Date'].apply(lambda x: int(time.mktime(time.strptime(x,'%m/%d/%Y' ))))

df_appoint['Time Accepted'] = df_appoint['Time Accepted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_appoint['Time Submitted'] = df_appoint['Time Submitted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

### testing this part

df_appoint = df_appoint[df_appoint['Initial Appointment Date'] <= df_appoint['Time Submitted']]

df_appoint["Time_To_Sub"] = df_appoint['Time Submitted'] - df_appoint['Initial Appointment Date']

df_appoint["Time_To_Sub"] = df_appoint["Time_To_Sub"].apply(lambda x: ((abs(x)+x)/2)//86400)

# case 2 accepted & no appointment & not rejected & submitted
# the simple case sub

df_accpt_sub =df_clean[(df_clean['Initial Appointment Date']=='nan') & (df_clean['Time Rejected']=='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']!='nan')]

df_accpt_sub['Time Accepted'] = df_accpt_sub['Time Accepted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_accpt_sub['Time Submitted'] = df_accpt_sub['Time Submitted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_accpt_sub["Time_To_Sub"] = df_accpt_sub['Time Submitted'] - df_accpt_sub['Time Accepted']

df_accpt_sub["Time_To_Sub"] = df_accpt_sub["Time_To_Sub"].apply(lambda x: x // 86400)

#case 3
#still in flight with appointment

df_appoint_act =df_clean[(df_clean['Initial Appointment Date']!='nan') & (df_clean['Time Rejected']=='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']=='nan')]

df_appoint_act['Initial Appointment Date'] = df_appoint_act['Initial Appointment Date'].apply(lambda x: int(time.mktime(time.strptime(x,'%m/%d/%Y' ))))

df_appoint_act["Time_To_Sub"] = epoch_time - df_appoint_act['Initial Appointment Date']

df_appoint_act["Time_To_Sub"] = df_appoint_act["Time_To_Sub"].apply(lambda x: ((abs(x)+x)/2)//86400)

#### Case 4 in flight with no appointment

df_no_app =df_clean[(df_clean['Initial Appointment Date']=='nan') & (df_clean['Time Rejected']=='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']=='nan')]

df_no_app['Time Accepted'] = df_no_app['Time Accepted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_no_app["Time_To_Sub"] = epoch_time - df_no_app['Time Accepted']

df_no_app["Time_To_Sub"] = df_no_app["Time_To_Sub"].apply(lambda x: ((abs(x)+x)/2)//86400)



#### Rejected section

#Case 5 rejected but accepted
df_rej_app =df_clean[(df_clean['Time Rejected']!='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']!='nan')]


df_rej_app['Time Rejected'] = df_rej_app['Time Rejected'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app['First Submit Time'] = df_rej_app['First Submit Time'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app['Time Submitted'] = df_rej_app['Time Submitted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app['Time Accepted'] = df_rej_app['Time Accepted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app["Time_To_Sub"] = df_rej_app['First Submit Time']-df_rej_app['Time Accepted']

df_rej_app["Time_To_Sub"] = df_rej_app["Time_To_Sub"].apply(lambda x: x // 86400)

#case 6 rejected and Submitted again

df_rej_app2 =df_clean[(df_clean['Time Rejected']!='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']!='nan') & (df_clean['Time Submitted']!= df_clean['First Submit Time'])]

df_rej_app2['First Submit Time'] = df_rej_app2['First Submit Time'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app2['Time Submitted'] = df_rej_app2['Time Submitted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app2["Time_To_Sub"] = df_rej_app2['Time Submitted']-df_rej_app2['First Submit Time']

df_rej_app2["Time_To_Sub"] = df_rej_app2["Time_To_Sub"].apply(lambda x: x // 86400)

# Case 7 rejected, but never submitted again

df_rej_app3 =df_clean[(df_clean['Time Rejected']!='nan') & (df_clean['Time Accepted']!='nan') & (df_clean['Time Submitted']!='nan') & (df_clean['Time Submitted']== df_clean['First Submit Time'])]

df_rej_app3['First Submit Time'] = df_rej_app3['First Submit Time'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app3['Time Submitted'] = df_rej_app3['Time Submitted'].apply(lambda x: int(time.mktime(time.strptime(x, pattern))))

df_rej_app3["Time_To_Sub"] = epoch_time - df_rej_app3['Time Submitted']

df_rej_app3["Time_To_Sub"] = df_rej_app3["Time_To_Sub"].apply(lambda x: x // 86400)


#Setting it up!!!


frames2 = [df_appoint,df_accpt_sub,df_appoint_act,df_no_app,df_rej_app,df_rej_app2,df_rej_app3]

df_submit = pd.concat(frames2)

df_clean_mean_sub = df_submit.join(df_submit.groupby('Selected Partner Name')["Time_To_Sub"].mean().round(decimals=0, out=None), on='Selected Partner Name', rsuffix='_mean')

df_mean_sub = df_clean_mean_sub.groupby('Selected Partner Name')["Time_To_Sub",'Dispatches_Total']


df_mean_sub_csv = df_mean_sub.mean().round(decimals=0, out=None).reset_index()

df_mean_sub_csv['Time_To_Sub'] = df_mean_sub_csv['Time_To_Sub'].astype(int)

#df_mean_sub_csv['Dispatches_Total'] = df_mean_sub_csv['Dispatches_Total'].astype(int)

df_mean_sub_csv.to_csv('Partner_Submit_Time.csv', index=False)


#######


### the circle is complete


######


frames3 = [df_mean_accept_csv,df_mean_sub_csv]

df_tots = pd.concat(frames3)

df_totals = df_tots.groupby('Selected Partner Name')["Time_To_Accept",'Time_To_Sub','Dispatches_Total']


df_totals_sum = df_totals.mean().round(decimals=0, out=None).reset_index()

df_totals_sum = df_totals_sum.fillna(0)


df_totals_sum['Unified Score'] = df_totals_sum['Time_To_Accept'] + df_totals_sum['Time_To_Sub']

df_totals_sum['Time_To_Sub'] = df_totals_sum['Time_To_Sub'].astype(int)

df_totals_sum['Time_To_Accept'] = df_totals_sum['Time_To_Accept'].astype(int)


df_totals_sum['Unified Score'] = df_totals_sum['Unified Score'].astype(int)



df_totals_sum = df_totals_sum[['Selected Partner Name','Time_To_Accept','Time_To_Sub','Unified Score','Dispatches_Total']]


df_totals_sum_sort = df_totals_sum.sort_values('Unified Score',ascending=False)

df_totals_sum_sort.to_csv('Partner_Scores.10.31.17.csv', index=False)







#### the end
