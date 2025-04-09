#!/usr/bin/env python
# coding: utf-8

# In[160]:


# make sure openpyxl is installed for working with excel sheets in pandas
import pandas as pd


# In[161]:


# some constants that will allow us to access our excel files without typing in the whole filename
FOLDER_RAW_DATA = './data/raw/'
FOLDER_OUTPUT = './data/output/'
EXCEL_FILETYPE = '.xlsx'
FILE_CRH_PIPELINE = 'Consumer Retail and Healthcare Pipeline Edited'
FILE_BS_PIPELINE = 'Business Services Pipeline Edited'
FILE_PE_CONTACTS = 'Private Equity Contacts Edited'
FILE_CONTACTS = 'Contacts'
FILE_EVENTS = 'Events'
FILE_PE_COMPANIES = 'PE Comps Edited'
LAP_DINNER = "Leaders and Partners Dinner"
MARKET_RECAP = "2019 Market Re-Cap"


# In[162]:


# Step 1: Create the Companies table.

# overview - do a bit of processing match the Pipeline files as well as possible
# major overhaul of the Private Equities company file to get it to match
# get the unique invest. bank companies.


# In[163]:


# load in our necessary files for this section
crh_pipe = pd.read_excel(FOLDER_RAW_DATA + FILE_CRH_PIPELINE + EXCEL_FILETYPE)
bs_pipe = pd.read_excel(FOLDER_RAW_DATA + FILE_BS_PIPELINE + EXCEL_FILETYPE)
pe_comp = pd.read_excel(FOLDER_RAW_DATA + FILE_PE_COMPANIES + EXCEL_FILETYPE)


# In[164]:


# consumer retail and healthcare doesn't have too much we need to do to process the data
# just create a new dataframe of only the columns we want to keep in the Companies table
# these properties exclude all of the information about the deals and banking information, just providing the direct company information. 
companies_crh = crh_pipe[['Company Name', 'Project Name', 'Date Added', 'LTM Revenue', 'LTM EBITDA', 'Vertical', 'Sub Vertical', 'Current Owner', 'Business Description', 'Lead MD']]


# In[165]:


# the business services pipeline needs a little bit of processing:
# I want to get the most recent EBITDA information possible for one column instead of the sparsely filled yearly ebitda found in this sheet
companies_bs = bs_pipe.copy()
# to get the most recent EBITDA we'll use the back-fill method to check across the columns
# we just need to give the order of columns we want to check in order left to right
companies_bs['LTM EBITDA'] = companies_bs[['2018E EBITDA', '2017A/E EBITDA', '2016A EBITDA', '2015A EBITDA', '2014A EBITDA']].bfill(axis=1).iloc[:, 0]
# test sheet to make sure that worked
# companies_bs.to_excel(FOLDER_OUTPUT + "test2" + EXCEL_FILETYPE, index=False)

# and now with the EBITDA processed the columns we want to keep are the same as the crh pipeline
companies_bs = companies_bs[['Company Name', 'Project Name', 'Date Added', 'LTM Revenue', 'LTM EBITDA', 'Vertical', 'Sub Vertical', 'Current Owner', 'Business Description', 'Lead MD']]


# In[166]:


# next we can grab the investment banks column from each of our pipeline files. There's not much information, but we need the names of these companies in the system to assign contacts to.
# sometimes there are multiple banks in these cells, split by , / or ;
# to do this we're going to have to use regex to split by the 3 options instead of just one character
banks_bs = bs_pipe.copy()
banks_bs = banks_bs[['Invest. Bank']]
banks_crh = crh_pipe.copy()
banks_crh = banks_crh[['Invest. Bank']]
banks = pd.concat([banks_bs, banks_crh])
# get rid of any blank values
banks = banks.dropna()
# and get rid of 'n/a' we don't want that
banks = banks[~banks['Invest. Bank'].str.contains('N/A')]
# print(banks)
# time to rename the column
banks.columns = ['Company Name']
# split the company names into lists with the dividers , / and ;
banks['Company Name'] = banks['Company Name'].str.split(r',\s*|;\s*|/\s*')
# use explode to turn the lists into their own separate rows
banks = banks.explode('Company Name')
# get rid of the duplicates
banks = banks.drop_duplicates()

# finally the only information we have about the banks is that they are, in fact, banks.
# so let's add that label to their Vertical column
banks['Vertical'] = 'Invest. Bank'


# In[167]:


# finally we need the companies from the Public Equities

companies_pe = pe_comp.copy()
# reduce to just the columns we want
companies_pe = companies_pe[['Company Name', 'Website', 'AUM (Bns)', 'Sectors', 'Sample Portfolio Companies', 'Comments']]
# make this title cased just for cleanliness
companies_pe['Sectors'] = companies_pe['Sectors'].str.title()
# create a column for the vertical
companies_pe[['Vertical']] = 'Private Equity'
# we have to do a little bit of processing changing the Sectors into Subverticals so it syncs up a little better
# if you want to be safe here before parsing the strings we could fillna, but in this case everything is filled already
# split the sectors into line breaks
companies_pe[['Sub Vertical', 'Sub Vertical (Secondary)']] = companies_pe['Sectors'].str.split('\n', n=1, expand=True)
# remove the little dash from it
companies_pe['Sub Vertical'] = companies_pe['Sub Vertical'].str.removeprefix('- ').str.strip()
companies_pe['Sub Vertical (Secondary)'] = companies_pe['Sub Vertical (Secondary)'].str.removeprefix('- ').str.strip()
# now reduce to just the columns we want
companies_pe = companies_pe[['Company Name', 'Website', 'AUM (Bns)', 'Vertical', 'Sub Vertical', 'Sub Vertical (Secondary)', 'Sample Portfolio Companies', 'Comments']]
# print(companies_pe)


# In[168]:


# ok now it's time to combine all of our different companies.
companies = pd.concat([companies_crh, companies_bs, banks, companies_pe])
# luckily we've set it up so there's no duplicates, but typically I would want to doublecheck there's no duplicate companies here
# but also that would require some way of identifying if duplicate company names weren't other companies
# companies.drop_duplicates('Company Name') 
# print(companies)
# I want an id column to appear at the front, so instead of creating an id column and reordering the columns we'll use Insert
companies = companies.dropna(subset=['Company Name'])

# NOW THIS CODE IS ON PAUSE
# although I thought we were in a good spot to finalize the order and everything here, we actually first have to build out all the contacts
# some of the contacts have some hidden companies that we didn't manage to get in, so I'll completing the company sheet at the end of step 2
# companies.insert(loc=0, column='id_company', value = companies.groupby('Company Name').ngroup())
# companies.to_excel(FOLDER_OUTPUT + "companies_concat_order" + EXCEL_FILETYPE, index=False)
# companies = companies.sort_values('id_company')
# print(companies)

# companies.to_excel(FOLDER_OUTPUT + "companies" + EXCEL_FILETYPE, index=False)


# In[169]:


# step 2: create the Contacts table

# get all of the contacts from the contacts sheet
# get the contacts from the Private Equity contacts
# get the bankers from the Invest. Bank companies in the BS and CR&H Pipelines
# validate that the events don't have any contacts missing

# when all of them are gathered need to join on the companies table to get the company id instead of the company name
# and need to an assign a contact id number for each person


# In[170]:


# import the other sheets necessary for this section

contacts_contacts = pd.read_excel(FOLDER_RAW_DATA + FILE_CONTACTS + EXCEL_FILETYPE)
contacts_pe = pd.read_excel(FOLDER_RAW_DATA + FILE_PE_CONTACTS + EXCEL_FILETYPE)
# we'll get the bankers from the pipelines later, but we already have those dataframes ready to go
# need to import the marketing events to validate the contacts in this section
events_lap = pd.read_excel(FOLDER_RAW_DATA + "Events" + EXCEL_FILETYPE, sheet_name=LAP_DINNER)
events_market = pd.read_excel(FOLDER_RAW_DATA + "Events" + EXCEL_FILETYPE, sheet_name=MARKET_RECAP)


# In[171]:


# a quick change for the contacts, to be congruent with the other data changing "firm" to "Company", and group seems to be the same as vertical
contacts_contacts = contacts_contacts.rename(columns={'Firm': 'Company', 'Group': 'Vertical'})


# In[172]:


# converting the bankers from the pipelines is going to look very similar to how we got the banks
bankers = crh_pipe.copy()
bankers = bankers[['Banker', 'Banker Email', 'Banker Phone Number', 'Company Name']]
# there actually aren't any bankers in the Business Services Pipeline, so we're skipping this, but leaving this code to remind myself I considered it
# bankers_bs = bs_pipe.copy()
# bankers_bs = bankers_bs[['Banker', 'Banker Email', 'Banker Phone Number', 'Company Name']]
# bankers = pd.concat([banks_bs, banks_crh])
# get rid of any blank values
bankers = bankers.dropna()
# time to rename the columns
bankers.columns = ['Name', 'E-mail', 'Phone', 'Company']
# split the company names into lists with the dividers , / and ; There's also an instance where there's a : instead of a ;
bankers['Company'] = bankers['Company'].str.split(r',\s*|;\s*|/\s*|:\s*')
# use explode to turn the lists into their own separate rows
bankers = bankers.explode('Company')
# get rid of the duplicates
bankers = bankers.drop_duplicates('E-mail')

# finally the only information we have about the banks is that they are, in fact, banks.
# so let's add that label to their Vertical column
bankers['Vertical'] = 'Invest. Bank'

# bankers.to_excel(FOLDER_OUTPUT + "test3" + EXCEL_FILETYPE, index=False)


# In[173]:


# now we can join our three sets of contacts
contacts_all = pd.concat([contacts_contacts, contacts_pe, bankers])

print(contacts_all)
# for a sanity check doublecheck how many duplicates there are
# email is our unique identifier across these contacts
duplicate_contacts = contacts_all['E-mail'].duplicated().sum()
print('duplicates', duplicate_contacts)

# there are duplicates so let's drop those
contacts_all = contacts_all.drop_duplicates('E-mail')


# In[174]:


# to see if we need any more inclusions in the contacts, we need to check the events
# make sure all event attendees are already accounted for
event_1_missing = events_lap[~events_lap['E-mail'].isin(contacts_all['E-mail'])]
event_2_missing = events_market[~events_market['E-mail'].isin(contacts_all['E-mail'])]
print(f'{len(event_1_missing)} contacts missing from first event')
print(f'{len(event_2_missing)} contacts missing from second event')


# In[175]:


# FINISHING STEP 1.5
# now that we have the contacts list built out we have to doublecheck that every contact has its company accounted for.
# spoiler: they don't.
companies_missing = contacts_all[~contacts_all['Company'].isin(companies['Company Name'])]
companies_missing = companies_missing[['Company']]
companies_missing = companies_missing.drop_duplicates('Company')
print(f'{len(companies_missing)} companies are missing from the companies sheet.')

# so we need to add those to the companies
companies_missing.columns = ['Company Name']
print(companies_missing)

companies = pd.concat([companies, companies_missing])


# In[176]:


# as a sanity check doublecheck there are none missing now.
companies_missing_2 = contacts_all[~contacts_all['Company'].isin(companies['Company Name'])]
companies_missing_2 = companies_missing_2[['Company']]
companies_missing_2 = companies_missing_2.drop_duplicates('Company')
print(f'{len(companies_missing_2)} companies are missing from the companies sheet.')


# In[177]:


# FINISHING STEP 1.5
# with all of the companies from the contacts added we can complete the company sheet
companies.insert(loc=0, column='id_company', value = companies.groupby('Company Name').ngroup())
companies.to_excel(FOLDER_OUTPUT + "companies_concat_order" + EXCEL_FILETYPE, index=False)
companies = companies.sort_values('id_company')
print(companies)

companies.to_excel(FOLDER_OUTPUT + "1_companies" + EXCEL_FILETYPE, index=False)


# In[178]:


# now that we know we have every contact we need our final steps are to create unique ids for the contacts
# and to associate the contact to the company id instead of its name
contacts_all.insert(loc=0, column='id_contact', value = contacts_all.groupby('E-mail').ngroup())


# In[179]:


contacts_all = contacts_all.merge(companies[['Company Name', 'id_company']], left_on='Company', right_on='Company Name', how='left')
contacts_all = contacts_all.sort_values('id_contact')
print(contacts_all)


# In[180]:


# step 2 is finished, we can export our contacts table now
contacts_all.to_excel(FOLDER_OUTPUT + "2_contacts" + EXCEL_FILETYPE, index=False)


# In[181]:


# STEP 3 Deals
# so for this one we just need to get the columns from the Pipeline files relevant to any deals going on
# Business Services don't have any bankers to associate, but for each of these deals we should associate 
# 1. the company id, 
# 2. the bank id,
# 3. the banker id


# In[182]:


# convert the deals from the pipelines
deals_crh = crh_pipe.copy()
deals_crh = deals_crh[['Company Name', 'Invest. Bank', 'Banker', 'Sourcing', 'Transaction Type', 'Enterprise Value', 'Est. Equity Investment', 'Status', 'Portfolio Company Status', 'Active Stage', 'Passed Rationale']]
# there actually aren't any bankers in the Business Services Pipeline, so we're skipping this, but leaving this code to remind myself I considered it
deals_bs = bs_pipe.copy()
# quick column name change to match the other file
deals_bs = deals_bs.rename(columns={'Equity Investment Est.': 'Est. Equity Investment'})
deals_bs = deals_bs[['Company Name', 'Invest. Bank', 'Banker', 'Sourcing', 'Transaction Type', 'Enterprise Value', 'Est. Equity Investment', 'Status']]
deals = pd.concat([deals_crh, deals_bs])
print(deals)


# In[183]:


# now we need to add the existing indexing
deals = deals.merge(companies[['Company Name', 'id_company']], on='Company Name', how='left')

# this one is a little awkward, we don't have banker email across both sheets so we have to match names which might not be intended behavior
# but it's the best we've got
deals = deals.merge(contacts_all[['Name', 'id_contact']], left_on='Banker', right_on='Name', how='left')
deals = deals.rename(columns={'id_contact':'banker_id', 'id_company':'deal_company_id'})
# finally we need the banking company id
deals = deals.merge(companies[['Company Name', 'id_company']], left_on='Invest. Bank', right_on='Company Name', how='left')
deals = deals.rename(columns={'id_company':'banking_company_id'})

print(deals)


# In[184]:


# no need to create indices for deals because they don't need to be referenced in the same way as the companies and contacts

# so step 3 is finished, we can export our deals table now
deals.to_excel(FOLDER_OUTPUT + "3_deals" + EXCEL_FILETYPE, index=False)


# In[185]:


# STEP 4: Marketing 

# cross reference all of the marketing attendees with their contact number


# In[186]:


# create a column for the event name
events_lap['Event'] = LAP_DINNER
events_market['Event'] = MARKET_RECAP

# combine the data frames by concatenating them
events = pd.concat([events_lap, events_market])
# could create unique indices for unique attendees based on their email and name.
# but this is unnecessary, we don't need unique ids for attendances, there will be no primary key
# combined_events['attendee_id'] = combined_events.groupby(['E-mail', 'Name']).ngroup()
# the issues with groupby here are - using different emails for the same person or spelling name differently
print(events)


# In[187]:


# merge the email of the attendee with their unique contact id 
events = events.merge(contacts_all[['E-mail', 'id_contact']], on='E-mail', how='left')

print(events)


# In[188]:


# and because we will not be indexing the event attendance step 4 is done

# this is optional, but I do like sorting here by name so that you can see their attendance for each event
events = events.sort_values('Name')

# export the file
events.to_excel(FOLDER_OUTPUT + "4_marketing_participants" + EXCEL_FILETYPE, index=False)


# In[189]:


# STEP 5 - Choice Fields
# although this is optional, it seems easy enough to create a list of all the fields we want so I'm going to go ahead and do that

# we'll build up a text variable and write it all at once to a text file
text = 'CHOICE FIELD VALUES FOR EACH TABLE\n'


# In[190]:


# table 1
verticals = set(companies['Vertical'])
sub_vert_prime = set(companies['Sub Vertical'])
sub_vert_sec = set(companies['Sub Vertical (Secondary)'])
sub_verticals = sub_vert_prime | sub_vert_sec

# table 2
contact_method = set(contacts_all['Preferred Contact Method']) 

# table 3
status = set(deals['Status']) 
port_comp_status = set(deals['Portfolio Company Status']) 
stage = set(deals['Active Stage']) 

# table 4
attendee_status_values = set(events['Attendee Status'])
attendee_events = set(events['Event'])


# In[191]:


text += f'\nVertical: \n{verticals}\n'
text += f'\nSub Vertical: \n{sub_verticals}\n'
text += f'\nPreferred Contact Method: \n{contact_method}\n'
text += f'\n(Deal) Status: \n{status}\n'
text += f'\nPortfolio Company Status: \n{port_comp_status}\n'
text += f'\nActive Stage: \n{stage}\n'
text += f'\nAttendee Status: \n{attendee_status_values}\n'
text += f'\nMarketing Events: \n{attendee_events}\n'


# In[192]:


# build the text file output
with open(FOLDER_OUTPUT+"5_choice_fields.txt", "w") as f:
    f.write(text)

