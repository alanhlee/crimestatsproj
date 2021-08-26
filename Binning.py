import pandas as pd
import pyodbc

server = 'britemssql.ce0lltgngjdb.us-east-1.rds.amazonaws.com,1433'
database = 'RPT_DM'
username = 'rpt_user'
password = 'rpt_user'

#Create a connection
cnxn = (pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server+';DATABASE='+database+';UID=' + username+';PWD=' + password))

binning_query = "select distinct acl.crime_category, ben_data.lefted, ben_data.crime_category as crime_category_original from dbo.Dallas acl left outer join(select distinct left(CRIME_CATEGORY_ORIGINAL, REPLACE(charindex('|', CRIME_CATEGORY_ORIGINAL), 0, 9999)-1) as lefted, CRIME_CATEGORY from dbo.ALL_CRIME_ACTUAL ac where city='Dallas' and crime_category is not null) ben_data on acl.crime_category=ben_data.lefted where ben_data.crime_category is null order by acl.crime_category"

binning_data = pd.read_sql(binning_query, cnxn, columns=['crime_category'])

update_statements = []
count = 0
total_rows = len(binning_data)

#Search for strings containing same category descriptions and basket them

def category_basket(common_str, master_category):
  global binning_data
  global count

  mask = binning_data['crime_category'].str.contains(common_str, regex=False, case=False)

  crime_basket = binning_data[mask]

  crime_basket['crime_category'] = crime_basket['crime_category'].apply(lambda x: "'" + str(x) + "'")

  count += len(crime_basket)

  #filter out ones basketed
  binning_data = binning_data[~mask]

  update_statements.append(f'''update ALL_CRIME_ACTUAL set crime_category = '{master_category}' where crime_category_original in ({', '.join(crime_basket['crime_category'])})''')

  #return (update_statements, count)
  
#Fire Arms Basket
category_basket('firearm', 'WEAPON LAW VIOLATIONS')
category_basket('weapon', 'WEAPON LAW VIOLATIONS')
category_basket('arson', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('desecration', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('graffiti', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('tamper', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('assault', 'ASSAULT OFFENSES')
category_basket('CRIM MISCHIEF <$100', 'ALL OTHER OFFENSES')
category_basket('CRIMINAL MISCHIEF > OR EQUAL $30K<$150K', 'ALL OTHER OFFENSES')
category_basket('CRIMINAL SIMULATION', 'ALL OTHER OFFENSES')
category_basket('VOIDED OFFENSE', 'ALL OTHER OFFENSES')
category_basket('CRIMINAL SOLICITATION', 'ALL OTHER OFFENSES')
category_basket('UNLAWFUL INSTALLATION OF TRACKING DEVICE', 'ALL OTHER OFFENSES')
category_basket('CRIMINAL CONSPIRACY', 'ALL OTHER OFFENSES')
category_basket('UNSOLICITED ELECTRONIC MAIL MESSAGES', 'ALL OTHER OFFENSES')
category_basket('ELECTRONIC ACCESS INTERFERENCE', 'ALL OTHER OFFENSES')
category_basket('HINDER APPREHENSION OR PROSECUTION KNOWN FELON', 'ALL OTHER OFFENSES')
category_basket('dumping', 'ENVIRONMENTAL CRIMES (NON-FBI)')
category_basket('impersonate', 'ALL OTHER OFFENSES')
category_basket('mir', 'ALL OTHER OFFENSES')
category_basket('SMUGGLING OF PERSONS', 'ALL OTHER OFFENSES')
category_basket('UNAUTHORIZED USE OF TEXAS TEMP TAG', 'ALL OTHER OFFENSES')
category_basket('other', 'ALL OTHER OFFENSES')
category_basket('PLACEMENT OF SERIAL NUMBERS WITH INTENT TO CHANGE IDENTITY', 'ALL OTHER OFFENSES')
category_basket('oppress', 'ALL OTHER OFFENSES')
category_basket('POSSESSION AND USE OF VOLATILE CHEMICAL', 'ALL OTHER OFFENSES')
category_basket('ELECTRONIC ACCESS INTERFERENCE', 'ALL OTHER OFFENSES')
category_basket('manslaughter', 'HOMICIDE OFFENSES')
category_basket('extort', 'EXTORTION/BLACKMAIL')
category_basket('blackmail', 'EXTORTION/BLACKMAIL')
category_basket('ENGAGING IN ORGANIZED CRIMINAL ACTIVITY MB', 'ALL OTHER OFFENSES')
category_basket('ESCAPE FROM CUSTODY', 'ALL OTHER OFFENSES')
category_basket('sex', 'SEX OFFENSES, FORCIBLE/SEX OFFENSES, NONFORCIBLE')
category_basket('firework', 'ALL OTHER OFFENSES')
category_basket('PURCHASE FURNISH ALCOHOL TO A MINOR', 'ALL OTHER OFFENSES')
category_basket('CRIMINAL CONSPIRACY', 'ALL OTHER OFFENSES')
category_basket('trespass', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('disorderly', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('intoxication', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('INVASIVE VISUAL RECORDING BATH/DRESS RM', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('lewd', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('ACCIDENTAL DEATH (NO OFFENSE)', 'ALL OTHER OFFENSES')
category_basket('BMV', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('vehicle', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('theft', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('burglary', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('robbery', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('traf vio', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('traffic vio', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('POSSSESS, MANUFACTOR OR DISTRIBUTE RETAIL THEFT  INSTRUMENT', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('POSSESSION OF STOLEN PROPERTY', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('CONSUME ON PREMISES PROHIBITED', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('marijuana', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('man del', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('drug', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('poss ', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('bestiality', 'ANIMAL CRIMES (NON-FBI)')
category_basket('cruelty', 'ANIMAL CRIMES (NON-FBI)')
category_basket('dog', 'ANIMAL CRIMES (NON-FBI)')
category_basket('animal', 'ANIMAL CRIMES (NON-FBI)')
category_basket('credit', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('counterfeit', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('fraud', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('forge', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('child', 'ABUSE (CHILD OR ADULT) (NON-FBI)')
category_basket('prostitution', 'PORNOGRAPHY/OBSCENE MATERIAL/PROSTITUTION OFFENSES')
# crime_basket = binning_data[binning_data['crime_category'].str.contains('Firearm', regex=False, case=False)]

# crime_basket['crime_category'] = crime_basket['crime_category'].apply(lambda x: "'" + str(x) + "'")

# count += len(crime_basket)

# update_statements.append(f'''update ALL_CRIME_ACTUAL set crime_category = 'WEAPON LAW VIOLATIONS' where crime_category_original in ({', '.join(crime_basket['crime_category'])})''')

#Fire Arms Basket

#Arson

# crime_basket = binning_data[binning_data['crime_category'].str.contains('Arson', regex=False, case=False)]

# crime_basket['crime_category'] = crime_basket['crime_category'].apply(lambda x: "'" + str(x) + "'")

# count += len(crime_basket)

# update_statements.append(f'''update ALL_CRIME_ACTUAL set crime_category = 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON' where crime_category_original in ({', '.join(crime_basket['crime_category'])})''')

#Arson

print('Total basketed: ' + str(count) + ' out of ' + str(total_rows))

print(update_statements)
