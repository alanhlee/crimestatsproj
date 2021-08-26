import pandas as pd
import pyodbc
import sys

# Example: in terminal `python binning.py Dallas`
#
# Reads Dallas_Cleaned db table and outputs update statements to bin categories into master categories

city = sys.argv[1]

server = 'britemssql.ce0lltgngjdb.us-east-1.rds.amazonaws.com,1433'
database = 'RPT_DM'
username = 'rpt_user'
password = 'rpt_user'

cnxn = (pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server+';DATABASE='+database+';UID=' + username+';PWD=' + password))

binning_query = f"select distinct acl.crime_category, ben_data.lefted, ben_data.crime_category as crime_category_original from dbo.{city}_Cleaned acl left outer join(select distinct left(CRIME_CATEGORY_ORIGINAL, REPLACE(charindex('|', CRIME_CATEGORY_ORIGINAL), 0, 9999)-1) as lefted, CRIME_CATEGORY from dbo.ALL_CRIME_ACTUAL ac where city='{city}' and crime_category is not null) ben_data on acl.crime_category=ben_data.lefted where ben_data.crime_category is null order by acl.crime_category"

category_differences = pd.read_sql(binning_query, cnxn)

update_statements = []
count = 0
total_rows = len(category_differences)

#Search for strings containing same category descriptions and basket them

def category_basket(common_str, master_category):
  global category_differences
  global count

  mask = category_differences['crime_category'].str.contains(common_str, regex=False, case=False)

  crime_basket = category_differences[mask].copy()

  crime_basket['crime_category'] = crime_basket['crime_category'].apply(lambda crime_category: "'" + str(crime_category) + "'")

  binned_count = len(crime_basket)

  print('Binning ' + str(binned_count) + ' | ' + 'Common String: (' + common_str + ')' + ' to ' + master_category)

  count += binned_count

  #filter out ones basketed
  category_differences = category_differences[~mask]

  if not crime_basket['crime_category'].empty:
    update_statements.append(f'''update ALL_CRIME_ACTUAL set crime_category = '{master_category}' where crime_category is null and crime_category_original in ({', '.join(crime_basket['crime_category'])})''')

# catches remaining unbinned crime_categories

def catch_remaining():
  global category_differences
  global count

  crime_basket = category_differences.copy()

  crime_basket['crime_category'] = crime_basket['crime_category'].apply(lambda crime_category: '\'ALL OTHER OFFENSES\'')

  binned_count = len(crime_basket)

  print('Binning ' + str(binned_count) + ' | ' + ' * ' + ' to ' + 'ALL OTHER OFFENSES')

  count += binned_count

  #filter out ones basketed
  category_differences = pd.DataFrame()

  if not crime_basket['crime_category'].empty:
    update_statements.append(f'''update ALL_CRIME_ACTUAL set crime_category = 'ALL OTHER OFFENSES' where crime_category is null and crime_category_original in ({', '.join(crime_basket['crime_category'])})''')

category_basket('arson', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('desecration', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('graffiti', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('tamper', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('defacement', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('vandalism', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('destruction', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')
category_basket('damage', 'DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY/ARSON')

  
category_basket('delinquency', 'ALL OTHER OFFENSES')
category_basket('non-agg', 'ALL OTHER OFFENSES')


category_basket('assault', 'ASSAULT OFFENSES')
category_basket('agg', 'ASSAULT OFFENSES')
category_basket('batt', 'ASSAULT OFFENSES')
category_basket('aslt', 'ASSAULT OFFENSES')


category_basket('dumping', 'ENVIRONMENTAL CRIMES (NON-FBI)')


category_basket('manslaughter', 'HOMICIDE OFFENSES')
category_basket('murder', 'HOMICIDE OFFENSES')
category_basket('homicide', 'HOMICIDE OFFENSES')


category_basket('extort', 'EXTORTION/BLACKMAIL')
category_basket('blackmail', 'EXTORTION/BLACKMAIL')


category_basket('sex', 'SEX OFFENSES, FORCIBLE/SEX OFFENSES, NONFORCIBLE')


category_basket('trespass', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('harass', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('disorderly', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('intoxication', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('INVASIVE VISUAL RECORDING BATH/DRESS RM', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('lewd', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('indicent', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('eavesdropping', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')
category_basket('conduct', 'DISORDERLY CONDUCT/PEEPING TOM/CURFEW/LOITERING/VAGRANCY VIOLATIONS/DRUNKENNESS/TRESPASS OF REAL PROPERTY/HARRASSMENT (NON-FBI)/NON COMPLIANCE W/ LAWS (NON-FBI)')


category_basket('BMV', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('stole', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('vehicle', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('theft', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('stolen', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('over $', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('under $', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('snatching', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('pickpocket', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('purse snatch', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('larc', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')
category_basket('retail crime', 'LARCENY/THEFT OFFENSES/STOLEN PROPERTY OFFENSES/MOTOR VEHICLE THEFT')


category_basket('burglary', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('robbery', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('entry', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('home invasion', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('POSSESSION OF STOLEN PROPERTY', 'BURGLARY/BREAKING & ENTERING/ROBBERY')
category_basket('POSSSESS, MANUFACTOR OR DISTRIBUTE RETAIL THEFT  INSTRUMENT', 'BURGLARY/BREAKING & ENTERING/ROBBERY')



category_basket('traf vio', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('traffic vio', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('reckless driving', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('racing', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('hit and run', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')
category_basket('hit & run', 'MOTOR VEHICLE LAW/TRAFFIC VIOLATIONS (NON-FBI)')



category_basket('CONSUME ON PREMISES PROHIBITED', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('marij', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('man del', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('manufacture / deliver', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('drug', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('cannabis', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('poss ', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('narc', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('possess', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('driving under', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('meth', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')
category_basket('ampheta', 'DRUG/NARCOTIC OFFENSES/LIQUOR LAW VIOLATIONS/DRIVING UNDER THE INFLUENCE')


category_basket('embezzle', 'EMBEZZLEMENT')


category_basket('beastiality', 'ANIMAL CRIMES (NON-FBI)')
category_basket('cruelty', 'ANIMAL CRIMES (NON-FBI)')
category_basket('dog', 'ANIMAL CRIMES (NON-FBI)')
category_basket('animal', 'ANIMAL CRIMES (NON-FBI)')


category_basket('credit', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('counterfeit', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('DECEPTIVE COLLECTION PRACTICES', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('fraud', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('finan', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('forge', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('check', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('money', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')
category_basket('cash', 'COUNTERFEITING/FORGERY/FRAUD OFFENSES/BAD CHECKS')


category_basket('prostitution', 'PORNOGRAPHY/OBSCENE MATERIAL/PROSTITUTION OFFENSES')
category_basket('pornography', 'PORNOGRAPHY/OBSCENE MATERIAL/PROSTITUTION OFFENSES')
category_basket('obscen', 'PORNOGRAPHY/OBSCENE MATERIAL/PROSTITUTION OFFENSES')
category_basket('voyeur', 'PORNOGRAPHY/OBSCENE MATERIAL/PROSTITUTION OFFENSES')

category_basket('abandonment', 'ABUSE (CHILD OR ADULT) (NON-FBI)')
category_basket('child', 'ABUSE (CHILD OR ADULT) (NON-FBI)')


category_basket('abduction', 'KIDNAPPING/ABDUCTION/RUNAWAY')
category_basket('runaway', 'KIDNAPPING/ABDUCTION/RUNAWAY')
category_basket('kidnap', 'KIDNAPPING/ABDUCTION/RUNAWAY')

category_basket('firearm', 'WEAPON LAW VIOLATIONS')
category_basket('weapon', 'WEAPON LAW VIOLATIONS')
category_basket('armed', 'WEAPON LAW VIOLATIONS')
category_basket('gun', 'WEAPON LAW VIOLATIONS')
category_basket('explosive', 'WEAPON LAW VIOLATIONS')
category_basket('rifle', 'WEAPON LAW VIOLATIONS')


catch_remaining()

print('Total basketed: ' + str(count) + ' out of ' + str(total_rows))

#Create a cursor
cursor = cnxn.cursor()

def run_query(query):
    '''Runs the query
    
    Args:
        query(str): An SQL query to run.
        
    side Effects:
        Runs the SQL query.
    '''
    cursor.execute(query)
    cnxn.commit()

#for statement in update_statements: 
  #run_query(statement)
  #print(statement)


#print(update_statements)





















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