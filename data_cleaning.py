import pandas as pd
from datetime import date

class DataCleaning():
    ''' 
    This is a class created primarily to define methods to clean data 
    '''

    def clean_user_data_dates(self, df, column):
        '''
        Cleans formatting errors in date of birth and joining dates. Used in the function clean_user_data
        
        Input:
            Date columns
        
        Returns:
            Formatted date columns
        '''
        

        df['year'] = df[column].str.extract(r'(\d{4})')
        df['month'] = df[column].str.extract(r'([a-zA-Z]+)')
        month_to_number = {
            'January': '01',
            'February': '02',
            'March': '03',
            'April': '04',
            'May': '05',
            'June': '06',
            'July': '07',
            'August': '08',
            'September': '09',
            'October': '10',
            'November': '11',
            'December': '12'
        }
        df['month'] = df['month'].map(month_to_number)
        
        condition = df['month'].isna()
        df.loc[condition,'month'] = df[column].str[-5:-3] 
        df['day'] = df[column].str[-2:] 

        df[column] = pd.to_datetime(df[column], errors = 'coerce').dt.date
        condition2 = df[column].isna()
        df.loc[condition2,column] = df['year'] + '-' + df['month'] + '-' + df['day'] 
        df.drop(['year','month','day'],axis = 1,inplace=True)
        return df[column]
    

    def clean_user_data(self, df):
        ''' 
        Cleans user data from legacy_user table
        
        Input: 
            legacy_user table
        
        Returns:
            Clean data including formatted telephone number, addresses, removal of NULL etc
        '''
        #1. Sorting inconsistency in country and country codes  
        condition = (df['country_code'] == 'GGB')
        df.loc[condition,'country_code'] = 'GB'

        #2. Removes rows with all NULL values
        null_filter = (df['date_of_birth']!='NULL')
        clean_df = df[null_filter]

        #3. Checking the 15 countries that don't have proper names and only have one value each
        uk_filter = (clean_df['country'] == 'United Kingdom')
        de_filter = (clean_df['country'] == 'Germany')
        us_filter = (clean_df['country'] == 'United States')
        clean_df = clean_df[uk_filter | de_filter | us_filter]

        #4. CLEANING ADDRESS AS IT HAS "\n" character instead of space. Also adding space before capital letter of address
        clean_df['address'] = clean_df['address'].str.replace('\n',' ')
        clean_df['address'] = clean_df['address'].str.replace('/',' ')
        clean_df['address'] = clean_df['address'].str.replace(r'([A-Z])', r' \1')

        #5. CLEANING INCORRECT FORMATTING OF BOTH DATE COLUMNS
        clean_df['date_of_birth'] = self.clean_user_data_dates(clean_df, 'date_of_birth')
        clean_df['join_date'] = self.clean_user_data_dates(clean_df, 'join_date')

        #6. FORMATTING TELEPHONE NUMBERS
        # (i) adding country phone codes
        uk_filter = (clean_df['country'] == 'United Kingdom')
        de_filter = (clean_df['country'] == 'Germany')
        us_filter = (clean_df['country'] == 'United States')
        clean_df['phone_country_code'] = ''
        clean_df.loc[uk_filter,'phone_country_code'] = '+44 (0) '
        clean_df.loc[de_filter,'phone_country_code'] = '+49 (0) '
        clean_df.loc[us_filter,'phone_country_code'] = '+1 '

        # (ii) adding main phone numbers - different code by country

        clean_df[['main_phone_number','us_extension']] = clean_df['phone_number'].str.split('x', expand=True)
        clean_df['us_extension'] = clean_df['us_extension'].astype(str).replace('None','') 
        condition =  (clean_df['us_extension'] != '')
        clean_df.loc[condition, 'us_extension'] = 'x' + clean_df['us_extension']
        clean_df['main_phone_number'] = clean_df['main_phone_number'].replace(r'[^0-9]', '', regex=True).astype(str)
        
        def main_phone_number(row):
            if row['country'] == 'United Kingdom' or row['country'] == 'United States':
                return row['main_phone_number'][-10:]
            else:
                row['first_zero'] = row['main_phone_number'].find('0')
                return row['main_phone_number'][row['first_zero']+1:]
        
        clean_df['main_phone_number'] = clean_df.apply(main_phone_number, axis = 1)

        # (iii) PUTTING TOGETHER PHONE NUMBERS

        def formatted_phone_number(row):
            if row['country'] == 'United Kingdom' or row['country'] == 'Germany':
                return str(row['phone_country_code']) + str(row['main_phone_number'])
            else:
                return str(row['phone_country_code']) + str(row['main_phone_number']) +  str(row['us_extension'])

        clean_df['formatted_phone_number'] = clean_df.apply(formatted_phone_number, axis = 1)
        clean_df.drop(['phone_country_code', 'us_extension', 'main_phone_number','phone_number'],axis = 1,inplace=True)

        #7. Resetting index
        clean_df  = clean_df.reset_index(drop=True)
        return clean_df
    

    def clean_card_data(self, df):
        '''
        Cleans card data

        Inputs:
            Takes dataframe

        Returns:
            Cleaned dataframe             
        '''
        #1. Taking out NULL values
        null_filter = (df['card_number'] != 'NULL')
        clean_df = df[null_filter]

        #2. Taking out rows that don't have right card providers (e.g., ABC2345 instead of Amex/Diners etc)
        value_counts = clean_df['card_provider'].value_counts() 
        min_frequency = 12
        value_filter = clean_df['card_provider'].map(value_counts) >= min_frequency
        clean_df = clean_df[value_filter]

        #3. Formatting date columns that have D/M/Y 
        clean_df['date_payment_confirmed'] = pd.to_datetime(clean_df['date_payment_confirmed'], format = 'mixed').dt.date

        #4. Changing card number to numeric type (for space?)
        clean_df['card_number'] = clean_df['card_number'].replace(r'[^\d]','',regex=True)
        clean_df['card_number'] = pd.to_numeric(clean_df['card_number'], errors = 'coerce')

        #5. Resetting index
        clean_df  = clean_df.reset_index(drop=True)
        return clean_df
        

    def clean_store_data(self, store_df):
        '''
        Cleans store data

        Inputs:
            Takes dataframe

        Returns:
            Cleaned dataframe             
        '''
        #1. When lat is not NA, you can see values of all columns are wrong. So only keeping rows that are NA
        store_df_clean = store_df[store_df['lat'].isna()]

        #2. Deleting irrelevant rows and columns
        store_df_clean.drop([0,'lat','index'], axis=1,inplace=True)
        store_df_clean = store_df_clean.drop(450)

        #3. Renaming typos in continent
        fil1 = (store_df_clean['continent'] == 'eeEurope')
        fil2 = (store_df_clean['continent'] == 'eeAmerica')
        store_df_clean.loc[fil1,'continent'] = 'Europe'
        store_df_clean.loc[fil2,'continent'] = 'America'

        #4. Correctly assigning continent based on country code (assuming country code is correct)

        uk_filter = (store_df_clean['country_code'] == 'GB')
        de_filter = (store_df_clean['country_code'] == 'DE')
        us_filter = (store_df_clean['country_code'] == 'US')

        store_df_clean.loc[uk_filter | de_filter,'continent'] = 'Europe'
        store_df_clean.loc[us_filter,'continent'] = 'America'

        #5. Correcting type in address column (/n replaced with space)
        store_df_clean['address'] = store_df_clean['address'].str.replace('\n',' ')

        #6. Changing to right data types for some columns
        store_df_clean['longitude'] = store_df_clean['longitude'].astype(float)
        store_df_clean['latitude'] = store_df_clean['latitude'].astype(float)
        store_df_clean['staff_numbers'] = store_df_clean['staff_numbers'].replace(r'[^\d]','',regex=True) # Taking out string characters in staff_numbers in 5 instances - assuming typo
        store_df_clean['staff_numbers'] = pd.to_numeric(store_df_clean['staff_numbers'], errors = 'coerce').astype('Int32')
        store_df_clean['opening_date'] = pd.to_datetime(store_df_clean['opening_date'], format = 'mixed').dt.date
        store_df_clean['country_code'] = store_df_clean['country_code'].astype('category')

        #7. Resetting index
        store_df_clean  = store_df_clean.reset_index(drop=True)
        return store_df_clean
    

    def convert_product_weights(self, product_df2):
        '''
        Converts weieght column from multiple formats to single consistent kg format

        Input:
            Product details dataframe with weights column

        Returns:
            Product details dataframe with consistent weights column
        '''
        #1. Recognise unique format such as 12 x 400g into standard 4800gm
        product_df2[['dim1', 'dim2']] = product_df2['weight'].str.split('x', expand=True)
 
        # 2.Extract metric for each row (kg, g, l, oz)
        product_df2['units'] = product_df2['dim1'].str.extract(r'([a-zA-Z]+)')
        product_df2['units2'] = product_df2['dim2'].str.extract(r'([a-zA-Z]+)')
        product_df2.loc[product_df2['units'].isna(), 'units'] = product_df2['units2']

        #3. Remove metric detail ('kg' etc) to convert to float 
        product_df2['dim1'] = product_df2['dim1'].str.replace(r'([a-zA-Z]+)','',regex=True) 
        product_df2['dim2'] = product_df2['dim2'].str.replace(r'([a-zA-Z]+)','',regex=True)
        product_df2.loc[product_df2['dim2'].isna(), 'dim2'] = 1

        #4. Remove one value which is '77 .' and then convert both dimensions to float
        product_df2['dim1'] = product_df2['dim1'].astype(str).apply(lambda x: x[:-1] if x.endswith('.') else x)
        product_df2['dim1'] = product_df2['dim1'].astype(float)
        product_df2['dim2'] = product_df2['dim2'].astype(float)

        #5. Obtain total weight for all formats
        product_df2['weight_clean'] = product_df2['dim1'] * product_df2['dim2']

        #6. Convert all weights to kilos with below conversion formats
        product_df2['conversion'] = 1.0
        product_df2.loc[(product_df2['units'] == 'g') | (product_df2['units'] == 'l'), 'conversion'] = 0.001
        product_df2.loc[product_df2['units'] == 'oz', 'conversion'] = 0.000035274
        product_df2['weight_clean2'] = product_df2['weight_clean'] * product_df2['conversion']

        #7. Tidy up column naming and drop extra columns
        product_df2['weight'] = product_df2['weight_clean2']
        product_df2 = product_df2.rename(columns ={'weight': 'weight_kg'})
        product_df2.drop(['units','units2', 'dim1', 'dim2','weight_clean', 'weight_clean2','conversion'], axis=1, inplace=True)
        return product_df2


    def clean_products_data(self, product_df):
        '''
        Cleans product details data
        
        Input:
            Product details dataframe
        Returns:
            Cleaned product details dataframe        
        '''
        #1. Cleaning product data 
        product_df2 = product_df
        
        #2. Dropping 'unnamed' column which is index
        product_df2.drop('Unnamed: 0', axis=1, inplace=True)

        #3. Dropping four rows that have all columns as NaN
        product_df2 = product_df2.dropna(how='all')

        #4. Taking out rows that don't have right categories and removed values (frequency = 1)
        value_counts = product_df2['category'].value_counts()
        value_filter = product_df2['category'].map(value_counts) > 1
        product_df2 = product_df2[value_filter]
        product_df2.info()

        #5. Changing column name from 'removed' to 'availability'
        product_df2 = product_df2.rename(columns ={'removed': 'availability'})

        #6. Changing formats of columns to relevant formats
        product_df2['product_price'] = product_df2['product_price'].replace('£','',regex=True) #Taking out the pound symbol
        product_df2['product_price'] = product_df2['product_price'].astype(float)
        product_df2 = product_df2.rename(columns ={'product_price': 'product_price_GBP'})
        product_df2['EAN'] = product_df2['EAN'].astype(int)
        product_df2['date_added'] = pd.to_datetime(product_df2['date_added'], format='mixed')

        #7. Resetting index
        product_df2  = product_df2.reset_index(drop=True)
        return product_df2


    def clean_orders_data(self, df):
        '''
        Cleans orders data by removing unnecessary columns

        Input: 
            Order dataframe

        Returns:
            Clean order data frame
        
        '''
        df.drop(['level_0','index','first_name','last_name','1'], axis=1, inplace=True)
        return df


    def clean_order_dates(self,df):
        '''
        Cleans date and time details of order

        Input: 
            Dataframe

        Returns:
            Cleaned dataframe
        
        '''
        #1. Removing all rows that have 'NULL' as value in all columns
        not_null = (df['timestamp']!='NULL')
        clean_df = df[not_null]

        #2. Convert year, month and day into numeric format. Removing string values in all columns (returned as NaN after forced conversion to numeric)
        clean_df['year'] = pd.to_numeric(clean_df['year'], errors='coerce')
        only_numeric = clean_df['year'].notna()
        clean_df2 = clean_df[only_numeric]
        clean_df2['year'] = clean_df2['year'].astype(int)
        clean_df2['month'] = pd.to_numeric(clean_df2['month'], errors='coerce').astype(int)
        clean_df2['day'] = pd.to_numeric(clean_df2['day'], errors='coerce').astype(int)
        clean_df2['date'] = pd.to_datetime(clean_df2[['year','month','day']])

        #3. Creating date variable and deleting unnecessary columns
        clean_df2.drop(['month','day','year'], axis=1,inplace=True)
        desired_order = ['date','timestamp','time_period','date_uuid']
        clean_df2 = clean_df2[desired_order]
        return clean_df2