import pandas as pd
from datetime import date
import sqlalchemy
import sqlite3

#------------------------Load the date-----------------------------
path = str(input())
path = path.replace('\\','/')



try:
    df = pd.read_csv(path, delimiter=';')
except FileNotFoundError:
    print(f'No such file or directory {path}')


# -----------------------Remove unnecessary columns----------------
df = df.drop(columns=['altura', 'peso'])



# -----------------------Rename columns----------------------------
df = df.rename(columns={'fecha_nacimiento': 'birth_date',
                        'fecha_vencimiento': 'due_date',
                        'deuda':'due_balance',
                        'direccion':'address',
                        'correo':'email',
                        'estatus_contacto':'status',
                        'prioridad':'priority',
                        'telefono':'phone'
                                        }) 

# -----------------------Replace missing values--------------------
df.fillna({'priority':0,
           'email':'MISSING',
           'status':'MISSING',
           'phone':'MISSING'}, inplace=True)

# -----------------------Change datatype---------------------------
df = df.astype({'birth_date': 'datetime64',
            'due_date':'datetime64',
            'phone':str,
            'priority':'int64'})

df['phone'] = df['phone'].apply(lambda x : x.replace(".0",""))



#------------------------tramsform to uppercase-------------------
df['first_name'] = df['first_name'].str.upper()
df['last_name'] = df['last_name'].str.upper()
df['gender'] = df['gender'].str.upper()
df['address'] = df['address'].str.upper()
df['email'] = df['email'].str.upper()
df['status'] = df['status'].str.upper()

#-------------------------Calculate age---------------------------
def from_dob_to_age(bird):
    today = date.today()
    age = today.year - bird.year - ((today.month, today.day) < (bird.month, bird.day))
    return age

df['age'] = df['birth_date'].apply(lambda x: from_dob_to_age(x))


#-------------------------Group by age----------------------------
group = [0,20,30,40,50,60,100]
names = ['1','2','3','4','5','6']
df['age_group'] = pd.cut(df['age'], group, labels = names)
df['age_group'] = df['age_group'].astype(int)

#-------------------------Calculate delinquency-------------------
df['delinquency'] = pd.Timestamp.now().normalize() - df['due_date']
df['delinquency'] = pd.to_numeric(df['delinquency'].dt.days.astype('int64'))


#-------------------------Dataframe clientes-----------------------
df_cliente = df[[
                'fiscal_id',
                'first_name',
                'last_name',
                'gender',
                'age',
                'age_group',
                'birth_date',
                'due_date',
                'delinquency',
                'due_balance',
                'address']]

#--------------------------Create dataframe email--------------------
df_email = df[['fiscal_id',
             'email',
             'phone',
             'status',
             'priority']]


#--------------------------Dataframe phone---------------------------

df_phone = df[['fiscal_id',
               'phone',
               'status',
               'priority']]

#--------------------------Create .xlsx files------------------------
df_cliente.to_excel('output/cliente.xlsx') 
df_email.to_excel('output/email.xlsx') 
df_phone.to_excel('output/phone.xlsx') 
print("Files created store in /output")


#--------------------------Load data into he database-----------------
engine = sqlalchemy.create_engine('sqlite:///database.db3')
conn = sqlite3.connect('database.db3')
cursor = conn.cursor()

print('Start loading data')
print('Opened database successfully')
sql_query = """
    CREATE TABLE IF NOT EXISTS cliente(
        fiscal_id VARCHAR(50),
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(50),
        age INT,
        age_group INT,
        birth_date DATE,
        due_date DATE,
        delinquency INT,
        due_balance INT,
        address VARCHAR(100)
    );
    
    
    CREATE TABLE IF NOT EXISTS email(
        fiscal_id VARCHAR(50),
        email VARCHAR(50),
        phone VARCHAR(50),
        status VARCHAR(50),
        priority INT,
        CONSTRAINT primary_key_constraint PRIMARY KEY (fiscal_id)
    );


    CREATE TABLE IF NOT EXISTS phone(
        fiscal_id VARCHAR(50),
        phone VARCHAR(50),
        status VARCHAR(50),
        priority INT,
        CONSTRAINT primary_key_constraint PRIMARY KEY (fiscal_id)
    );
    
    """

cursor.executescript(sql_query)
try:
    df_cliente.to_sql('cliente', engine, index=False, if_exists='append')
    df_email.to_sql('email', engine, index=False, if_exists='append')
    df_phone.to_sql('phone', engine, index=False, if_exists='append')
    print("Date store in database")
except:
    print('Data already exists in the database')
conn.close()
print('Close database successfully')