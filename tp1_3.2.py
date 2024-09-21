import subprocess
import psycopg2
import sys
from configparser import ConfigParser
import os
from datetime import datetime


DATABASE_NAME = 'PRODUCTS'
DATABASE_INI = 'database.ini'

USER_NAME = 'professor'
PASSWORD = 'batatinha'


import re
from enum import Enum

class CategoriesSub:
    def __init__(self, name='', id='', sub: 'CategoriesSub' = None, parent_id=''):
        self.parent_id = parent_id
        self.name = name
        self.id = id
        self.sub = sub
        
    def __str__(self, level=0):
        return print_category_cascade(self, level)

class Review:
    def __init__(self, total, downloaded, avg_rating):
        self.total = total
        self.downloaded = downloaded
        self.avg_rating = avg_rating
    def __str__(self):
        return f"Total: {self.total}\nDownloaded: {self.downloaded}\nAvgRating: {self.avg_rating}"

class ReviewSub:
    def __init__(self, date='', customer='', rating='', votes='', helpful=''):
        self.date = date
        self.customer = customer
        self.rating = rating
        self.votes = votes
        self.helpful = helpful
        
    def __str__(self):
        return f"\tDate: {self.date}\n\tCustomer: {self.customer}\n\tRating: {self.rating}\n\tVotes: {self.votes}\n\tHelpful: {self.helpful}\n\n"
    
class Similar:
    def __init__(self, total='', ids=[]):
        self.total = total
        self.ids = ids
        
    def __str__(self):
        return f"Total: {self.total}\nIDs: {self.ids}"
class ProductAttributesENUM(Enum):
    ID = 1
    ASIN = 2
    TITLE = 3
    GROUP = 4
    SALESRANK = 5
    SIMILAR = 6
    CATEGORIES = 7
    CATEGORIES_SUB = 8
    REVIEWS = 9
    REVIEWS_SUB = 10

class Product:
    def __init__(self, id='', asin='', title='', group='', salesrank='', similar='', categories='', categories_sub=None, reviews='', reviews_sub=[]):
        if categories_sub is None:
            categories_sub = []
        self.id = id
        self.asin = asin
        self.title = title
        self.group = group
        self.salesrank = salesrank
        self.similar = similar
        self.categories = categories
        self.categories_sub = categories_sub
        self.reviews = reviews
        self.reviews_sub = []
    
    def __str__(self):
        categories_sub_str = str(self.categories_sub) if self.categories_sub else ''
        review_sub_str = "\n".join(str(sub_review) for sub_review in self.reviews_sub)
        return (f"\n\nId: {self.id}\nASIN: {self.asin}\ntitle: {self.title}\n"
                f"group: {self.group}\nsalesrank: {self.salesrank}\nsimilar: {self.similar}\n"
                f"categories: {self.categories}\nCategories-sub:\n{categories_sub_str}\nReviews: {self.reviews}\nReview-sub:\n{review_sub_str}")

def get_line_type(line):
    if line.startswith('Id'):
        return ProductAttributesENUM.ID
    elif line.startswith('ASIN'):
        return ProductAttributesENUM.ASIN
    elif line.startswith('title'):
        return ProductAttributesENUM.TITLE
    elif line.startswith('group'):
        return ProductAttributesENUM.GROUP
    elif line.startswith('salesrank'):
        return ProductAttributesENUM.SALESRANK
    elif line.startswith('similar'):
        return ProductAttributesENUM.SIMILAR
    elif line.startswith('categories'):
        return ProductAttributesENUM.CATEGORIES
    elif line.startswith('|'):
        return ProductAttributesENUM.CATEGORIES_SUB
    elif line.startswith('reviews'):
        return ProductAttributesENUM.REVIEWS
    elif re.match(r'^\d{4}-\d{1,2}-\d{1,2}', line):
        return ProductAttributesENUM.REVIEWS_SUB
    return None

def get_simple_parameter(line, index):
    return line[index:].strip()

def get_parameter_for_similar_atribute(line, index):
    parameters = line[index:].split()
    total = int(parameters[0])
    ids = parameters[1:]
    
    new_similar = Similar(total, ids)
    
    return new_similar

def get_parameter_for_reviews_atribute(line, index):
    parameters = line[index:].split()
    total = int(parameters[1])
    downloaded = int(parameters[3])
    avg_rating = float(parameters[6])
    
    new_reviews = Review(total, downloaded, avg_rating)
    
    return new_reviews

def parse_category(category_str):
    
    id = ''
    index_aux = 0
    
    for i in range(1, len(category_str)):
        if category_str[-i] == '[':
            index_aux = -i
            break
        elif (i != 1):
            id = category_str[-i] + id
    
    name = category_str[0:index_aux]
    
    return name, id
    
def filter_empty_strings(vetor):
    return [item for item in vetor if item != '']
    
def map_subcategory_obj(parameters, new_category, parent_id):
    if len(parameters) > 0:
        name, id = parse_category(parameters[0])
        
        # Criar nova subcategoria se não existir
        if new_category is None:
            new_category = CategoriesSub(name=name, id=id, parent_id=parent_id)
        else:
            new_category.parentId = parent_id
            new_category.name = name
            new_category.id = id
        
        # Recursivamente mapear as subcategorias
        new_category.sub = map_subcategory_obj(parameters[1:], new_category.sub, new_category.id)
        
    return new_category  # Retorna o objeto CategoriesSub atualizado

def print_category_cascade(category, level=0, result=''):
    if category is None:
        return ''
    
    indent = '\t' * level
    
    result += f"{indent}Name: {category.name}, ID: {category.id}, Parent ID: {category.parent_id}\n"
    
    if category.sub:
        result += print_category_cascade(category.sub, level + 1)
    
    return result


def get_parameter_for_subcategories_atribute(line):
    
    parameters = line.split('|')
    parameters = filter_empty_strings(parameters)
    
    new_category = CategoriesSub()
    
    new_category = map_subcategory_obj(parameters, new_category, new_category.parent_id)
    
    return new_category

def get_sub_review(line):
    
    new_sub_review = ReviewSub()
    parameters = line.split()
    
    new_sub_review.date = parameters[0]
    new_sub_review.customer = parameters[2]
    new_sub_review.rating = parameters[4]
    new_sub_review.votes = parameters[6]
    new_sub_review.helpful = parameters[8]
    
    return new_sub_review    
    
lista_produtos = []

with open('amazon-meta.txt', 'r') as file:
    lines = file.readlines()
    new_product = Product()
    
    for line in lines:
        line = line.strip()
        if line:
            line_type = get_line_type(line)
            if line_type == ProductAttributesENUM.ID:
                if new_product.id:
                    lista_produtos.append(new_product)
                new_product = Product()
                new_product.id = get_simple_parameter(line, 3)
            elif line_type == ProductAttributesENUM.ASIN:
                new_product.asin = get_simple_parameter(line, 5)
            elif line_type == ProductAttributesENUM.TITLE:
                new_product.title = get_simple_parameter(line, 6)
            elif line_type == ProductAttributesENUM.GROUP:
                new_product.group = get_simple_parameter(line, 6)
            elif line_type == ProductAttributesENUM.SALESRANK:
                new_product.salesrank = get_simple_parameter(line, 10)
            elif line_type == ProductAttributesENUM.SIMILAR:
                new_product.similar = get_parameter_for_similar_atribute(line, 8)
            elif line_type == ProductAttributesENUM.CATEGORIES:
                new_product.categories = get_simple_parameter(line, 11)
            elif line_type == ProductAttributesENUM.CATEGORIES_SUB:
                new_product.categories_sub.append(get_parameter_for_subcategories_atribute(line))
            elif line_type == ProductAttributesENUM.REVIEWS:
                new_product.reviews = get_parameter_for_reviews_atribute(line, 8)
            elif line_type == ProductAttributesENUM.REVIEWS_SUB:
                new_product.reviews_sub.append(get_sub_review(line))
    
    if new_product.id:
        lista_produtos.append(new_product)


def error_handling(err):
    if err != '':
        print(err)
        sys.exit(1)
    return

def create_database_ini(file_name):

    file_path = resolve_path(file_name)
    DATABASE_NAME_LOWER = DATABASE_NAME.lower()
    with open(file_path, 'w') as config_file:
        config_file.write("[postgresql]\n")
        config_file.write("host=localhost\n")
        config_file.write(f"database={DATABASE_NAME_LOWER}\n")
        config_file.write(f"user={USER_NAME}\n")
        config_file.write(f"password={PASSWORD}\n")


def create_database():
    result = subprocess.run(['sudo',"-u" , "postgres" , "psql",'-d' ,'postgres','-c' , 
                            f'CREATE DATABASE {DATABASE_NAME};'],capture_output=True,text=True)
    error_handling(result.stderr)

def create_user():
    USER_NAME_LOWER = USER_NAME.lower()
    DATABASE_NAME_LOWER = DATABASE_NAME.lower()


    create_user_command = f"CREATE USER {USER_NAME_LOWER} WITH PASSWORD '{PASSWORD}';"
    user_credentials = subprocess.run(['sudo',"-u" , "postgres", "psql", "-d",  "postgres",'-c' ,
                            create_user_command],capture_output=True,text=True)
    error_handling(user_credentials.stderr)

    grant_access_command = f"GRANT CONNECT ON DATABASE {DATABASE_NAME_LOWER} TO {USER_NAME_LOWER};"
    grant_user_access_to_db = subprocess.run(['sudo',"-u" , "postgres" ,'psql','-d' , "postgres",'-c' , 
                            grant_access_command],capture_output=True,text=True)
    error_handling(grant_user_access_to_db.stderr)

    grant_usage_command = f"GRANT USAGE ON SCHEMA public TO {USER_NAME_LOWER};"
    subprocess.run(['sudo', "-u", "postgres", 'psql', '-d', DATABASE_NAME_LOWER, '-c', grant_usage_command], capture_output=True, text=True)

    permission_command = f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {USER_NAME_LOWER};"
    give_user_permissions = subprocess.run(['sudo',"-u" , "postgres" ,'psql','-d' ,DATABASE_NAME_LOWER,'-c' , 
                            permission_command],capture_output=True,text=True)
    error_handling(give_user_permissions.stderr)

    own_command = f"ALTER DATABASE {DATABASE_NAME_LOWER} OWNER TO {USER_NAME_LOWER}" 
    give_own_to_user = subprocess.run(['sudo',"-u" , "postgres" ,'psql','-d' ,DATABASE_NAME_LOWER,'-c' , 
                            own_command],capture_output=True,text=True)
    error_handling(give_own_to_user.stderr)


def resolve_path(file_name):
    current_dir_path = os.getcwd()
    file_path = f"{current_dir_path}/{file_name}"
    return file_path


def load_config(file_name='database.ini', section='postgresql'):
    parser = ConfigParser()
    file_path = resolve_path(file_name)
    
    parser.read(file_path)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, file_path))

    return config



def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        my_connection = psycopg2.connect(**config)
        print('Connected to the PostgreSQL server.')
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    return my_connection

def close_connection(my_connection):
    try:
        my_connection.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def close_cursor(my_cursor):
    try:
        my_cursor.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def create_cursor(my_connection):
    try:
        my_cursor = my_connection.cursor()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
    
    return my_cursor

def create_tables(my_connection,my_cursor):
    """ Create tables in the PostgreSQL database"""
    commands = (
        # TABLE PRODUCT OK
        """
        CREATE TABLE PRODUCT (
            PRODUCT_ID INT UNIQUE,
            ASIN CHAR(10) NOT NULL,
            TITLE VARCHAR,
            PRODUCT_GROUP VARCHAR,
            SALES_RANK INT,
            PRIMARY KEY (PRODUCT_ID,ASIN)
        )
        """,
        """ CREATE TABLE PRODUCT_SIMILAR (
                PRODUCT_ID INT,
                SIMILAR_ASIN CHAR(10),
                FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID),
                PRIMARY KEY (PRODUCT_ID, SIMILAR_ASIN)
                )
        """,
        """
        CREATE TABLE CATEGORY (
                CATEGORY_NAME VARCHAR(100),
                CATEGORY_ID INT,
                PARENT_ID INT NULL,
                PRIMARY KEY (CATEGORY_ID),
                FOREIGN KEY (PARENT_ID) REFERENCES CATEGORY(CATEGORY_ID),
                UNIQUE (CATEGORY_NAME, CATEGORY_ID)
        )
        """,
        """
        CREATE TABLE PRODUCT_CATEGORY (
                PRODUCT_ID INT,
                CATEGORY_ID INT,
                PRIMARY KEY (PRODUCT_ID, CATEGORY_ID),
                FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID),
                FOREIGN KEY (CATEGORY_ID) REFERENCES CATEGORY(CATEGORY_ID)
        )
        """,
        """
        CREATE DOMAIN RATING AS INT CHECK( VALUE > 0 AND VALUE<=5 )
        """,
        
        """
        CREATE TABLE REVIEW (
                REVIEW_ID INTEGER GENERATED ALWAYS AS IDENTITY,
                PRODUCT_ID INT,
                REVIEW_DATE DATE,
                CUSTOMER_ID CHAR(14),
                REVIEW_RATING RATING,
                VOTE INT,
                HELPFUL INT,
                PRIMARY KEY (PRODUCT_ID,CUSTOMER_ID, REVIEW_ID),
                FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID)
                
        )
        """)
    try:
        for command in commands:
            my_cursor.execute(command)
        my_connection.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)



def insert_into_product(my_connection, my_cursor, PRODUCT_ID:int, ASIN:str, TITLE:str, PRODUCT_GROUP:str, SALES_RANK:int ):
    command = f"""INSERT INTO PRODUCT (PRODUCT_ID, ASIN, TITLE, PRODUCT_GROUP, SALES_RANK) 
                  VALUES (%s,%s,%s,%s,%s)"""
    try:
        my_cursor.execute(command, (PRODUCT_ID, ASIN, TITLE, PRODUCT_GROUP, SALES_RANK))
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)    


def insert_into_product_similar(my_connection, my_cursor, PRODUCT_ID:int, SIMILAR_ASIN:str):
    command = f"""INSERT INTO PRODUCT_SIMILAR (PRODUCT_ID, SIMILAR_ASIN) 
                  VALUES (%s,%s)"""
    try:
        my_cursor.execute(command, (PRODUCT_ID, SIMILAR_ASIN))
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def insert_into_category(my_connection, my_cursor, CATEGORY_NAME:str, CATEGORY_ID:int ,PARENT_ID:int):
    command = f"""INSERT INTO CATEGORY (CATEGORY_NAME, CATEGORY_ID, PARENT_ID) 
                  VALUES (%s,%s,%s) ON CONFLICT (CATEGORY_NAME, CATEGORY_ID) DO NOTHING"""
    try:
        my_cursor.execute(command, (CATEGORY_NAME, CATEGORY_ID, PARENT_ID))
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        my_connection.rollback()


def insert_into_product_category(my_connection, my_cursor, PRODUCT_ID:int , CATEGORY_ID:int):
    command = f"""INSERT INTO PRODUCT_CATEGORY (PRODUCT_ID, CATEGORY_ID) 
                  VALUES (%s,%s)"""
    try:
        my_cursor.execute(command, (PRODUCT_ID, CATEGORY_ID))
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)   

def insert_into_review(my_connection, my_cursor, PRODUCT_ID:int, REVIEW_DATE:str, CUSTOMER_ID:str, REVIEW_RATING:int, VOTE: int, HELPFUL: int):
    command = """INSERT INTO review (PRODUCT_ID, REVIEW_DATE, CUSTOMER_ID, REVIEW_RATING, VOTE, HELPFUL)
                 VALUES (%s, %s, %s, %s, %s, %s)"""
    try:
        my_cursor.execute(command, (PRODUCT_ID, REVIEW_DATE, CUSTOMER_ID, REVIEW_RATING, VOTE, HELPFUL))
    except (psycopg2.DatabaseError, Exception) as error:
        my_connection.rollback()
        print(error)
        
def map_product_list(my_connection, my_cursor):
    print("Inserting products... ")
    counter = 0
    for product in lista_produtos:
        if product.title == '' and product.group == '' and product.salesrank == '':
            product.title = None
            product.group = None
            product.salesrank = None
        
        insert_into_product(my_connection, my_cursor, product.id, product.asin, product.title, product.group, product.salesrank)
        if isinstance(product.similar, Similar):
            similar_ids_list = product.similar.ids
            map_similar_list(my_connection, my_cursor, product.id, similar_ids_list)
        map_category_list(my_connection, my_cursor, product.categories_sub)
        map_category_product_list(my_connection, my_cursor, product.categories_sub, product.id)
        map_review_list(my_connection, my_cursor, product.id, product.reviews_sub)
        if counter == 10000:
            counter = 0
            my_connection.commit()
        counter += 1
        
    my_connection.commit()
    print("Data saved.")



def map_similar_list(my_connection, my_cursor, product_id, similar_ids_list):
    if len(similar_ids_list) == 0:
        None
    else:    
        for similar_id in similar_ids_list:
            insert_into_product_similar(my_connection, my_cursor, product_id, similar_id)
        
def map_category_product_list(my_connection, my_cursor, categories, product_id):
    if len(categories) == 0:
        None
    else:
        for category_index in range(len(categories)):
            child_category = categories[category_index]
            while (child_category.sub != None):
                child_category = child_category.sub
            insert_into_product_category(my_connection, my_cursor, product_id, child_category.id)
            
def map_category_list(my_connection, my_cursor, categories):
    if len(categories) == 0:
        None
    else:
        for category_index in range(len(categories)):
            child_category = categories[category_index]
            while (child_category != None):
                if child_category.parent_id == "":
                    insert_into_category(my_connection, my_cursor, child_category.name, child_category.id, None)
                else:
                    insert_into_category(my_connection, my_cursor, child_category.name, child_category.id, child_category.parent_id)
                child_category = child_category.sub
                
def map_review_list(my_connection, my_cursor, product_id, review_list):
    if len(review_list) == 0:
        None
    else:
        for review in review_list:
            review_date = datetime.strptime(review.date, "%Y-%m-%d")
            insert_into_review(my_connection, my_cursor, product_id, review_date, review.customer, review.rating, review.votes, review.helpful)
    


if __name__ == '__main__':

    create_database()
    create_database_ini(DATABASE_INI)
    create_user()
    config = load_config()
    my_connection = connect(config)
    my_cursor = create_cursor(my_connection)
    create_tables(my_connection,my_cursor)
    map_product_list(my_connection,my_cursor)
    close_cursor(my_cursor)
    close_connection(my_connection)