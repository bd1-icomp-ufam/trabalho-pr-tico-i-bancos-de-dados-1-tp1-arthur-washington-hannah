from configparser import ConfigParser
import psycopg2
import os

def resolve_path(file_name):
    current_dir_path = os.path.dirname(__file__)
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

def create_cursor(my_connection):
    try:
        my_cursor = my_connection.cursor()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
    
    return my_cursor

def query_1(cursor, product_id):

    query_high_rating = """
        SELECT REVIEW_ID, CUSTOMER_ID, REVIEW_RATING, HELPFUL, VOTE
        FROM REVIEW
        WHERE PRODUCT_ID = %s
        ORDER BY REVIEW_RATING DESC, HELPFUL DESC
        LIMIT 5;
    """

    # Query para obter os 5 comentários mais úteis e com menor avaliação
    query_low_rating = """
        SELECT REVIEW_ID, CUSTOMER_ID, REVIEW_RATING, HELPFUL, VOTE
        FROM REVIEW
        WHERE PRODUCT_ID = %s
        ORDER BY REVIEW_RATING ASC, HELPFUL DESC
        LIMIT 5;
    """

    # Executar a query para comentários com maior avaliação
    cursor.execute(query_high_rating, (product_id,))
    high_rating_reviews = cursor.fetchall()

    # Executar a query para comentários com menor avaliação
    cursor.execute(query_low_rating, (product_id,))
    low_rating_reviews = cursor.fetchall()

    def print_reviews(title, reviews):
        # Cabeçalho
        print(title)
        
        # Linhas da tabela (reviews)
        for review in reviews:
            print(f"REVIEW_ID    : {review[0]}")
            print(f"CUSTOMER_ID  : {review[1]}")
            print(f"REVIEW_RATING: {review[2]}")
            print(f"HELPFUL      : {review[3]}")
            print(f"VOTE         : {review[4]}")
            print("-" * 40)  # Linha de separação entre as reviews

    # Exibir os resultados
    print_reviews("\n\n5 Comentários mais úteis com maior avaliação:\n", high_rating_reviews)
    print_reviews("\n\n5 Comentários mais úteis com menor avaliação:\n", low_rating_reviews)
    
        
def query_2(cursor, product_id):

    query_similar_products = """
        SELECT p_sim.SIMILAR_ASIN, p2.TITLE, p2.SALES_RANK
        FROM PRODUCT p
        JOIN PRODUCT_SIMILAR p_sim ON p.PRODUCT_ID = p_sim.PRODUCT_ID
        JOIN PRODUCT p2 ON p_sim.SIMILAR_ASIN = p2.ASIN
        WHERE p.PRODUCT_ID = %s
        AND p2.SALES_RANK < p.SALES_RANK;
    """

    # Executar a query
    cursor.execute(query_similar_products, (product_id,))
    similar_products = cursor.fetchall()

    def print_similar_products(title, products):
        print(title)
        
        for product in products:
            print(f"SIMILAR_ASIN : {product[0]}")
            print(f"TÍTULO       : {product[1]}")
            print(f"SALES_RANK   : {product[2]}")
            print("-" * 40)  # Linha de separação entre os produtos

    print_similar_products("\nProdutos similares com maiores vendas:\n", similar_products)


def query_3(cursor, product_id):
    query_avg_rating = """ SELECT REVIEW_1.REVIEW_DATE, avg(REVIEW_2.REVIEW_RATING)
                           FROM REVIEW AS REVIEW_1 ,REVIEW AS REVIEW_2
                           WHERE REVIEW_1.REVIEW_DATE >= REVIEW_2.REVIEW_DATE
                           and REVIEW_1.PRODUCT_ID = %s
                           and REVIEW_2.PRODUCT_ID = REVIEW_1.PRODUCT_ID
                           GROUP BY REVIEW_1.REVIEW_DATE
                           ORDER BY REVIEW_1.REVIEW_DATE"""

    cursor.execute(query_avg_rating, (product_id,))
    ratings = cursor.fetchall()


    print("EVOLUÇÃO DAS MÉDIAS:")
    for rating in ratings:
        print(f"DATE: {rating[0]}, RATING_AVG: {rating[1]}")
        
def query_4(cursor):
    my_query = """  
              WITH RankedProducts AS (
              SELECT 
                  ASIN,
                  TITLE,
                  PRODUCT_GROUP,
                  SALES_RANK,
                  ROW_NUMBER() OVER (PARTITION BY PRODUCT_GROUP ORDER BY SALES_RANK) AS rank
              FROM 
                  PRODUCT
      )
      SELECT 
          ASIN,
          TITLE,
          PRODUCT_GROUP,
          SALES_RANK
      FROM 
          RankedProducts
      WHERE 
          rank <= 10
      ORDER BY 
          PRODUCT_GROUP, SALES_RANK"""

    cursor.execute(my_query)
    answers = cursor.fetchall()
    
    def print_ranked_products(title, products):
        print(title)
        
        # Linhas da tabela (products)
        for product in products:
            if product[1] and product[2] and product[3]:
                print(f"ASIN          : {product[0]}")
                print(f"TÍTULO        : {product[1]}")
                print(f"PRODUCT_GROUP : {product[2]}")
                print(f"SALES_RANK    : {product[3]}")
                print("-" * 40)  # Linha de separação entre os produtos

    print_ranked_products("Top 10 Produtos por Grupo de Produto:", answers)
            
def query_5(cursor):
    my_query = """
    SELECT 
    P.TITLE, 
        AVG(R.HELPFUL) AS AVG_HELPFUL
    FROM 
        PRODUCT P
    JOIN 
        REVIEW R ON P.PRODUCT_ID = R.PRODUCT_ID
    WHERE 
        R.REVIEW_RATING > 0  -- Considera apenas reviews com rating maior que zero
    GROUP BY 
        P.TITLE
    HAVING 
        AVG(R.HELPFUL) > 0  -- Garante que só produtos com avaliações úteis sejam considerados
    ORDER BY 
        AVG_HELPFUL DESC
    LIMIT 10;
    """
    cursor.execute(my_query)
    answers = cursor.fetchall()
    
    def print_avg_helpful_products(title, products):
        # Cabeçalho
        print(title)
        
        # Linhas da tabela (products)
        for product in products:
            print(f"TÍTULO       : {product[0]}")
            print(f"AVG_HELPFUL  : {product[1]:.2f}")
            print("-" * 40)  # Linha de separação entre os produtos

    print_avg_helpful_products("Top 10 Produtos com a Maior Média de Avaliações Úteis:", answers)
        
def query_6(cursor):
    my_query = """
        SELECT 
        P.PRODUCT_GROUP, 
        AVG(R.HELPFUL) AS AVG_HELPFUL
    FROM 
        PRODUCT P
    JOIN 
        REVIEW R ON P.PRODUCT_ID = R.PRODUCT_ID
    WHERE 
        R.REVIEW_RATING >= 1  -- Considera apenas avaliações com rating positivo
    GROUP BY 
        P.PRODUCT_GROUP
    HAVING 
        AVG(R.HELPFUL) > 0  -- Garante que apenas grupos com médias úteis positivas sejam considerados
    ORDER BY 
        AVG_HELPFUL DESC
    LIMIT 5;
    """
    cursor.execute(my_query)
    answers = cursor.fetchall()

    def print_avg_helpful_groups(title, groups):
        # Cabeçalho
        print(title)
        
        # Linhas da tabela (groups)
        for group in groups:
            print(f"GRUPO DE PRODUTO : {group[0]}")
            print(f"AVG_HELPFUL      : {group[1]:.2f}")
            print("-" * 40)  # Linha de separação entre os grupos
            
    print_avg_helpful_groups("Top 5 Grupos de Produtos com a Maior Média de Avaliações Úteis: \n", answers)
        
def query_7(cursor):
    my_query = """
        WITH RankedReviews AS (
    SELECT 
        P.PRODUCT_GROUP, 
        R.CUSTOMER_ID, 
        COUNT(R.REVIEW_ID) AS TOTAL_REVIEWS,
        ROW_NUMBER() OVER (PARTITION BY P.PRODUCT_GROUP ORDER BY COUNT(R.REVIEW_ID) DESC) AS RANK
    FROM 
        PRODUCT P
    JOIN 
        REVIEW R ON P.PRODUCT_ID = R.PRODUCT_ID
    GROUP BY 
        P.PRODUCT_GROUP, 
        R.CUSTOMER_ID
)
SELECT 
    PRODUCT_GROUP, 
    CUSTOMER_ID, 
    TOTAL_REVIEWS
FROM 
    RankedReviews
WHERE 
    RANK <= 10
ORDER BY 
    PRODUCT_GROUP, 
    TOTAL_REVIEWS DESC;
    """
    cursor.execute(my_query)
    answers = cursor.fetchall()
    
    def print_ranked_reviews(title, reviews):
        # Cabeçalho
        print(title)
        
        # Linhas da tabela (reviews)
        for review in reviews:
            print(f"GRUPO DE PRODUTO : {review[0]}")
            print(f"ID DO CLIENTE    : {review[1]}")
            print(f"TOTAL DE REVIEWS : {review[2]}")
            print("-" * 40)  # Linha de separação entre os reviews

    print_ranked_reviews("Top 10 Clientes com Mais Reviews por Grupo de Produto:", answers)
    
        
def menu():
    print()
    
    print("|--------------------------------------MENU--------------------------------------|")
    print("1 - Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
    print("2 - Dado um produto, listar os produtos similares com maiores vendas do que ele")
    print("3 - Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
    print("4 - Listar os 10 produtos líderes de venda em cada grupo de produtos")
    print("5 - Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
    print("6 - Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
    print("7 - Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    print("8 - SAIR")
    
    print()
    
if __name__ == '__main__':
    config = load_config()
    my_connection = connect(config)
    my_cursor = create_cursor(my_connection)
    
    menu()
    option = input("Digite a opção desejada: ")
    print()
    
    while option != '8':
        match option:
            case '1':
                product_id = int(input("Insira o ID do produto: "))
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_1(my_cursor, product_id)
                print()
                input("Pressione enter para voltar ao menu... ")
                os.system("clear")
            case '2':
                product_id = int(input("Insira o ID do produto: "))
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_2(my_cursor, product_id)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '3':
                product_id = int(input("Insira o ID do produto: "))
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_3(my_cursor, product_id)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '4':
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_4(my_cursor)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '5':
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_5(my_cursor)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '6':
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_6(my_cursor)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '7':
                print()
                print("--------------RESPOSTA DA CONSULTA--------------")
                print()
                query_7(my_cursor)
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
            case '8':
                break
            case _:
                print()
                print("Opção inválida. Por favor, tente novamente.")
                print()
                input("Pressione Enter para voltar ao menu... ")
                os.system("clear")
        
        menu()
        option = input("Digite a opção desejada: ")
                
    close_connection(my_connection)