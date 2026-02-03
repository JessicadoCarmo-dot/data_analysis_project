from pyspark.sql import SparkSession                #importação do SparkSession
from pyspark.sql.functions import col, trim, lower   #funções para manipulação de colunas
import os                                          #biblioteca para manipulação de caminhos
import pycountry                                  #biblioteca para lidar com países

#Cria uma sessão do spark
spark = SparkSession.builder \
    .appName("LimpezaDeDados") \
    .master("local[*]") \
    .getOrCreate()


# Caminho do CSV original
diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
caminho_csv =r"dados tratados/covid19tweetslimposoriginal.csv"


# Ler CSV com Spark
df = spark.read.csv(
    caminho_csv,
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"'
)

print("Esquema do dataset original:")
df.printSchema()

linhas_iniciais = df.count()
print(f"➡ Linhas originais: {linhas_iniciais}")

# Limpeza básica, remove valores nulos e inválidos
valores_invalidos = ["NaN", "nan", "NULL", "null", "", " "]

for coluna in df.columns:
    df = df.withColumn(coluna, trim(col(coluna)))
    df = df.replace(valores_invalidos, None, subset=[coluna])

df = df.dropna(how='all').dropna().dropDuplicates()

# Remover colunas indesejadas
colunas_para_remover = [
    "user_followers", "user_friends", "user_favourites",
    "user_verified", "user_created", "retweets",
    "favorites", "is_retweet"
]
colunas_existentes = [c for c in colunas_para_remover if c in df.columns]
df = df.drop(*colunas_existentes)


# Filtrar apenas países válidos automaticamente

if "user_location" in df.columns:
    # Criar conjunto com todos os nomes oficiais de países (em minúsculas)
    paises_validos = set([c.name.lower() for c in pycountry.countries])

    # Converter o dataframe para pandas temporariamente
    df_pd = df.toPandas()

    # Função que verifica se o campo contém um país válido
    def contem_pais_valido(texto):
        if not isinstance(texto, str):
            return False
        texto_lower = texto.lower()
        for pais in paises_validos:
            if pais in texto_lower:
                return True
        return False

    # Aplicar filtro
    df_pd = df_pd[df_pd["user_location"].apply(contem_pais_valido)]
    print(f"🌍 Linhas restantes após filtrar países válidos: {len(df_pd)}")
else:
    print("⚠️ Coluna 'user_location' não encontrada.")
    df_pd = df.toPandas()


# Salvar CSV limpo
# ==========================
pasta_saida = os.path.join(diretorio_projeto, "output")
os.makedirs(pasta_saida, exist_ok=True)

caminho_saida = os.path.join(pasta_saida, "covid19tweetslimposoriginal3.csv")
df_pd.to_csv(caminho_saida, index=False, encoding="utf-8")

print(f" CSV limpo salvo com sucesso em: {caminho_saida}")


# Encerra o Spark
spark.stop()
