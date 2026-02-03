#ANALISE DOS MEIOS DE ACESSO AO TWITTER
import pandas as pd                                    #biblioteca para manipulação de dados
 
data = pd.read_csv(r"dados tratados/covid19tweetslimposoriginal.csv")

# Limpeza na coluna 'source'
data['source'] = data['source'].fillna('').str.lower()

#Função para identificar o meio de acesso
def identificar_acesso(fonte):
    fonte = str(fonte).lower()
    if "iphone" in fonte:
        return "iPhone"
    elif "android" in fonte:
        return "Android"
    elif "web" in fonte:
        return "Web"
    else:
        return "Outros"
 
data["Acesso"] = data["source"].apply(identificar_acesso)
 
# Contagem dos meios de acesso
print("Meios de acesso ao Twitter:")
print(data["Acesso"].value_counts())


#ANALISE DE SENTIMENTO SIMPLES COM TEXTBLOB E PYSPARK

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.functions import length        #para filtrar textos vazios

#Criar SparkSession
spark = SparkSession.builder \
.appName("Senrimentos Simples")\
.master("local[*]") \
.getOrCreate() 
# Lendo dados (exemplo CSV com coluna 'text')
df = spark.read.csv(r"dados tratados/covid19tweetslimposoriginal.csv", header=True, inferSchema=True)
df = df.na.drop(subset=["text"])      # elimina só textos realmente nulos
df = df.filter(length(col("text")) > 1)  # elimina apenas textos vazios de verdade


# Função Python para análise de sentimento
def analisar_sentimento(texto):
    if texto:
        sentimento = TextBlob(texto).sentiment.polarity
        if sentimento > 0:
            return "Positivo"
        elif sentimento < 0:
            return "Negativo"
        else:
            return "Neutro"
    return "Neutro"


# Registrar UDF
sentiment_udf = udf(analisar_sentimento, StringType())

# Criar nova coluna com sentimento
df_sentimento = df.withColumn("sentimento", sentiment_udf(df["text"]))

# Mostrar resultado
df_sentimento.select("text", "sentimento").show(100, truncate=False)


#ANALISE DE PAÍSES COM MAIS TWEETS SOBRE COVID-19
import pandas as pd          #biblioteca para manipulação de dados
import plotly.express as px        #biblioteca para visualização de dados

# Carregar dados tratados
tweets = pd.read_csv(
r"dados tratados/covid19tweetslimposoriginal.csv"
)

# Limpeza na coluna 'user_location'
tweets['user_location'] = tweets['user_location'].fillna('').str.lower()

#-- Dicionário de países e suas variações comuns
paises = {
    'brazil': 'Brasil',
    'brasil': 'Brasil',
    'portugal': 'Portugal',
    'usa': 'Estados Unidos',
    'america': 'Estados Unidos',
    'united states': 'Estados Unidos',
    'california': 'Estados Unidos',
    'colorado': 'Estados Unidos',
    'kansas': 'Estados Unidos',
    'england': 'Reino Unido',
    'london': 'Reino Unido',
    'scotland': 'Reino Unido',
    'ireland': 'Irlanda',
    'australia': 'Austrália',
    'peru': 'Peru',
    'india': 'Índia',
    'malaysia': 'Malásia',
    'canada': 'Canadá',
    'nigeria': 'Nigéria',
    'cameroon': 'Camarões',
    'singapore': 'Singapura',
    'kenya': 'Quênia','switzerland': 'Suíça'
}

#-- Função para detectar país na localização do usuário
def detectar_pais(loc):
    for chave, nome in paises.items():
        if chave in loc:
            return nome
    return None 

tweets['pais'] = tweets['user_location'].apply(detectar_pais)

#-- Filtrar apenas tweets com país identificado
tweets = tweets.dropna(subset=['pais'])

# --- Contar tweets por país
contagem = tweets['pais'].value_counts().reset_index()
contagem.columns = ['País', 'Nº de Tweets']

print("Análise concluída")
print(contagem.head(20))

#ANÁLISE DE PALAVRAS MAIS USADAS COM MAPREDUCE
from mrjob.job import MRJob             #-- Biblioteca MRJob para MapReduce em Python
import re                               #-- Biblioteca para expressões regulares
import pandas as pd                     # =-- Biblioteca para manipulação de dados

#regex para identificar palavras (mínimo 3 letras, incluindo acentos)
palavra_regex = re.compile(r"\b[a-zA-Zá-úÁ-Ú]{3,}\b")

#lista de stopwords comuns em inglês e termos irrelevantes
stopwords = {
    'the', 'and', 'for', 'are', 'you', 'with', 'this', 'that', 'from', 'was', 'have', 'has',
    'but', 'not', 'all', 'will', 'they', 'his', 'her', 'she', 'him', 'our', 'out', 'who',
    'about', 'what', 'when', 'where', 'how', 'why', 'can', 'just', 'get', 'your', 'their',
    'had', 'been', 'also', 'into', 'were', 'more', 'one', 'some', 'like', 'now', 'than',
    'its', 'covid', 'co', 'amp', 'https', 't', 'in', 'on', 'of', 'to', 'is', 'it', 'a', 'an',
    'new', 'day', 'last',
}

# Definição da classe MRJob
class MRContadorPalavras(MRJob):
    """
    Classe principal que implementa o MapReduce com MRJob.
    """

    def mapper(self, _, linha):
        """
        Fase MAP: recebe cada linha (tweet) e emite (palavra, 1) para cada palavra válida.
        """
        for palavra in palavra_regex.findall(linha.lower()):
            if palavra not in stopwords:
                yield (palavra, 1)

    def reducer(self, palavra, contagens):
        """
        Fase REDUCE: soma todas as ocorrências da mesma palavra.
        """                
        yield (palavra, sum(contagens))

# Execução do job MRJob
if __name__ == '__main__':
    # Caminho do arquivo CSV original
    caminho_csv = r"dados tratados/covid19tweetslimposoriginal.csv"

    #ler apenas a coluna 'text' do CSV
    df = pd.read_csv(caminho_csv, usecols=['text'])
    df['text'] = df['text'].fillna('')
    caminho_tmp = "tweets_texto.txt"
    df['text'].to_csv(caminho_tmp, index=False, header=False)

    # Executar o job MRJob
    job = MRContadorPalavras(args=[caminho_tmp])
    palavras_contadas = []

    with job.make_runner() as runner:
        runner.run()
        for chave, valor in job.parse_output(runner.cat_output()):
            palavras_contadas.append((chave, valor))

    # Converter resultado em DataFrame
    df_resultado = pd.DataFrame(palavras_contadas, columns=['palavra', 'quantidade'])

    # Top 25 palavras mais usadas
    df_top25 = df_resultado.nlargest(25, 'quantidade')

    # Exibir resultado final
    print("\n🔝 Top 25 palavras mais utilizadas:")
    print(df_top25.to_string(index=False))


# CÓDIGO DASHBOARD INTERATIVO 
from dash import Dash, html, dcc                             #bibliotecas do Dash para criar o dashboard
from dash.dependencies import Input, Output                  #bibliotecas para interatividade no Dash
import plotly.express as px                                  #biblioteca para gráficos interativos

#conversão dos DataFrames para uso no Dash
df_sentimento_pd = df_sentimento.toPandas()                  #DataFrame de sentimentos em Pandas
contagem_paises = contagem                                   #DataFrame de contagem de países
df_top25_palavras = df_top25                                 #DataFrame de top 25 palavras
meios_acesso = data["Acesso"].value_counts().reset_index()   #DataFrame de meios de acesso
meios_acesso.columns = ["Acesso", "Quantidade"]              #Renomear colunas


# Criação do App Dash
app = Dash(__name__)
app.title = " Dashboard COVID-19 Tweets"

# Layout principal com HTML e CSS na pasta assets

app.layout = html.Div(
    className="container",
    children=[
        html.Header("Análise Exploratória de Tweets relacionados à COVID-19", className="titulo"),

       # 🔸 Filtro para a parte da análise de sentimentos
        html.Div(
            className="filtros",
            children=[
                html.Label("Filtrar Sentimento:"),
                dcc.Dropdown(
                    id="filtro_sentimento",
                    options=[
                        {"label": "Todos", "value": "Todos"},
                        {"label": "Positivo", "value": "Positivo"},
                        {"label": "Neutro", "value": "Neutro"},
                        {"label": "Negativo", "value": "Negativo"},
                    ],
                    value="Todos",
                    className="dropdown",
                ),
            ],
        ),

        # 🔸 Gráficos
        html.Div(
            className="graficos",
            children=[
                # Análise de sentimentos
                dcc.Graph(id="grafico_sentimentos"),

                # Países com mais discussões ativas
                dcc.Graph(
                    figure=px.bar(
                        contagem_paises,
                        x="País",
                        y="Nº de Tweets",
                        color="País",
                        title="Volume de Tweets por País",
                        template="plotly_dark",
                    ).update_layout(paper_bgcolor="#111", plot_bgcolor="#111"),
                    id="grafico_paises",
                ),

                # Meios de acesso
                dcc.Graph(
                    figure=px.pie(
                        meios_acesso,
                        names="Acesso",
                        values="Quantidade",
                        title="Plataformas Utilizadas para Publicação dos Tweets",
                        template="plotly_dark",
                    ).update_layout(paper_bgcolor="#111", plot_bgcolor="#111"),
                    id="grafico_acesso",
                ),

                # Top 25 palavras mais usadas
                dcc.Graph(
                    figure=px.bar(
                        df_top25_palavras,
                        x="palavra",
                        y="quantidade",
                        title=" Termos Mais Frequentes (Processamento com MapReduce)",
                        template="plotly_dark",
                    ).update_layout(paper_bgcolor="#111", plot_bgcolor="#111"),
                    id="grafico_palavras",
                ),
            ],
        ),

        html.Footer("© 2025 | Projeto de Big Data desenvolvido por: Amanda Maciel, Daniella Justino, Jessica do Carmo e Suély Monteiro",className="rodape")
    ],
)
# Callback para atualizar o gráfico de sentimentos com base no filtro selecionado
@app.callback(
    Output("grafico_sentimentos", "figure"),
    Input("filtro_sentimento", "value")
)
def atualizar_grafico(sentimento):
    df_base = df_sentimento_pd.copy()

    # Segurança contra None
    if sentimento is None:
        sentimento = "Todos"

    # Cores para os sentimentos
    cores_sentimentos = {
        "Positivo": "#00ff99",
        "Negativo": "#ff0000",
        "Neutro": "#ffe138"
    }

    # Contagem dos sentimentos
    contagem = df_base["sentimento"].value_counts().reset_index()
    contagem.columns = ["Sentimento", "Quantidade"]

    # Capitalizar os nomes dos sentimentos
    contagem["Sentimento"] = contagem["Sentimento"].str.capitalize()

    # Filtro
    if sentimento != "Todos":
        contagem = contagem[contagem["Sentimento"] == sentimento]

        # Cor única
        cor_usada = [cores_sentimentos[sentimento]]
    else:
    # Cor para múltiplos sentimentos
        cor_usada = [cores_sentimentos[s] for s in contagem["Sentimento"]]

    fig = px.bar(
        contagem,
        x="Sentimento",
        y="Quantidade",
        title="Análise de sentimentos",
        color="Sentimento",
        color_discrete_sequence=cor_usada
    )

    fig.update_layout(
        paper_bgcolor="#0d1117",
        plot_bgcolor="#0d1117",
        font_color="white"
    )
    return fig


# Executar servidor
if __name__ == "__main__":
    app.run(debug=False)       #para o código não ficar rodando após executar o dash, por isso o "debug=False"

    
