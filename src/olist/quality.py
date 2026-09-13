"""Validação de qualidade com quarentena rastreável."""
import datetime
from typing import Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def aplica_regras(df: DataFrame, regras: Dict[str, Column]):
    """Separa o DataFrame em válidos e inválidos.

    `regras` é um dicionário onde a chave é o nome da regra (usado
    depois para explicar o motivo da rejeição) e o valor é a condição
    em si (uma coluna booleana). Uma linha só é válida se passar em
    TODAS as regras.

    Cada linha inválida recebe a coluna _motivo_rejeicao com o nome
    de cada regra que ela não passou, separados por ponto e vírgula.

    Retorna uma tupla (validos, invalidos).
    """
    # Sem regras, não tem o que validar: devolve o df original como
    # válido e um df vazio (mesmo schema) como inválido.
    if not regras:
        df_invalidos_vazio = df.limit(0).withColumn(
            "_motivo_rejeicao", F.lit(None).cast("string")
        )
        return df, df_invalidos_vazio

    # Junta todas as condições em uma só usando "&".
    # A linha só passa se cond_ok for True em todas as regras.
    cond_ok = None
    for condicao in regras.values():
        if cond_ok is None:
            cond_ok = condicao
        else:
            cond_ok = cond_ok & condicao

    validos = df.filter(cond_ok)

    # Para cada regra que falhou (condicao), guarda o nome dela.
    # concat_ws junta os nomes com ";" e ignora os valores nulos
    # (ou seja, as regras que a linha passou).
    nomes_das_regras_que_falharam = []
    for nome, condicao in regras.items():
        nomes_das_regras_que_falharam.append(F.when(~condicao, F.lit(nome)))
    motivo = F.concat_ws(";", *nomes_das_regras_que_falharam)

    invalidos = (
        df.filter(~cond_ok)
        .withColumn("_motivo_rejeicao", motivo)
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    return validos, invalidos


def deduplica(df: DataFrame, chaves: list, coluna_ordem: str) -> DataFrame:
    """Mantém apenas o registro mais recente por chave de negócio.

    `chaves` identifica o registro (ex.: id do pedido) e
    `coluna_ordem` diz qual registro é o "mais recente" entre os
    duplicados (ex.: data de atualização).
    """
    # Numera as linhas de cada grupo de chave, da mais recente (1)
    # para a mais antiga, e mantém só a linha número 1 de cada grupo.
    janela = Window.partitionBy(*chaves).orderBy(F.col(coluna_ordem).desc())
    return (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def registra_execucao(spark, batch_id, camada, tabela,
    lidas, gravadas, rejeitadas, inicio):
    """Grava uma linha na tabela de controle do pipeline."""
    fim = datetime.datetime.now()

    linha = [(
        batch_id, camada, tabela,
        int(lidas), int(gravadas), int(rejeitadas),
        inicio, fim,
    )]
    colunas = [
        "batch_id", "camada", "tabela", "linhas_lidas",
        "linhas_gravadas", "linhas_rejeitadas",
        "inicio", "fim",
    ]

    df_execucao = spark.createDataFrame(linha, colunas).withColumn(
        "duracao_seg", F.unix_timestamp("fim") - F.unix_timestamp("inicio")
    )

    df_execucao.write.format("delta").mode("append").saveAsTable(
        "olist_gold.control.pipeline_runs"
    )
