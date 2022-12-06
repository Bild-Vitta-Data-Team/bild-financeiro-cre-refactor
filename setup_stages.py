from platform import system
from Stages import (
    STG_MEGA_AGENTES,
    STG_MEGA_BOLETAGEM,
    STG_MEGA_CONTRATO_CLIENTE,
    STG_MEGA_CLASSIFICACAO_CONTRATO,
    STG_MEGA_PARCELA_RECEITA,
    STG_MEGA_RENEGOCIACAO_PAR,
)

def set_cre_pipeline(*args):
    if system().upper() == 'WINDOWS':
        return {arg.__file__.split('\\')[-1].replace('.py', ''): arg for arg in args}
    else:
        return {arg.__file__.split('/')[-1].replace('.py', ''): arg for arg in args}


PIPE = (
    set_cre_pipeline(
        STG_MEGA_AGENTES,
        STG_MEGA_BOLETAGEM,
        STG_MEGA_CONTRATO_CLIENTE,
        STG_MEGA_CLASSIFICACAO_CONTRATO,
        STG_MEGA_PARCELA_RECEITA,
        STG_MEGA_RENEGOCIACAO_PAR,
    )
)