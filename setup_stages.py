from platform import system
from Stages.BaseOracle import BaseOracle
from Stages.BaseSys import BaseSys
from Stages import (
    STG_SYS_T_VENDA,
    STG_MEGA_AGENTES,
    STG_MEGA_BOLETAGEM,
    STG_MEGA_CONTRATO_CLIENTE,
    STG_MEGA_CLASSIFICACAO_CONTRATO,
    STG_MEGA_PARCELA_RECEITA,
    STG_MEGA_RENEGOCIACAO_PAR
)

def set_cre_pipeline(*args):
    if system().upper() == 'WINDOWS':
        return {arg.__file__.split('\\')[-1].replace('.py', ''): arg for arg in args}
    else:
        return {arg.__file__.split('/')[-1].replace('.py', ''): arg for arg in args}

def set_cre_infos(pipe: dict):
    _pipe, _pipes = {}, []
    for n, m in pipe.items():
        if n.startswith("STG_MEGA"): 
            _pipe["JOB"] = BaseOracle
            _pipe["NAME"] = "STGBILD."+n
            _pipe["PROCESS"] = m
        
        elif n.startswith("STG_SYS"): 
            _pipe["JOB"] = BaseSys
            _pipe["NAME"] = "STGBILD."+n
            _pipe["PROCESS"] = m

        else:
            _pipe["JOB"] = "ARQ"
            _pipe["NAME"] = "STGBILD."+n
            _pipe["PROCESS"] = m

        _pipes.append(_pipe)

    return _pipes


pipe = (
    set_cre_pipeline(
        STG_SYS_T_VENDA,
        STG_MEGA_AGENTES,
        STG_MEGA_BOLETAGEM,
        STG_MEGA_CONTRATO_CLIENTE,
        STG_MEGA_CLASSIFICACAO_CONTRATO,
        STG_MEGA_PARCELA_RECEITA,
        STG_MEGA_RENEGOCIACAO_PAR,
    )
)

PIPE = set_cre_infos(pipe)