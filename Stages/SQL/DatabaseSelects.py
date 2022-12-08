
# Ordem alfabética das stages do CRE adaptadas e revisadas

AGENTES = """
( SELECT
    AGN_TAB_IN_CODIGO,
    AGN_PAD_IN_CODIGO,
    AGN_IN_CODIGO,
    AGN_BO_CONSOLIDADOR,
    AGN_ST_FANTASIA,
    AGN_ST_NOME,
    AGN_CH_TIPOPESSOAFJ,
    AGN_IN_NATJURID,
    AGN_ST_INSCRMUNIC,
    UF_ST_SIGLA,
    PA_ST_SIGLA,
    MUN_IN_CODIGO,
    AGN_ST_MUNICIPIO,
    TPL_ST_SIGLA,
    AGN_ST_LOGRADOURO,
    AGN_ST_NUMERO,
    AGN_ST_BAIRRO,
    AGN_ST_CEP,
    AGN_ST_CXPOSTAL,
    AGN_ST_COMPLEMENTO,
    AGN_ST_CEPCXPOSTAL,
    AGN_ST_REFERENCIA,
    AGN_ST_ENQUADRAIPI,
    AGN_ST_ENQUADRAICMS,
    AGN_ST_ENQUADRAISS,
    AGN_ST_CGC,
    AGN_CH_STATUSCGC,
    AGN_ST_FRETEPESOVALOR,
    AGN_ST_INSCRESTADUAL,
    PAI_AGN_TAB_IN_CODIGO,
    AGN_ST_ATIVECONOMICA,
    PAI_AGN_PAD_IN_CODIGO,
    PAI_AGN_IN_CODIGO,
    AGN_ST_SUFRAMA,
    AGN_ST_REPARTICAO,
    AGN_ST_EMAIL,
    AGN_BO_IPISIMPLES,
    AGN_BO_ICMSSIMPLES,
    AGN_BO_ISSSIMPLES,
    AGN_BO_SIMPLES,
    FORP_IN_CODIGO,
    AGN_BO_FLUXO,
    AGN_IN_NIVEL,
    AGN_ST_ORDEM,
    AGN_DT_ULTIMAATUCAD,
    AGN_IN_VALIDADECAD,
    AGN_ST_CEI,
    AGN_CH_TIPOINSCRICAO,
    AGN_CH_ENQUADRAMENTO,
    CNAE_ST_CODIGO,
    AGN_BO_RETERIR,
    AGN_BO_RETERINSS,
    AGN_ST_URL,
    AGN_ST_APELIDO,
    AGN_ST_SENHA,
    AGN_CH_STATUSINSCRESTADUAL,
    AGN_BO_PUBLICO,
    AGN_ST_INSCRPRODUTOR,
    USU_IN_INCLUSAO,
    AGN_CH_RURALTIPOPESSOAFJ,
    TAB05_IN_CODIGO,
    AGN_ST_NRCAEPF,
    AGN_ST_NRCNO,
    AGN_CH_TIPODESPADUANEIRA,
    AGN_IN_TIPOCAEPF,
    AGN_CH_NATRETPISCOFINS,
    AGN_BO_DISPENSADONIF,
    AGN_ST_NIF,
    DFP_IN_CODIGO,
    AGN_BO_RATEIOAFRMM,
    AGN_CH_TIPOAFRMM,
    AGN_ST_PISMEI,
    AGN_ST_CBOMEI
FROM 
    bild.glo_agentes ) AGENTES
""".replace('\n', ' ').replace('\t', ' ').strip()


BOLETAGEM = """
(SELECT
    ORG_TAB_IN_CODIGO,
    ORG_PAD_IN_CODIGO,
    ORG_IN_CODIGO,
    ORG_TAU_ST_CODIGO,
    CTO_IN_CODIGO,
    PAR_IN_CODIGO,
    FIL_IN_CODIGO,
    par_dt_vencimento,
    ban_in_numero,
    CTO_DT_CADASTRO,
    PAR_DT_MOVIMENTO,
    PAR_DT_BAIXA,
    CTO_RE_VALORCONTRATO_ORI,
    CTO_CH_TIPO,
    CTO_CH_STATUS,
    PAR_CH_PARCELA,
    PAR_CH_RECEITA,
    PAR_CH_ORIGEM,
    PAR_CH_PROCESSO,
    AGN_IN_CODIGO,
    UND_CODIGO,
    VALOR_ORIGINAL,
    VALOR_CORRIGIDO,
    VALOR_ATUALIZADO,
    VALOR_JUROS,
    VALOR_MULTA,
    VALOR_ATRASO,
    VALOR_DESCONTO,
    RESIDUO_PREVISTO,
    ORIGEM,
    PROCESSO,
    csf_st_descricao,
    pro_in_reduzido,
    pro_ide_st_codigo,
    pro_pad_in_codigo,
    pro_tab_in_codigo
FROM bild.vw_bld_bca_cre_bolet_bi 
WHERE PAR_DT_VENCIMENTO between '{}' and '{}' ) BOLETAGEM
""".replace('\n', ' ').replace('\t', ' ').strip()


CONTRATO_CLIENTE = """
( SELECT 
    ORG_TAB_IN_CODIGO,
    ORG_PAD_IN_CODIGO,
    ORG_IN_CODIGO,
    ORG_TAU_ST_CODIGO,
    CTO_IN_CODIGO,
    AGN_TAB_IN_CODIGO,
    AGN_PAD_IN_CODIGO,
    AGN_IN_CODIGO,
    AGN_TAU_ST_CODIGO
FROM 
    bild.vw_bld_bca_cre_cto_cli_bi ) CONTRATO_CLIENTE
""".replace('\n', ' ').replace('\t', ' ').strip()


CLASSIFICACAO_CONTRATO = """
( SELECT 
    CSF_IN_CODIGO,
    CSF_ST_DESCRICAO,
    ACAO_TAB_IN_CODIGO,
    ACAO_PAD_IN_CODIGO,
    ACAO_IN_CODIGO,
    CLA_TAB_IN_CODIGO,
    CLA_PAD_IN_CODIGO,
    MUL_CLA_IDE_ST_CODIGO,
    MUL_CLA_IN_REDUZIDO,
    SIN_CLA_IDE_ST_CODIGO,
    SIN_CLA_IN_REDUZIDO,
    ATR_CLA_IDE_ST_CODIGO,
    ATR_CLA_IN_REDUZIDO,
    ANT_CLA_IDE_ST_CODIGO,
    ANT_CLA_IN_REDUZIDO,
    AVI_CLA_IDE_ST_CODIGO,
    AVI_CLA_IN_REDUZIDO,
    CSF_BO_VINCULATAXACTO
FROM 
    bild.dbm_classificacao ) CLASSIFICACAO_CONTRATO 
""".replace('\n', ' ').replace('\t', ' ').strip()

RENEGOCIACAO_PAR = """
(
SELECT
    ORG_TAB_IN_CODIGO,
    ORG_PAD_IN_CODIGO,
    ORG_IN_CODIGO,
    ORG_TAU_ST_CODIGO,
    CTO_IN_CODIGO,
    REN_IN_CODIGO,
    PRO_TAB_IN_CODIGO,
    PRO_PAD_IN_CODIGO,
    PRO_IDE_ST_CODIGO,
    PRO_IN_REDUZIDO,
    DT_GERACAO,
    STATUS,
    DT_STATUS,
    REN_IN_CODIGOWEB,
    PARCELA_ORIGEM,
    VALOR_ORIGEM,
    TIPO_ORIGEM,
    VENCIMENTO_ORIGEM,
    PARCELA_DESTINO,
    VALOR_DESTINO,
    TIPO_DESTINO,
    VENCIMENTO_DESTINO
FROM bild.vw_bld_bca_renegociacao
WHERE DT_GERACAO between '{}' AND '{}'
) RENEGOCIACAO_PAR
""".replace('\n', ' ').replace('\t', ' ').strip()


PARCELA_RECEITA = """
( Select
    distinct
      p.ORG_TAB_IN_CODIGO
    , p.ORG_PAD_IN_CODIGO
    , p.ORG_IN_CODIGO
    , p.ORG_TAU_ST_CODIGO
    , p.CTO_IN_CODIGO
    , p.PAR_IN_CODIGO
    , p.CND_IN_CODIGO
    , p.CNDIT_IN_CODIGO
    , p.PAR_BO_GERARESIDUO
    , p.PAR_BO_TABELAPRICE
    , p.PAR_CH_PARCELA
    , p.PAR_CH_RECEITA
    , p.PAR_CH_ORIGEM
    , p.PAR_CH_STATUS
    , p.PAR_DT_STATUS
    , p.PAR_DT_GERACAO
    , p.PAR_DT_VENCIMENTO
    , p.PAR_DT_MOVIMENTO
    , p.PAR_RE_VALORORIGINAL
    , p.PAR_RE_VALORPAGO
    , p.PAR_RE_VALORMULTA
    , p.PAR_RE_VALORATRASO
    , p.PAR_RE_VALORJUROS
    , p.PAR_RE_VALORJUROSBX
    , p.PAR_RE_VALORCORRECAO
    , p.PAR_RE_VALORCORRECAOBX
    , p.PAR_RE_VALORDESCONTO
    , p.PAR_RE_VALORRESIDUO
    , p.PAR_RE_VALORJUROSTP
    , p.PAR_RE_VALORJUROSREN
    , p.PAR_RE_RESIDUOCOBRANCA
    , p.PAR_DT_BAIXA
    , p.PAR_CH_RECEITABAIXA
    , p.BAN_IN_NUMERO
    , p.PAR_ST_AGENCIA
    , p.PAR_ST_CONTA
    , p.PAR_ST_CHEQUE
    , p.PAR_ST_OBSERVACAO
    , p.PAR_BO_SELECIONADO
    , p.PAI_CTO_IN_CODIGO
    , p.PAI_PAR_IN_CODIGO
    , p.PAR_IN_RECIBO
    , p.PAR_ST_OBS_CLIENTE
    , p.PAR_BO_CONTRATUAL
    , p.PAR_IN_RESIDUO
    , p.PAR_BO_REAJUSTADA
    , p.PAR_RE_CREDITO
    , p.PAR_ST_HISTORICO
    , p.PAR_RE_JROTPNCOBRADO
    , p.PAR_RE_DIFTAXA
    , p.CTT_IN_CODIGO
    , p.PAR_DT_DEPOSITO
    , p.PAR_CH_PROCESSO
    , p.PAR_DT_PROCESSO
    , p.PAR_DT_REAJUSTE
    , p.PAR_DT_DESCONGELA
    , p.PAR_CH_PRORATABX
    , p.PAR_BO_CONFDIVIDA
    , p.PAR_BO_CORRIGEVCTO
    , p.PAR_ST_OBSCORRIGEVCTO
    , p.PAR_RE_CORRECAOVCTO
    , p.PAR_RE_JUROSVENCTO
    , p.PAR_RE_PERCVMJUROS
    , p.RESCOB_IN_CODIGO
    , p.PAR_BO_PAGOWEB
    , p.PAR_DT_REALIZACAOBX
    , p.PAR_RE_VALORCORRECAO_ATR
    , p.PAR_BO_RESCOB
    , p.REA_IN_CODIGO
    , p.PAR_RE_VMJROTPNCOB
    , p.PAR_RE_PERCVMSAC
    , p.PAR_BO_CONSINVESTIDOR
    , p.PAR_RE_VALORBASEINV
    , p.PAR_RE_VALORBASETERRENEIRO
    , p.PAR_RE_VLRINFRA
    , p.PAR_RE_VALORBASERATEIO
    , p.PAR_CH_AMORTIZACAO
    , p.PAR_RE_VALORITBI
    , p.PAR_RE_VALORCRI
    , p.PAR_BO_SECURITIZADA
    , p.PAR_BO_CHEQUEDEVOLVIDO
    , p.CTT_IN_CHEQUEDEVOLVIDO
    , p.PAR_RE_VLRRENEGOCIADOSEC
    , p.PAI_PAR_CH_ORIGEM
    , p.PAR_RE_VLRORIGINALSAC
    , p.ASS_IN_CODIGO
    , p.PAR_BO_CONSDESCDIF
    , p.PAR_BO_BLOQUEADA
    , p.PAR_RE_DIFCORRECAONEG
    , p.PAR_BO_CONTABILIZAALUGUEL
    , imm.imo_dt_movimento,
    imm.mov_in_codigo,
    decode(u.UND_CODIGO,null,e.bem_in_codigo,u.UND_CODIGO) UND_CODIGO,
    f.AGN_IN_CODIGO,
    c.fil_in_codigo,
    p.par_re_valororiginal  VALOR_ORIGINAL,
    p.par_re_valororiginal
        + bild.ALX_PCK_BLDAPP.F_Car_Correcao_Parcela(
                p.org_tab_in_codigo,
                p.org_pad_in_codigo,
                p.org_in_codigo,
                p.org_tau_st_codigo,
                p.cto_in_codigo,
                p.par_in_codigo,
                trunc(sysdate)
        )
    VALOR_CORRIGIDO,
    NVL(
        bild.PCK_CAR_FNC.FNC_CAR_CORRIGE(
            P.ORG_TAB_IN_CODIGO,
            P.ORG_PAD_IN_CODIGO,
            P.ORG_IN_CODIGO,
            P.ORG_TAU_ST_CODIGO,
            P.CTO_IN_CODIGO,
            P.PAR_IN_CODIGO,
            TRUNC(SYSDATE),
            'RPS',
            'A',
            -1
        )
    , 0) VALOR_ATUALIZADO,
        round(NVL(P.PAR_RE_VALORPAGO, 0), 2)
        + ROUND( NVL( P.PAR_RE_RESIDUOCOBRANCA  , 0), 2)
        + ROUND( NVL( P.PAR_RE_VALORMULTA       , 0), 2)
        + ROUND( NVL( P.PAR_RE_VALORATRASO      , 0), 2)
        - ROUND( NVL( P.PAR_RE_VALORDESCONTO    , 0), 2)
        + ROUND( NVL( P.PAR_RE_VALORCORRECAO_ATR, 0), 2)
        + ROUND(
            NVL(
                bild.pck_car_fnc.fnc_car_total_taxasparcela(
                    p.org_tab_in_codigo
                    , p.org_pad_in_codigo
                    , p.org_in_codigo
                    , p.org_tau_st_codigo
                    , p.cto_in_codigo
                    , p.par_in_codigo
                )
            , 0)
        , 4)
        + nvl(
            bild.ALX_PCK_BLDAPP.F_Car_VlrPago_Maior(
                p.org_tab_in_codigo,
                p.org_pad_in_codigo,
                p.org_in_codigo,
                p.org_tau_st_codigo,
                p.cto_in_codigo,
                p.par_in_codigo,
                trunc(sysdate)
            )
        ,0)
    VALOR_PAGO,
    nvl(
        bild.ALX_PCK_BLDAPP.F_Car_VlrPago_Maior(
            p.org_tab_in_codigo,
            p.org_pad_in_codigo,
            p.org_in_codigo,
            p.org_tau_st_codigo,
            p.cto_in_codigo,
            p.par_in_codigo,
            trunc(sysdate)
        )
    ,0) VALOR_PAGO_MAIOR,
    bild.ALX_PCK_BLDAPP.F_Car_VlrJurCont_Parcela(
        p.org_tab_in_codigo,
        p.org_pad_in_codigo,
        p.org_in_codigo,
        p.org_tau_st_codigo,
        p.cto_in_codigo,
        p.par_in_codigo,
        trunc(sysdate)
    ) VALOR_JUROS,
    bild.ALX_PCK_BLDAPP.F_Car_VlrMulta_Parcela(
        p.org_tab_in_codigo,
        p.org_pad_in_codigo,
        p.org_in_codigo,
        p.org_tau_st_codigo,
        p.cto_in_codigo,
        p.par_in_codigo,
        trunc(sysdate)
    ) VALOR_MULTA,
    bild.ALX_PCK_BLDAPP.F_Car_VlrAtraso_Parcela(
        p.org_tab_in_codigo,
        p.org_pad_in_codigo,
        p.org_in_codigo,
        p.org_tau_st_codigo,
        p.cto_in_codigo,
        p.par_in_codigo,
        trunc(sysdate)
    ) VALOR_ATRASO,
    p.par_re_valordesconto VALOR_DESCONTO,
    d.cnd_re_valor VALOR_TERMO,
    nvl(
        bild.pck_car_fnc.fnc_car_valorcorrecao(
            p.org_tab_in_codigo,
            p.org_pad_in_codigo,
            p.org_in_codigo,
            p.org_tau_st_codigo,
            p.cto_in_codigo,
            p.par_in_codigo,
            decode(p.par_ch_receitabaixa, 'B', decode('B', 'B', p.par_dt_baixa, p.par_dt_vencimento), p.par_dt_baixa),
            'RP',
            'D',
            -1
        ) - q.rescob_re_correcaocobrada,0
    ) RESIDUO_PREVISTO,
    bild.ALX_PCK_BLDAPP.F_Car_Correcao_Parcela(p.org_tab_in_codigo,
                                             p.org_pad_in_codigo,
                                             p.org_in_codigo,
                                             p.org_tau_st_codigo,
                                             p.cto_in_codigo,
                                             p.par_in_codigo,
                                             trunc(sysdate)) VALOR_CORRECAO,
    ROUND( NVL( bild.pck_car_fnc.fnc_car_total_taxasparcela( p.org_tab_in_codigo
                                                            , p.org_pad_in_codigo
                                                            , p.org_in_codigo
                                                            , p.org_tau_st_codigo
                                                            , p.cto_in_codigo
                                                            , p.par_in_codigo), 0), 4) VALOR_TAXA,
    SUBSTR(
        bild.PCK_CAR_FNC.FNC_CAR_ORIGEMPARCELA(
            P.ORG_TAB_IN_CODIGO,
            P.ORG_PAD_IN_CODIGO,
            P.ORG_IN_CODIGO,
            P.ORG_TAU_ST_CODIGO,
            P.CTO_IN_CODIGO,
            P.PAR_IN_CODIGO,
            P.PAR_CH_ORIGEM,
            P.CND_IN_CODIGO,
            P.PAR_BO_TABELAPRICE,
            2
        )
    ,1,50) ORIGEM,
    SUBSTR(
        bild.PCK_CAR_FNC.FNC_CAR_ORIGEMPARCELA(
            P.ORG_TAB_IN_CODIGO,
            P.ORG_PAD_IN_CODIGO,
            P.ORG_IN_CODIGO,
            P.ORG_TAU_ST_CODIGO,
            P.CTO_IN_CODIGO,
            P.PAR_IN_CODIGO,
            P.PAR_CH_ORIGEM,
            P.CND_IN_CODIGO,
            P.PAR_BO_TABELAPRICE,
            1
        )
    ,1,50) PROCESSO,
    cf.csf_st_descricao,
    c.pro_in_reduzido,
    c.pro_ide_st_codigo,
    c.pro_pad_in_codigo,
    c.pro_tab_in_codigo,
    bild.alx_pck_utilafh.FNC_REC_SIGLA_IND_CORR_VIG_PAR(p.org_tab_in_codigo
                                                 , p.org_pad_in_codigo
                                                 , p.org_in_codigo
                                                 , p.org_tau_st_codigo
                                                 , p.cto_in_codigo
                                                 , p.par_in_codigo)                    SIGLA_IND_CORRECAO,
    crp.ren_in_codigo AS REN_IN_CODIGO,
    p.par_re_valorjurosren AS RENEGOCIACAO
from
    bild.car_contrato           c,
    bild.car_contrato_cliente   x,
    bild.car_parcela            p,
    bild.glo_agentes            a,
    bild.glo_pessoa_fisica      f,
    bild.car_contrato_envolvido e,
    bild.dbm_vw_envolvido       u,
    bild.dbm_entrega_obra_real  o,
    bild.car_contrato_termo     t,
    bild.dbm_condicao           d,
    bild.car_tipo_termo         z,
    bild.car_parcela_destino    w,
    bild.glo_contasfin          b,
    bild.car_residuo_cobranca   q,
    bild.dbm_classificacao      cf,
    bild.car_movimento_parcela  mvv,
    bild.car_integra_movimento  imm,
    bild.car_renegociacao_parcela  crp
Where
    bild.ALX_PCK_UTILAFH.F_StatusAgente(c.org_tab_in_codigo,c.org_pad_in_codigo, c.fil_in_codigo,c.org_tau_st_codigo) = 'A'
    and   c.org_tab_in_codigo     = p.org_tab_in_codigo
    and   c.org_pad_in_codigo     = p.org_pad_in_codigo
    and   c.org_in_codigo         = p.org_in_codigo
    and   c.org_tau_st_codigo     = p.org_tau_st_codigo
    and   c.cto_in_codigo         = p.cto_in_codigo

    and   p.org_tab_in_codigo     = mvv.org_tab_in_codigo(+)
    and   p.org_pad_in_codigo     = mvv.org_pad_in_codigo(+)
    and   p.org_in_codigo         = mvv.org_in_codigo(+)
    and   p.org_tau_st_codigo     = mvv.org_tau_st_codigo(+)
    and   p.cto_in_codigo         = mvv.cto_in_codigo(+)
    and   p.par_in_codigo         = mvv.par_in_codigo(+)

    AND   mvv.org_tab_in_codigo   = imm.org_tab_in_codigo(+)
    AND   mvv.org_pad_in_codigo   = imm.org_pad_in_codigo(+)
    AND   mvv.org_in_codigo       = imm.org_in_codigo(+)
    AND   mvv.org_tau_st_codigo   = imm.org_tau_st_codigo(+)
    AND   mvv.mov_in_codigo       = imm.mov_in_codigo(+)

    and   p.par_ch_status        <> 'I'
    and   c.org_tab_in_codigo     = x.org_tab_in_codigo
    and   c.org_pad_in_codigo     = x.org_pad_in_codigo
    and   c.org_in_codigo         = x.org_in_codigo
    and   c.org_tau_st_codigo     = x.org_tau_st_codigo
    and   c.cto_in_codigo         = x.cto_in_codigo

    and   x.agn_tab_in_codigo     = a.agn_tab_in_codigo
    and   x.agn_pad_in_codigo     = a.agn_pad_in_codigo
    and   x.agn_in_codigo         = a.agn_in_codigo

    and   a.agn_tab_in_codigo     = f.agn_tab_in_codigo (+)
    and   a.agn_pad_in_codigo     = f.agn_pad_in_codigo (+)
    and   a.agn_in_codigo         = f.agn_in_codigo     (+)
    and   f.agn_ch_tipo (+)       = 'P'

    and   c.org_tab_in_codigo     = e.org_tab_in_codigo
    and   c.org_pad_in_codigo     = e.org_pad_in_codigo
    and   c.org_in_codigo         = e.org_in_codigo
    and   c.org_tau_st_codigo     = e.org_tau_st_codigo
    and   c.cto_in_codigo         = e.cto_in_codigo

    and   e.est_org_tab_in_codigo = u.tab_in_codigo(+)
    and   e.est_org_pad_in_codigo = u.pad_in_codigo(+)
    and   e.est_org_in_codigo     = u.org_in_codigo(+)
    and   e.est_org_tau_st_codigo = u.tau_st_codigo(+)
    and   e.est_in_codigo         = u.und_codigo(+)

    and   u.tab_in_codigo         = o.org_tab_in_codigo(+)
    and   u.pad_in_codigo         = o.org_pad_in_codigo(+)
    and   u.org_in_codigo         = o.org_in_codigo(+)
    and   u.tau_st_codigo         = o.org_tau_st_codigo(+)
    and   u.blo_codigo            = o.est_in_codigo(+)

    and   p.org_tab_in_codigo     = t.org_tab_in_codigo (+)
    and   p.org_pad_in_codigo     = t.org_pad_in_codigo (+)
    and   p.org_in_codigo         = t.org_in_codigo     (+)
    and   p.org_tau_st_codigo     = t.org_tau_st_codigo (+)
    and   p.cto_in_codigo         = t.cto_in_codigo     (+)
    and   p.ctt_in_codigo         = t.ctt_in_codigo     (+)

    and   t.org_tab_in_codigo     = d.org_tab_in_codigo (+)
    and   t.org_pad_in_codigo     = d.org_pad_in_codigo (+)
    and   t.org_in_codigo         = d.org_in_codigo     (+)
    and   t.org_tau_st_codigo     = d.org_tau_st_codigo (+)
    and   t.cnd_in_codigo         = d.cnd_in_codigo     (+)

    and   t.tte_in_codigo         = z.tte_in_codigo     (+)

    and   p.org_tab_in_codigo     = w.org_tab_in_codigo (+)
    and   p.org_pad_in_codigo     = w.org_pad_in_codigo (+)
    and   p.org_in_codigo         = w.org_in_codigo     (+)
    and   p.org_tau_st_codigo     = w.org_tau_st_codigo (+)
    and   p.cto_in_codigo         = w.cto_in_codigo     (+)
    and   p.par_in_codigo         = w.par_in_codigo     (+)

    and   w.agn_tab_in_codigo     = b.agn_tab_in_codigo (+)
    and   w.agn_pad_in_codigo     = b.agn_pad_in_codigo (+)
    and   w.agn_in_codigo         = b.agn_in_codigo     (+)
    and   w.agn_tau_st_codigo     = b.agn_tau_st_codigo (+)

    and   p.org_tab_in_codigo     = q.org_tab_in_codigo (+)
    and   p.org_pad_in_codigo     = q.org_pad_in_codigo (+)
    and   p.org_in_codigo         = q.org_in_codigo     (+)
    and   p.org_tau_st_codigo     = q.org_tau_st_codigo (+)
    and   p.cto_in_codigo         = q.cto_in_codigo     (+)
    and   p.par_in_codigo         = q.par_in_codigo     (+)
    and   c.csf_in_codigo         = cf.csf_in_codigo
     
    and p.org_tab_in_codigo = crp.org_tab_in_codigo (+)
    and p.org_pad_in_codigo = crp.org_pad_in_codigo (+)
    and p.org_in_codigo     = crp.org_in_codigo     (+)
    and p.org_tau_st_codigo = crp.org_tau_st_codigo (+)
    and p.cto_in_codigo     = crp.cto_in_codigo     (+)
    and p.par_in_codigo     = crp.par_in_codigo     (+)
    and par_dt_baixa IS NOT NULL
    and par_dt_baixa >= '{}'
    and c.fil_in_codigo NOT IN ({})
    ) STG_PARCELA_RECEITA
""".replace('\n', ' ').replace('\t', ' ').strip()


# ========
#  SYS
# ========

VENDAS = f"""
( SELECT 
    t_vendas.id                   AS CONTRATO_SYS,
    t_unidades.id                 as COD_EXTERNO_UNIDADE,
    imobiliarias.id               as IMOBILIARIA,
    gerentes.id                   as CODIGO_GERENTE,
    supervisores.id               as CODIGO_SUPERVISOR,
    corretores.id                 as CODIGO_CORRETOR,
    t_etapas_vendas.id            as CODIGO_VENDA,
    t_usos.id                     as CODIGO_USO,
    t_vendas.id_status_repasse    as STATUS_REPASSE,
    clientes.CPF                  as CPF_CNPJ,
    t_unidades.cod_exporta        as est_in_codigo,
    clientes.instrucao            AS GRAU_INSTRUCAO,
    empreendimentos.prazo_entrega AS DATA_ENTREGA_OBRA,
    empreendimentos.chaves_dt     AS DATA_ENTREGA_CHAVES,
    t_vendas.distrato_dt          AS DATA_DISTRATO,
    valida_dt                     AS DATA_VALIDA_VENDA
FROM 
    t_vendas
LEFT JOIN clientes ON clientes.id = t_vendas.id_cliente_1
LEFT JOIN gerentes ON gerentes.id = clientes.id_gerente
LEFT JOIN imobiliarias ON imobiliarias.id = clientes.id_imobiliaria
LEFT JOIN supervisores ON supervisores.id = clientes.id_supervisor
LEFT JOIN corretores ON corretores.id = clientes.id_corretor
LEFT JOIN t_unidades ON t_unidades.id = t_vendas.id_unidade
LEFT JOIN empreendimentos ON empreendimentos.id = t_vendas.id_empreend and empreendimentos.ativo = 'a'
LEFT JOIN t_etapas_vendas ON t_etapas_vendas.codigo = t_vendas.status
LEFT JOIN t_usos ON t_usos.id = t_unidades.id_uso
WHERE 
    t_vendas.status in (55, 50) ) STG_VENDAS
""".replace('\n', ' ').replace('\t', ' ').strip()