# Projeto Bild Financeiro CRE Refactor

<hr>

<p>A ideia do projeto CRE refactor é transcrever os jobs e todas as dependências do projeto Talend para a linguagem Python. Por baixo dos panos o Talend é apenas Java, existe a opção de exportar o código java do Talend com todos os scripts em Java, mas dado a algumas dificuldades, principalmente em relação a transcrição e inputs de variáveis globais nos contextos e de manipulação dos mesmos. A ideia é ter apenas um projeto bem mais simplificado que faça as mesmas coisas que o Talend faz com mais maleabilidade e possibilidades de edição.</p>


<p>Projeto CRE consiste de várias dependencias, principalmente durante a utilização da Data de Parâmetro de fechamento CRE (DW_PRD).</p>

>
> **Under Working!**
>
> Necessário Antes de Continuar:
> 
> 1. Validação F_Parcela;
> 2. Validação de Demais Tabelas Mapeadas;
>

**Keep It Simple!**

**Azure Blobs Arquivos**
- [ ] STG_ARQ_PARAM;
- [ ] STG_ARQ_INDICE;
- [ ] STG_ARQ_OCORRENCIA;
- [ ] STG_ARQ_REGIONAIS;

**Oracle Fonte Principal**

- [x] STG_MEGA_AGENTES;
- [x] STG_MEGA_BOLETAGEM;
- [ ] STG_MEGA_BOLETAGEM_RETROATIVO"";
- [ ] STG_MEGA_CARTEIRA;
- [x] STG_MEGA_CLASSIFICACAO_CONTRATO;
- [ ] STG_MEGA_CLIENTE;
- [ ] STG_MEGA_CONDICAO;
- [x] STG_MEGA_CONTRATO_CLIENTE;
- [ ] STG_MEGA_CONTRATO_CONSOLIDADO;
- [ ] STG_MEGA_CONTRATOS;
- [ ] STG_MEGA_D_EMPREENDIMENTOS;
- [ ] STG_MEGA_INADIMPLENCIA;
- [ ] STG_MEGA_INTEGRACAO_MOVIMENTO;
- [ ] STG_MEGA_MEDICOES_ASSOCIATIVO;
- [ ] STG_MEGA_OCORRENCIA_CONTRATO_AUTOMATICA;
- [ ] STG_MEGA_OCORRENCIA_CONTRATO;
- [ ] STG_MEGA_OCORRENCIA_GERAL;
- [ ] STG_MEGA_PARCELA_BAIXADA;
- [ ] STG_MEGA_PARCELA_BENS_RETROATIVO"";
- [ ] STG_MEGA_PARCELA_RECEITA_BENS;
- [ ] STG_MEGA_PARCELA_RECEITA_FINAL;
- [ ] STG_MEGA_PARCELA_RECEITA_RETROATIVO"";
- [x] STG_MEGA_PARCELA_RECEITA;
- [ ] STG_MEGA_RENEGOCIACAO_ANTES_PAR;
- [ ] STG_MEGA_RENEGOCIACAO_ANTES;
- [ ] STG_MEGA_RENEGOCIACAO_DEPOIS;
- [x] STG_MEGA_RENEGOCIACAO_PAR;
- [ ] STG_MEGA_RENEGOCIACAO;
- [ ] STG_MEGA_TERMO;
- [ ] STG_MEGA_UNIDADE;

**MySQL Integração**

- [ ] STG_SYS_CLIENTE;
- [ ] STG_SYS_CORRETOR;
- [ ] STG_SYS_GERENTE;
- [ ] STG_SYS_IMOBILIARIA;
- [ ] STG_SYS_SURPEVISOR;
- [ ] STG_SYS_T_EMPREENDIMENTO;
- [ ] STG_SYS_T_ETAPA_VENDA;
- [ ] STG_SYS_T_STATUS_REPASSE;
- [ ] STG_SYS_T_USO;
- [x] STG_SYS_T_VENDAS;
- [ ] STG_SYS_UNIDADE;
