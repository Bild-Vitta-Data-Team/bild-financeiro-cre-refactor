D_REGIONAL = f"""
SELECT 
    REGIONAL,
    CODIGO_REGIONAL,
    CODIGO_REGIONAL_2,
    CODIGO_REGIONAL_3,
    CODIGO_REGIONAL_4,
    CODIGO_REGIONAL_5,
    CODIGO_REGIONAL_6,
    CODIGO_REGIONAL_7,
    CODIGO_REGIONAL_8,
    CODIGO_REGIONAL_9,
    CODIGO_REGIONAL_10,
    CODIGO_REGIONAL_11,
    CODIGO_REGIONAL_12,
    CODIGO_REGIONAL_13,
    CODIGO_REGIONAL_14,
    CODIGO_REGIONAL_15,
    CODIGO_REGIONAL_16,
    CODIGO_REGIONAL_17,
    CODIGO_REGIONAL_18,
    CODIGO_REGIONAL_19,
    CODIGO_REGIONAL_20
FROM 
    StgBild.STG_ARQ_REGIONAL
""".replace('\n', ' ').replace('\t', ' ').strip()