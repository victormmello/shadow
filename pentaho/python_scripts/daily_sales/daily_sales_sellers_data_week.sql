SELECT
COALESCE(lmd.desc_motivo_desconto,'VAZIO') as cupom,
sf.sigla_filial + ' - ' + SUBSTRING(lv.vendedor_apelido, 1, CHARINDEX(' ', lv.vendedor_apelido, 1) - 1) as vendedor,
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))) as filial,
CASE
	WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
	WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
	WHEN CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
END as periodo,
'' as data_venda,
SUM(vp.qtde) as itens,
SUM(vp.qtde*(vp.preco_liquido + vp.desconto_item)) as preco_original_total,
SUM(vp.qtde*(v.valor_pago/v.valor_venda_bruta*vp.preco_liquido)) as receita_total,
SUM(c.preco1*vp.qtde) as custo_total
FROM loja_venda_produto vp
INNER JOIN loja_venda v on
	v.ticket = vp.ticket and
	vp.codigo_filial = v.codigo_filial and
	v.data_venda = vp.data_venda
INNER JOIN loja_vendedores lv on
	lv.codigo_filial = v.codigo_filial and
	lv.vendedor = v.vendedor
INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
INNER JOIN (
	SELECT 'IGUATEMI CAMPINAS' as filial, 'IGUATEMI' as sigla_filial UNION
	SELECT 'MM DOM PEDRO' as filial, 'DOM PEDRO' as sigla_filial UNION
	SELECT 'PREMIUM OUTLET ITUPEVA' as filial, 'OUTLET' as sigla_filial UNION
	SELECT 'SANTA URSULA' as filial, 'RIBEIRAO' as sigla_filial UNION
	SELECT 'JUNDIAI SHOPPING' as filial, 'JUNDIAI' as sigla_filial UNION
	SELECT 'SANTOS PRAIAMAR' as filial, 'SANTOS' as sigla_filial UNION
	SELECT 'PIRACICABA SHOPPING' as filial, 'PIRACICABA' as sigla_filial UNION
	SELECT 'CAMBUI' as filial, 'CAMBUI' as sigla_filial UNION
	SELECT 'E-COMMERCE' as filial, 'E-COMMERCE' as sigla_filial
) sf on sf.filial = COALESCE(NULLIF(LTRIM(RTRIM(f.filial)),'e-commerce'),'CAMBUI')
INNER JOIN produtos p on p.produto = vp.produto
INNER JOIN produtos_precos c on
	p.produto = c.produto and
	c.codigo_tab_preco = 02 -- 02 custo
LEFT JOIN loja_motivos_desconto lmd on lmd.codigo_desconto = v.codigo_desconto
WHERE
p.grupo_produto != 'GIFTCARD' and
vp.qtde > 0 and
vp.preco_liquido > 0 and
(
	CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) -- Semana Atual
	OR CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) -- Semana Passada
	OR CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) -- Semana Ano Passado
)
GROUP BY
COALESCE(lmd.desc_motivo_desconto,'VAZIO'),
sf.sigla_filial + ' - ' + SUBSTRING(lv.vendedor_apelido, 1, CHARINDEX(' ', lv.vendedor_apelido, 1) - 1),
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))),
CASE
	WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
	WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
	WHEN CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
END


UNION

SELECT
'VAZIO' as cupom,
'' as vendedor,
'E-COMMERCE' as filial,
CASE
	WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
	WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
	WHEN CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
END as periodo,
'' as data_venda,
SUM(fp.qtde) as itens,
SUM(fp.valor + fp.desconto_item) as preco_original_total,
SUM(f.valor_total/f.valor_sub_itens*fp.valor) as receita_total,
SUM(c.preco1*fp.qtde) as custo_total
FROM faturamento_prod fp
INNER JOIN faturamento f on
	f.nf_saida = fp.NF_SAIDA and
	fp.filial = f.filial
INNER JOIN produtos p on p.produto = fp.produto
INNER JOIN produtos_precos c on
	p.produto = c.produto and
	c.codigo_tab_preco = 02 -- 02 custo
WHERE
p.grupo_produto != 'GIFTCARD' and
fp.qtde > 0 and
f.valor_sub_itens > 0 and
fp.serie_nf IN (2,7) and
f.natureza_saida = '100.01' and
f.filial = 'e-commerce' and
(
	CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) -- Semana Atual
	OR CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) -- Semana Passada
	OR CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) -- Semana Ano Passado
)
GROUP BY
CASE
	WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
	WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
	WHEN CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
END