SELECT
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))) as filial,
YEAR(vp.data_venda)*100 + MONTH(vp.data_venda) as anomes,
CONVERT(NVARCHAR,vp.data_venda,103) as data_venda,
SUM(vp.qtde*(v.valor_pago/v.valor_venda_bruta*vp.preco_liquido)) as receita_total
FROM loja_venda_produto vp
INNER JOIN loja_venda v on
	v.ticket = vp.ticket and
	vp.codigo_filial = v.codigo_filial and
	v.data_venda = vp.data_venda
INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
INNER JOIN produtos p on p.produto = vp.produto
INNER JOIN produtos_precos c on
	p.produto = c.produto and
	c.codigo_tab_preco = 02 -- 02 custo
LEFT JOIN bi_categorizacao_categoria ccc on
	ccc.grupo_produto = p.grupo_produto and
	ccc.cod_categoria = p.cod_categoria and
	ccc.subgrupo_produto is null and
	ccc.tipo_produto is null
LEFT JOIN bi_categorizacao_categoria cct on
	cct.grupo_produto = p.grupo_produto and
	cct.tipo_produto = p.tipo_produto and
	cct.subgrupo_produto is null and
	cct.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria ccs on
	ccs.grupo_produto = p.grupo_produto and
	ccs.subgrupo_produto = p.subgrupo_produto and
	ccs.tipo_produto is null and
	ccs.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria cc on
	cc.grupo_produto = p.grupo_produto and
	cc.subgrupo_produto is null and
	cc.tipo_produto is null and
	cc.cod_categoria is null
WHERE
p.grupo_produto != 'GIFTCARD' and
vp.qtde > 0 and
vp.preco_liquido > 0 and
v.valor_venda_bruta > 0 and
vp.data_venda >= '20170401'
GROUP BY
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))),
YEAR(vp.data_venda)*100 + MONTH(vp.data_venda),
CONVERT(NVARCHAR,vp.data_venda,103)


UNION

SELECT
'E-COMMERCE' as filial,
YEAR(f.emissao)*100 + MONTH(f.emissao) as anomes,
CONVERT(NVARCHAR,f.emissao,103) as data_venda,
SUM(f.valor_total/f.valor_sub_itens*fp.valor) as receita_total
FROM faturamento_prod fp
INNER JOIN faturamento f on
	f.nf_saida = fp.NF_SAIDA and
	fp.filial = f.filial
INNER JOIN produtos p on p.produto = fp.produto
INNER JOIN produtos_precos c on
	p.produto = c.produto and
	c.codigo_tab_preco = 02 -- 02 custo
LEFT JOIN bi_categorizacao_categoria ccc on
	ccc.grupo_produto = p.grupo_produto and
	ccc.cod_categoria = p.cod_categoria and
	ccc.subgrupo_produto is null and
	ccc.tipo_produto is null
LEFT JOIN bi_categorizacao_categoria cct on
	cct.grupo_produto = p.grupo_produto and
	cct.tipo_produto = p.tipo_produto and
	cct.subgrupo_produto is null and
	cct.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria ccs on
	ccs.grupo_produto = p.grupo_produto and
	ccs.subgrupo_produto = p.subgrupo_produto and
	ccs.tipo_produto is null and
	ccs.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria cc on
	cc.grupo_produto = p.grupo_produto and
	cc.subgrupo_produto is null and
	cc.tipo_produto is null and
	cc.cod_categoria is null
WHERE
p.grupo_produto != 'GIFTCARD' and
fp.qtde > 0 and
f.valor_sub_itens > 0 and
fp.serie_nf IN (2,7) and
f.natureza_saida = '100.01' and
f.filial = 'e-commerce' and
f.emissao >= '20170401'
GROUP BY
CONVERT(NVARCHAR,f.emissao,103),
YEAR(f.emissao)*100 + MONTH(f.emissao)