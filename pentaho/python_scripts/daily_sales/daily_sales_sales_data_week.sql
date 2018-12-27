WITH sales as (
	SELECT
	p.colecao,
	--'' as colecao,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
	LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))) as filial,
	CASE
		WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
		WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
		WHEN CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
	END as periodo,
	CASE WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN vp.data_venda ELSE '' END as data_venda,
	SUM(vp.qtde) as itens,
	CAST(SUM(vp.qtde*(vp.preco_liquido + vp.desconto_item)) as INT) as preco_original_total,
	CAST(SUM(vp.qtde*(v.valor_pago/v.valor_venda_bruta*vp.preco_liquido)) as INT) as receita_total,
	CAST(SUM(c.preco1*vp.qtde) as INT) as custo_total
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
	(
		CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) -- Semana Atual
		OR CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) -- Semana Passada
		OR CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) -- Semana Ano Passado
	)
	GROUP BY
	p.colecao,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
	LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))),
	CASE
		WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
		WHEN CAST(vp.data_venda as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
		WHEN CAST(vp.data_venda as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
	END,
	CASE WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN vp.data_venda ELSE '' END


	UNION

	SELECT
	p.colecao,
	--'' as colecao,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
	'E-COMMERCE' as filial,
	CASE
		WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
		WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
		WHEN CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
	END as periodo,
	CASE WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN f.emissao ELSE '' END as data_venda,
	SUM(fp.qtde) as itens,
	CAST(SUM(fp.valor + fp.desconto_item) as INT) as preco_original_total,
	CAST(SUM(f.valor_total/f.valor_sub_itens*fp.valor) as INT) as receita_total,
	CAST(SUM(c.preco1*fp.qtde) as INT) as custo_total
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
	(
		CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) -- Semana Atual
		OR CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) -- Semana Passada
		OR CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) -- Semana Ano Passado
	)
	GROUP BY
	p.colecao,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
	CASE
		WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-7 as DATE) AND CAST(GETDATE()-1 as DATE) THEN 'WEEK'
		WHEN CAST(f.emissao as DATE) BETWEEN CAST(GETDATE()-14 as DATE) AND CAST(GETDATE()-8 as DATE) THEN 'LW'
		WHEN CAST(f.emissao as DATE) BETWEEN DATEADD(week,-52,CAST(GETDATE()-7 as DATE)) AND DATEADD(week,-52,CAST(GETDATE()-1 as DATE)) THEN 'LYW'
	END,
	CASE WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN f.emissao ELSE '' END
), collections as (
	SELECT TOP 4
	s.colecao
	FROM sales s
	WHERE
	s.periodo = 'WEEK'
	GROUP BY
	s.colecao
	ORDER BY
	SUM(s.receita_total) desc
), categories as (
	SELECT TOP 20
	s.categoria
	FROM sales s
	WHERE
	s.periodo = 'WEEK'
	GROUP BY
	s.categoria
	ORDER BY
	SUM(s.receita_total) desc
)
SELECT
COALESCE(c.colecao,'') as colecao,
COALESCE(cat.categoria,'') as categoria,
s.filial,
s.periodo,
MAX(s.data_venda) as data_venda,
SUM(s.itens) as itens,
SUM(s.preco_original_total) as preco_original_total,
SUM(s.receita_total) as receita_total,
SUM(s.custo_total) as custo_total
FROM sales s
LEFT JOIN collections c on c.colecao = s.colecao
LEFT JOIN categories cat on cat.categoria = s.categoria
GROUP BY
COALESCE(c.colecao,''),
COALESCE(cat.categoria,''),
s.filial,
s.periodo