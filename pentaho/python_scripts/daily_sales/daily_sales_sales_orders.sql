SELECT
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))) as filial,
CASE
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN 'MTD'
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112) THEN 'LM'
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) THEN 'LY'
END as periodo,
COUNT(DISTINCT CAST(v.data_venda as varchar) + v.codigo_filial + v.ticket) as orders
FROM loja_venda_produto vp
INNER JOIN loja_venda v on
	v.ticket = vp.ticket and
	vp.codigo_filial = v.codigo_filial and
	v.data_venda = vp.data_venda
INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
INNER JOIN produtos p on p.produto = vp.produto
WHERE
p.grupo_produto != 'GIFTCARD' and
vp.qtde > 0 and
vp.preco_liquido > 0 and
DAY(vp.data_venda) <= DAY(GETDATE()-1) and CONVERT(NVARCHAR(6), vp.data_venda, 112) IN (
	CONVERT(NVARCHAR(6), GETDATE()-1, 112), -- Mes Atual
	CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112), -- Mes Passado
	CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) -- Ano Passado
)
GROUP BY
LTRIM(RTRIM(COALESCE(NULLIF(f.filial,'E-COMMERCE'),'CAMBUI'))),
CASE
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN 'MTD'
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112) THEN 'LM'
	WHEN CONVERT(NVARCHAR(6), vp.data_venda, 112) = CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) THEN 'LY'
END

UNION

SELECT
'E-COMMERCE' as filial,
CASE
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN 'MTD'
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112) THEN 'LM'
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) THEN 'LY'
END as periodo,
COUNT(DISTINCT f.nf_saida) as orders
FROM faturamento_prod fp
INNER JOIN faturamento f on
	f.nf_saida = fp.NF_SAIDA and
	fp.filial = f.filial
INNER JOIN produtos p on p.produto = fp.produto
WHERE
p.grupo_produto != 'GIFTCARD' and
fp.qtde > 0 and
f.valor_sub_itens > 0 and
fp.serie_nf IN (2,7) and
f.natureza_saida = '100.01' and
f.filial = 'e-commerce' and
DAY(f.emissao) <= DAY(GETDATE()-1) and CONVERT(NVARCHAR(6), f.emissao, 112) IN (
	CONVERT(NVARCHAR(6), GETDATE()-1, 112), -- Mes Atual
	CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112), -- Mes Passado
	CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) -- Ano Passado
)
GROUP BY
CASE
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), GETDATE()-1, 112) THEN 'MTD'
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), DATEADD(m,-1,GETDATE()-1), 112) THEN 'LM'
	WHEN CONVERT(NVARCHAR(6), f.emissao, 112) = CONVERT(NVARCHAR(6), DATEADD(yy,-1,GETDATE()-1), 112) THEN 'LY'
END