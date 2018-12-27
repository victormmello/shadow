SELECT
p.colecao,
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
'' as aux1,
'' as aux2,
'' as aux3,
COUNT(DISTINCT cp.produto + cp.cor_produto) as produtos_a_receber
FROM compras_produto cp
INNER JOIN compras c on cp.pedido = c.pedido
INNER JOIN produtos p on cp.produto = p.produto
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
YEAR(cp.entrega)*100 + MONTH(cp.entrega) = YEAR(GETDATE()-1)*100 + MONTH(GETDATE()-1)
and cp.qtde_entregar > 0
and cp.qtde_entregue = 0
GROUP BY
p.colecao,
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros')