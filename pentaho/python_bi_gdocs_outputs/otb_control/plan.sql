SELECT
pp.anomes,
pp.categoria,
pp.liso_estampado,
SUM(pp.custo_recebido - COALESCE(NULLIF(cart.custo_a_receber,0),pp.custo_a_receber)) as custo_carteira,
SUM(pp.quantidade_recebida - COALESCE(NULLIF(cart.quantidade_a_receber,0),pp.quantidade_a_receber)) as volume_carteira
FROM bi_purchase_plan pp
LEFT JOIN (
	SELECT
	YEAR(cp.entrega)*100 + MONTH(cp.entrega) as anomes,
	CASE WHEN p.griffe = 'NAKD' OR p.fabricante = 'ANJUU/AISHTY' THEN 'NAKD' ELSE 'Marcia Mello' END as marca,
	CASE
		WHEN c.preco1 < 20 THEN '01. 0-20'
		WHEN c.preco1 < 40 THEN '02. 20-40'
		WHEN c.preco1 < 60 THEN '03. 40-60'
		WHEN c.preco1 < 80 THEN '04. 60-80'
		WHEN c.preco1 < 100 THEN '05. 80-100'
		WHEN c.preco1 < 125 THEN '06. 100-125'
		WHEN c.preco1 < 150 THEN '07. 125-150'
		WHEN c.preco1 < 175 THEN '08. 150-175'
		WHEN c.preco1 < 200 THEN '09. 175-200'
		WHEN c.preco1 < 250 THEN '10. 200-250'
		WHEN c.preco1 < 300 THEN '11. 250-300'
		WHEN c.preco1 < 400 THEN '12. 300-400'
		WHEN c.preco1 < 500 THEN '13. 400-500'
		ELSE '14. 500+'
	END as faixa_de_custo,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
	CASE WHEN LTRIM(RTRIM(pc.desc_cor_produto)) LIKE 'estampa%' THEN 'Estampado' ELSE 'Liso' END as liso_estampado,
	SUM(cp.qtde_entregar) as quantidade_a_receber,
	SUM(cp.valor_entregar) as custo_a_receber
	FROM compras_produto cp
	INNER JOIN compras com on cp.pedido = com.pedido
	INNER JOIN produtos p on cp.produto = p.produto
	INNER JOIN produtos_precos c on
		p.produto = c.produto and
		c.codigo_tab_preco = 02 -- 02 custo
	INNER JOIN produto_cores pc on
		cp.produto = pc.produto and
		cp.cor_produto = pc.cor_produto
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
	LEFT JOIN bi_categorizacao_cor cor on cor.cor = pc.desc_cor_produto
	WHERE
	YEAR(cp.entrega)*100 + MONTH(cp.entrega) >= YEAR(%(date_to)s)*100 + MONTH(%(date_to)s)
	and cp.qtde_entregar > 0
	and cp.qtde_entregue = 0
	GROUP BY
	YEAR(cp.entrega)*100 + MONTH(cp.entrega),
	CASE WHEN p.griffe = 'NAKD' OR p.fabricante = 'ANJUU/AISHTY' THEN 'NAKD' ELSE 'Marcia Mello' END,
	CASE
		WHEN c.preco1 < 20 THEN '01. 0-20'
		WHEN c.preco1 < 40 THEN '02. 20-40'
		WHEN c.preco1 < 60 THEN '03. 40-60'
		WHEN c.preco1 < 80 THEN '04. 60-80'
		WHEN c.preco1 < 100 THEN '05. 80-100'
		WHEN c.preco1 < 125 THEN '06. 100-125'
		WHEN c.preco1 < 150 THEN '07. 125-150'
		WHEN c.preco1 < 175 THEN '08. 150-175'
		WHEN c.preco1 < 200 THEN '09. 175-200'
		WHEN c.preco1 < 250 THEN '10. 200-250'
		WHEN c.preco1 < 300 THEN '11. 250-300'
		WHEN c.preco1 < 400 THEN '12. 300-400'
		WHEN c.preco1 < 500 THEN '13. 400-500'
		ELSE '14. 500+'
	END,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
	CASE WHEN LTRIM(RTRIM(pc.desc_cor_produto)) LIKE 'estampa%' THEN 'Estampado' ELSE 'Liso' END
) cart on
	pp.anomes = cart.anomes and
	pp.marca = cart.marca and
	pp.faixa_de_custo = cart.faixa_de_custo and
	pp.categoria = cart.categoria and
	pp.liso_estampado = cart.liso_estampado
WHERE
pp.anomes >= YEAR(%(date_to)s)*100+MONTH(%(date_to)s)
GROUP BY
pp.anomes,
pp.categoria,
pp.liso_estampado