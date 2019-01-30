-- AnoMês:
IF OBJECT_ID('bi_category_sales_datas') IS NOT NULL
  DROP TABLE bi_category_sales_datas;
SELECT 
YEAR(vp.data_venda)*100 + MONTH(vp.data_venda) as anomes,
MAX(vp.data_venda) as ultima_venda
INTO dbo.bi_category_sales_datas
FROM loja_venda vp
WHERE
YEAR(vp.data_venda)*100 + MONTH(vp.data_venda) >= YEAR(DATEADD(yy,-2,GETDATE()-1))*100 + MONTH(DATEADD(yy,-2,GETDATE()-1)) and
CAST(vp.data_venda as DATE) < CAST(GETDATE() as DATE)
GROUP BY
YEAR(vp.data_venda)*100 + MONTH(vp.data_venda);

-- Estoque:
IF OBJECT_ID('bi_category_sales_estoque') IS NOT NULL
  DROP TABLE bi_category_sales_estoque;
SELECT
YEAR(GETDATE()-1)*100 + MONTH(GETDATE()-1) as anomes,
e.filial,
e.produto,
e.cor_produto,
SUM(e.estoque_disponivel) as quantidade_estoque
INTO dbo.bi_category_sales_estoque
FROM w_estoque_disponivel_sku e
INNER JOIN produtos p on p.produto = e.produto
WHERE
p.grupo_produto != 'GIFTCARD' and
e.estoque_disponivel > 0 and
e.filial in ('iguatemi campinas','santa ursula','mm dom pedro','premium outlet itupeva','santos praiamar','piracicaba shopping','jundiai shopping','e-commerce')
GROUP BY
e.filial,
e.produto,
e.cor_produto;

-- Vendas:
IF OBJECT_ID('bi_category_sales_vendas') IS NOT NULL
  DROP TABLE bi_category_sales_vendas;
SELECT
v.anomes,
v.filial,
v.produto,
v.cor_produto,
SUM(v.itens) as itens,
SUM(v.preco_original_total) as preco_original_total,
SUM(v.receita_total) as receita_total,
SUM(v.custo_total) as custo_total
INTO dbo.bi_category_sales_vendas
FROM (
	SELECT
	YEAR(vp.data_venda)*100 + MONTH(vp.data_venda) as anomes,
	f.filial,
	vp.produto,
	vp.cor_produto,
	SUM(vp.qtde) as itens,
	SUM(vp.qtde*(vp.preco_liquido + vp.desconto_item)) as preco_original_total,
	SUM(vp.qtde*(v.valor_pago/v.valor_venda_bruta*vp.preco_liquido)) as receita_total,
	SUM(COALESCE(NULLIF(vp.custo,0),(vp.preco_liquido + vp.desconto_item)/3.5)*vp.qtde) as custo_total
	FROM loja_venda_produto vp
	INNER JOIN loja_venda v on
		v.ticket = vp.ticket and
		vp.codigo_filial = v.codigo_filial and
		v.data_venda = vp.data_venda
	INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
	WHERE
	vp.qtde > 0 and
	vp.preco_liquido > 0 and
	v.valor_venda_bruta > 0 and
	CAST(vp.data_venda as DATE) < CAST(GETDATE() as DATE) and
	YEAR(vp.data_venda)*100 + MONTH(vp.data_venda) >= YEAR(DATEADD(yy,-2,GETDATE()-1))*100 + MONTH(DATEADD(yy,-2,GETDATE()-1))
	GROUP BY
	YEAR(vp.data_venda)*100 + MONTH(vp.data_venda),
	f.filial,
	vp.produto,
	vp.cor_produto

	UNION

	SELECT
	YEAR(f.emissao)*100 + MONTH(f.emissao) as anomes,
	f.filial,
	fp.produto,
	fp.cor_produto,
	SUM(fp.qtde) as itens,
	SUM(fp.valor + fp.desconto_item) as preco_original_total,
	SUM(f.valor_total/f.valor_sub_itens*fp.valor) as receita_total,
	SUM(COALESCE(NULLIF(fp.custo_na_data,0),(fp.preco+fp.desconto_item)/3.5)*fp.qtde) as custo_total
	FROM faturamento_prod fp
	INNER JOIN faturamento f on
		f.nf_saida = fp.NF_SAIDA and
		fp.filial = f.filial
	WHERE
	fp.qtde > 0 and
	f.valor_sub_itens > 0 and
	fp.serie_nf IN (2,7) and
	f.natureza_saida = '100.01' and
	f.filial = 'e-commerce' and
	CAST(f.emissao as DATE) < CAST(GETDATE() as DATE) and
	YEAR(f.emissao)*100 + MONTH(f.emissao) >= YEAR(DATEADD(yy,-2,GETDATE()-1))*100 + MONTH(DATEADD(yy,-2,GETDATE()-1))		
	GROUP BY
	YEAR(f.emissao)*100 + MONTH(f.emissao),
	f.filial,
	fp.produto,
	fp.cor_produto
) v
GROUP BY
v.anomes,
v.filial,
v.produto,
v.cor_produto;

-- Recebimento:
IF OBJECT_ID('bi_category_sales_recebimentos') IS NOT NULL
  DROP TABLE bi_category_sales_recebimentos;
SELECT
YEAR(e.emissao)*100 + MONTH(e.emissao) as anomes,
e.filial,
ep.produto,
ep.cor_produto,
SUM(ep.qtde_entrada) as quantidade_recebida,
SUM(ep.valor) as custo_recebido,
MAX(e.emissao) as ultimo_recebimento
INTO dbo.bi_category_sales_recebimentos
FROM loja_entradas e
INNER JOIN loja_entradas_produto ep on ep.romaneio_produto = e.romaneio_produto
WHERE
e.entrada_por = 02
and e.filial in ('iguatemi campinas','santa ursula','mm dom pedro','premium outlet itupeva','santos praiamar','piracicaba shopping','jundiai shopping','e-commerce')
and YEAR(e.emissao)*100 + MONTH(e.emissao) >= YEAR(DATEADD(yy,-2,GETDATE()-1))*100 + MONTH(DATEADD(yy,-2,GETDATE()-1))
and ep.qtde_entrada > 0
GROUP BY
YEAR(e.emissao)*100 + MONTH(e.emissao),
e.filial,
ep.produto,
ep.cor_produto;

-- Carteira:
IF OBJECT_ID('bi_category_sales_carteira') IS NOT NULL
  DROP TABLE bi_category_sales_carteira;
SELECT
YEAR(cp.entrega)*100 + MONTH(cp.entrega) as anomes,
c.filial_a_entregar as filial,
cp.produto,
cp.cor_produto,
SUM(cp.qtde_entregar) as quantidade_a_receber,
SUM(cp.valor_entregar) as custo_a_receber
INTO dbo.bi_category_sales_carteira
FROM compras_produto cp
INNER JOIN compras c on cp.pedido = c.pedido
WHERE
YEAR(cp.entrega)*100 + MONTH(cp.entrega) BETWEEN YEAR(DATEADD(yy,-2,GETDATE()-1))*100 + MONTH(DATEADD(yy,-2,GETDATE()-1)) and YEAR(GETDATE()-1)*100 + MONTH(GETDATE()-1)
and cp.qtde_entregar > 0
and cp.qtde_entregue = 0
GROUP BY
YEAR(cp.entrega)*100 + MONTH(cp.entrega),
c.filial_a_entregar,
cp.produto,
cp.cor_produto;

IF OBJECT_ID('bi_category_sales') IS NOT NULL
  DROP TABLE bi_category_sales;
SELECT
f.filial,
p.produto,
pc.cor_produto,
MAX(CASE WHEN p.griffe = 'NAKD' OR p.fabricante = 'ANJUU/AISHTY' THEN 'NAKD' ELSE 'Marcia Mello' END) as marca,
MAX(COALESCE(cor.grupo_cor,'Outro')) as grupo_cor,
MAX(p.tipo_produto) as tipo_produto,
MAX(COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros')) as categoria,
MAX(CASE WHEN LTRIM(RTRIM(pc.desc_cor_produto)) LIKE 'estampa%' THEN 'Estampado' ELSE 'Liso' END) as liso_estampado,
MAX(LTRIM(RTRIM(p.fabricante))) as fabricante,
MAX(CASE
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
END) as faixa_de_custo,
d.anomes,
COALESCE(SUM(e.quantidade_estoque),0) as quantidade_estoque,
COALESCE(SUM(e.quantidade_estoque * c.preco1),0) as custo_estoque,
COALESCE(SUM(v.itens),0) as itens,
COALESCE(SUM(v.preco_original_total),0) as preco_original_total,
COALESCE(SUM(v.receita_total),0) as receita_total,
COALESCE(SUM(v.custo_total),0) as custo_total,
COALESCE(SUM(r.quantidade_recebida),0) as quantidade_recebida,
COALESCE(SUM(r.custo_recebido),0) as custo_recebido,
COALESCE(MAX(r.ultimo_recebimento),0) as ultimo_recebimento,
COALESCE(SUM(cart.quantidade_a_receber),0) as quantidade_a_receber,
COALESCE(SUM(cart.custo_a_receber),0) as custo_a_receber,
MAX(d.ultima_venda) as ultima_venda
INTO dbo.bi_category_sales
FROM produto_cores pc
INNER JOIN produtos p on pc.produto = p.produto
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
CROSS JOIN (
	SELECT 'IGUATEMI CAMPINAS' as filial UNION
	SELECT 'SANTA URSULA' as filial UNION
	SELECT 'MM DOM PEDRO' as filial UNION
	SELECT 'PREMIUM OUTLET ITUPEVA' as filial UNION
	SELECT 'SANTOS PRAIAMAR' as filial UNION
	SELECT 'PIRACICABA SHOPPING' as filial UNION
	SELECT 'JUNDIAI SHOPPING' as filial UNION
	SELECT 'E-COMMERCE' as filial
) f
CROSS JOIN bi_category_sales_datas d
INNER JOIN produtos_precos c on
	pc.produto = c.produto and
	c.codigo_tab_preco = 02 -- 02 custo
LEFT JOIN bi_category_sales_estoque e on
	e.anomes = d.anomes and
	e.filial = f.filial and
	e.produto = pc.produto and
	e.cor_produto = pc.cor_produto
LEFT JOIN bi_category_sales_vendas v on
	v.anomes = d.anomes and
	v.filial = f.filial and
	v.produto = pc.produto and
	v.cor_produto = pc.cor_produto
LEFT JOIN bi_category_sales_recebimentos r on
	r.anomes = d.anomes and
	r.filial = f.filial and
	r.produto = pc.produto and
	r.cor_produto = pc.cor_produto
LEFT JOIN bi_category_sales_carteira cart on
	cart.anomes = d.anomes and
	cart.filial = f.filial and
	cart.produto = pc.produto and
	cart.cor_produto = pc.cor_produto
WHERE
p.grupo_produto != 'GIFTCARD' and
COALESCE(e.quantidade_estoque,v.itens,r.quantidade_recebida,cart.quantidade_a_receber,0) > 0
GROUP BY
f.filial,
p.produto,
pc.cor_produto,
d.anomes;

IF OBJECT_ID('bi_category_sales_datas') IS NOT NULL
  DROP TABLE bi_category_sales_datas;
IF OBJECT_ID('bi_category_sales_estoque') IS NOT NULL
  DROP TABLE bi_category_sales_estoque;
IF OBJECT_ID('bi_category_sales_vendas') IS NOT NULL
  DROP TABLE bi_category_sales_vendas;
IF OBJECT_ID('bi_category_sales_recebimentos') IS NOT NULL
  DROP TABLE bi_category_sales_recebimentos;
IF OBJECT_ID('bi_category_sales_carteira') IS NOT NULL
  DROP TABLE bi_category_sales_carteira;
