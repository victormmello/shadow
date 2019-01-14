SELECT
p.colecao,
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
COALESCE(gf.filial,LTRIM(RTRIM(e.filial))) as filial,
SUM(CASE WHEN COALESCE(gf.filial,LTRIM(RTRIM(e.filial))) != 'E-COMMERCE' OR vpi.produto IS NOT NULL THEN e.estoque_disponivel END) as quantidade_estoque,
SUM(CASE WHEN COALESCE(gf.filial,LTRIM(RTRIM(e.filial))) != 'E-COMMERCE' OR vpi.produto IS NOT NULL THEN e.estoque_disponivel * c.preco1 END) as custo_estoque,
COUNT(DISTINCT CASE WHEN COALESCE(gf.filial,LTRIM(RTRIM(e.filial))) != 'E-COMMERCE' OR vpi.produto IS NOT NULL THEN e.produto + e.cor_produto END) as produtos
FROM w_estoque_produtos_transito e
INNER JOIN produtos p on e.produto = p.produto
INNER JOIN produtos_precos c on
	e.produto = c.produto and
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
LEFT JOIN (
	SELECT 'E-COMMERCE' as filial, 'E-COMMERCE' as grupo_filial UNION
	SELECT 'CAMBUI' as filial, 'E-COMMERCE' as grupo_filial
) gf on gf.grupo_filial = e.filial
LEFT JOIN bi_vtex_product_images vpi on
	vpi.produto = e.produto and
	vpi.cor_produto = e.cor_produto and
	vpi.numero = 1 and
	gf.filial = 'E-COMMERCE'
WHERE
p.grupo_produto != 'GIFTCARD' and
c.codigo_tab_preco = 02 and
e.estoque_disponivel > 0 and
e.filial in ('iguatemi campinas','santa ursula','mm dom pedro','premium outlet itupeva','santos praiamar','piracicaba shopping','jundiai shopping','e-commerce')
GROUP BY
p.colecao,
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
COALESCE(gf.filial,LTRIM(RTRIM(e.filial)))