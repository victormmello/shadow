WITH collections as (
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'OI' ELSE 'V' END +
	CAST(CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN YEAR(GETDATE()-1) ELSE YEAR(GETDATE()-1)+1 END AS CHAR) as collection
	UNION
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'OI' ELSE 'V' END +
	CAST(CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN YEAR(GETDATE()-1)-1 ELSE YEAR(GETDATE()-1) END AS CHAR) as collection
	UNION
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'V' ELSE 'OI' END +
	CAST(YEAR(GETDATE()-1) AS CHAR) as collection
),
base as (
	SELECT
	ppc.produto,
	ppc.cor_produto,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
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
	ROW_NUMBER() OVER(PARTITION BY 
		COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
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
		END
		ORDER BY ppc.potencial_pc1 DESC) as numero,
	ppc.potencial_itens
	FROM bi_potencial_produto_cor ppc
	INNER JOIN produtos p on p.produto = ppc.produto
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
	p.colecao IN (SELECT * FROM collections) and
	ppc.potencial_pc1 > 0
)
SELECT
b.categoria,
b.faixa_de_custo,
CASE
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.2 THEN 'A'
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.5 THEN 'B'
	ELSE 'C'
END as classificacao,
AVG(b.potencial_itens)*30*3*1.5 as potencial_medio,
SUM(b.potencial_itens)*30*3*1.5 as soma_potencial
FROM base b
INNER JOIN (
	SELECT
	b.categoria,
	b.faixa_de_custo,
	MAX(numero) as modelos
	FROM base b
	GROUP BY
	b.categoria,
	b.faixa_de_custo
	HAVING
	COUNT(*) > 10
) m on 
	b.categoria = m.categoria and
	b.faixa_de_custo = m.faixa_de_custo
GROUP BY
b.categoria,
b.faixa_de_custo,
CASE
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.2 THEN 'A'
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.5 THEN 'B'
	ELSE 'C'
END