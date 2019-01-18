SELECT DISTINCT 
t.concatenado,
t.grupo_produto,
t.subgrupo_produto,
COALESCE(c.classificacao,'Outros') as classificacao,
COALESCE(c.tipo_cod_barras,3) as tipo_cod_barras
FROM (
	SELECT DISTINCT
	RTRIM(p.grupo_produto) + '-' + RTRIM(p.subgrupo_produto) as concatenado,
	RTRIM(p.grupo_produto) as grupo_produto,
	RTRIM(p.subgrupo_produto) as subgrupo_produto
	FROM produtos p
	WHERE
	(p.grupo_produto like '[_]%' or p.grupo_produto = 'CALCADOS')
	and p.data_cadastramento >= GETDATE()-360
	
	UNION

	SELECT
	RTRIM(ps.grupo_produto) + '-' + RTRIM(ps.subgrupo_produto) as concatenado,
	RTRIM(ps.grupo_produto) as grupo_produto,
	RTRIM(ps.subgrupo_produto) as subgrupo_produto
	FROM produtos_subgrupo ps
	WHERE
	ps.inativo = 0 and
	ps.data_para_transferencia >= GETDATE()-360
) t
LEFT JOIN (
	SELECT '_BERMUDA' as grupo_produto, 'Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_CALCA' as grupo_produto, 'Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_SHORT' as grupo_produto, 'Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_SAIA' as grupo_produto, 'Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_BLUSA' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_BODY' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_CAMISA' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_CAMISETA' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_CHEMISIE' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_TUNICA' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT 'BLUSA' as grupo_produto, 'Top' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_CASACO' as grupo_produto, 'Casacos e Jaquetas' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_BLAZER' as grupo_produto, 'Casacos e Jaquetas' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_COLETE' as grupo_produto, 'Casacos e Jaquetas' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_JAQUETA' as grupo_produto, 'Casacos e Jaquetas' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_PIJAMA' as grupo_produto, 'Top+Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_VESTIDO' as grupo_produto, 'Top+Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT '_MACACAO' as grupo_produto, 'Top+Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT 'VESTIDO' as grupo_produto, 'Top+Bottom' as classificacao, 4 as tipo_cod_barras
	UNION SELECT 'CALCADOS' as grupo_produto, 'Calcados' as classificacao, 4 as tipo_cod_barras
	UNION SELECT 'ACESSORIOS' as grupo_produto, 'Outros' as classificacao, 3 as tipo_cod_barras
	UNION SELECT 'BIJUTERIA' as grupo_produto, 'Outros' as classificacao, 3 as tipo_cod_barras
	UNION SELECT 'MM HOME' as grupo_produto, 'Outros' as classificacao, 3 as tipo_cod_barras

) c on c.grupo_produto = t.grupo_produto
ORDER BY t.concatenado