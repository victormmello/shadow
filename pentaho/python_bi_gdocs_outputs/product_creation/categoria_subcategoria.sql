SELECT
MAX(RTRIM(cat.categoria_produto)) as categoria_produto,
p.cod_categoria,
MIN(p.cod_subcategoria) as cod_subcategoria
FROM produtos p
INNER JOIN produtos_categoria cat on cat.cod_categoria = p.cod_categoria
INNER JOIN produtos_subcategoria sub_cat on
	sub_cat.cod_subcategoria = p.cod_subcategoria and
	sub_cat.cod_categoria = p.cod_categoria
WHERE
(p.grupo_produto like '[_]%' or p.grupo_produto IN ('CALCADOS','_CALCADOS'))
and p.data_cadastramento >= GETDATE()-360
and p.cod_categoria IN ('01','02','03','BD')
GROUP BY
p.cod_categoria